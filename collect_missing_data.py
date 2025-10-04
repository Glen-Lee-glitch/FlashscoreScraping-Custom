#!/usr/bin/env python3
"""
누락된 status와 odds 데이터를 수집하는 스크립트
"""

import json
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import sys
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# 데이터베이스 설정
DB_CONFIG = {
    "host": "aws-1-ap-northeast-2.pooler.supabase.com",
    "database": "postgres",
    "user": "postgres.dvwwcmhzlllaukscjuya",
    "password": "!Qdhdbrclf56",
    "port": "6543"
}

def connect_to_db():
    """데이터베이스 연결"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ 데이터베이스 연결 성공")
        return conn
    except Exception as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")
        return None

def setup_selenium_driver():
    """Selenium 드라이버 설정"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # 헤드리스 모드
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(10)
        return driver
    except Exception as e:
        print(f"❌ Selenium 드라이버 설정 실패: {e}")
        return None

def extract_status_from_page(driver, match_link):
    """경기 페이지에서 status 정보 추출"""
    try:
        driver.get(match_link)
        time.sleep(3)  # 페이지 로딩 대기
        
        # status를 찾기 위한 여러 선택자 시도
        status_selectors = [
            '.event__time',
            '.event__stage',
            '.detailScore__status',
            '.matchInfo__status',
            '[class*="status"]',
            '[class*="time"]'
        ]
        
        for selector in status_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and text not in ['', 'VS', 'v', '-']:
                        # 경기 상태 판별
                        if any(keyword in text.lower() for keyword in ['종료', 'finished', 'ft', '완료']):
                            return "종료"
                        elif any(keyword in text.lower() for keyword in ['live', '진행', '중']):
                            return "진행중"
                        elif any(keyword in text.lower() for keyword in ['예정', 'scheduled', 'upcoming']):
                            return "예정"
                        elif ':' in text and len(text) <= 5:  # 시간 형태 (예: "90+5", "HT")
                            continue
                        else:
                            return text
            except:
                continue
        
        return None
        
    except Exception as e:
        print(f"⚠️ status 추출 실패 ({match_link}): {e}")
        return None

def extract_odds_from_page(driver, match_link):
    """경기 페이지에서 odds 정보 추출"""
    try:
        # odds 페이지로 이동
        odds_link = match_link.replace('#/match-summary/match-summary', '#/odds-comparison/1x2-odds/full-time')
        driver.get(odds_link)
        time.sleep(5)  # 동적 로딩 대기
        
        # Over/Under odds 찾기
        odds_data = {
            "over-under": []
        }
        
        # Over/Under 탭 클릭 시도
        try:
            over_under_tab = driver.find_element(By.XPATH, "//span[contains(text(), 'Over/Under') or contains(text(), '오버/언더')]")
            over_under_tab.click()
            time.sleep(3)
        except:
            pass
        
        # Over/Under odds 테이블 찾기
        try:
            # 다양한 선택자로 odds 테이블 찾기
            table_selectors = [
                '.odds-table',
                '.oddsTable',
                '[class*="odds"]',
                'table'
            ]
            
            for selector in table_selectors:
                try:
                    tables = driver.find_elements(By.CSS_SELECTOR, selector)
                    for table in tables:
                        rows = table.find_elements(By.TAG_NAME, 'tr')
                        for row in rows:
                            cells = row.find_elements(By.TAG_NAME, 'td')
                            if len(cells) >= 3:
                                # 첫 번째 셀에서 handicap 값 추출
                                handicap_text = cells[0].text.strip()
                                if re.match(r'^\d+\.?\d*$', handicap_text):
                                    handicap = float(handicap_text)
                                    
                                    # 두 번째와 세 번째 셀에서 over/under odds 추출
                                    over_text = cells[1].text.strip()
                                    under_text = cells[2].text.strip()
                                    
                                    if re.match(r'^\d+\.?\d*$', over_text) and re.match(r'^\d+\.?\d*$', under_text):
                                        over_odds = float(over_text)
                                        under_odds = float(under_text)
                                        
                                        odds_entry = {
                                            "handicap": str(handicap),
                                            "average": {
                                                "over": str(over_odds),
                                                "under": str(under_odds)
                                            },
                                            "bookmakers": []
                                        }
                                        odds_data["over-under"].append(odds_entry)
                except:
                    continue
                    
        except Exception as e:
            print(f"⚠️ odds 테이블 파싱 실패: {e}")
        
        return odds_data if odds_data["over-under"] else None
        
    except Exception as e:
        print(f"⚠️ odds 추출 실패 ({match_link}): {e}")
        return None

def get_matches_missing_data():
    """데이터베이스에서 누락된 데이터가 있는 경기들 조회"""
    conn = connect_to_db()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # status가 null이거나 'Unknown'인 경기들
        cursor.execute("""
            SELECT id, match_link, status 
            FROM matches 
            WHERE (status IS NULL OR status = 'Unknown') 
            AND match_link IS NOT NULL
            ORDER BY match_time DESC
            LIMIT 50
        """)
        
        missing_status_matches = cursor.fetchall()
        
        # odds가 null인 경기들 (status가 있는 경기만)
        cursor.execute("""
            SELECT id, match_link, status, best_benchmark, best_over_odds, best_under_odds
            FROM matches 
            WHERE (best_benchmark IS NULL OR best_over_odds IS NULL OR best_under_odds IS NULL)
            AND status IS NOT NULL 
            AND status != 'Unknown'
            AND match_link IS NOT NULL
            ORDER BY match_time DESC
            LIMIT 50
        """)
        
        missing_odds_matches = cursor.fetchall()
        
        return missing_status_matches, missing_odds_matches
        
    except Exception as e:
        print(f"❌ 데이터베이스 조회 실패: {e}")
        return [], []
    finally:
        cursor.close()
        conn.close()

def update_match_status(match_id, status):
    """경기 상태 업데이트"""
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE matches SET status = %s WHERE id = %s",
            (status, match_id)
        )
        conn.commit()
        print(f"✅ status 업데이트: {match_id} -> {status}")
        return True
    except Exception as e:
        print(f"❌ status 업데이트 실패: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def update_match_odds(match_id, best_benchmark, best_over_odds, best_under_odds):
    """경기 배당률 업데이트"""
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE matches SET best_benchmark = %s, best_over_odds = %s, best_under_odds = %s WHERE id = %s",
            (best_benchmark, best_over_odds, best_under_odds, match_id)
        )
        conn.commit()
        print(f"✅ odds 업데이트: {match_id} -> {best_benchmark}/{best_over_odds}/{best_under_odds}")
        return True
    except Exception as e:
        print(f"❌ odds 업데이트 실패: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def find_best_odds(over_under_odds_list):
    """최적의 배당률을 찾는 함수"""
    if not over_under_odds_list or len(over_under_odds_list) == 0:
        return None
    
    best_odds = None
    min_difference = float('inf')
    max_over_odds = -1
    
    for odds_data in over_under_odds_list:
        try:
            over_odds = float(odds_data['average']['over'])
            under_odds = float(odds_data['average']['under'])
            
            difference = abs(over_odds - under_odds)
            
            if difference < min_difference:
                min_difference = difference
                max_over_odds = over_odds
                best_odds = odds_data
            elif difference == min_difference and over_odds > max_over_odds:
                max_over_odds = over_odds
                best_odds = odds_data
                
        except (ValueError, KeyError, TypeError):
            continue
    
    return best_odds

def main():
    """메인 함수"""
    print("🚀 누락된 데이터 수집 시작")
    
    # Selenium 드라이버 설정
    driver = setup_selenium_driver()
    if not driver:
        print("❌ Selenium 드라이버 초기화 실패")
        return
    
    try:
        # 누락된 데이터가 있는 경기들 조회
        missing_status_matches, missing_odds_matches = get_matches_missing_data()
        
        print(f"📊 누락된 status 경기: {len(missing_status_matches)}개")
        print(f"📊 누락된 odds 경기: {len(missing_odds_matches)}개")
        
        # Status 수집
        status_success = 0
        status_failed = 0
        
        for match in missing_status_matches:
            match_id = match['id']
            match_link = match['match_link']
            
            print(f"🔄 status 수집 중: {match_id}")
            
            status = extract_status_from_page(driver, match_link)
            if status:
                if update_match_status(match_id, status):
                    status_success += 1
                else:
                    status_failed += 1
            else:
                print(f"⚠️ status 추출 실패: {match_id}")
                status_failed += 1
            
            # 요청 간격 조절
            time.sleep(2)
        
        # Odds 수집
        odds_success = 0
        odds_failed = 0
        
        for match in missing_odds_matches:
            match_id = match['id']
            match_link = match['match_link']
            
            print(f"🔄 odds 수집 중: {match_id}")
            
            odds_data = extract_odds_from_page(driver, match_link)
            if odds_data and odds_data["over-under"]:
                best_odds = find_best_odds(odds_data["over-under"])
                if best_odds:
                    best_benchmark = best_odds['handicap']
                    best_over_odds = best_odds['average']['over']
                    best_under_odds = best_odds['average']['under']
                    
                    if update_match_odds(match_id, best_benchmark, best_over_odds, best_under_odds):
                        odds_success += 1
                    else:
                        odds_failed += 1
                else:
                    print(f"⚠️ 최적 odds 찾기 실패: {match_id}")
                    odds_failed += 1
            else:
                print(f"⚠️ odds 추출 실패: {match_id}")
                odds_failed += 1
            
            # 요청 간격 조절
            time.sleep(3)
        
        # 결과 출력
        print(f"\n📊 수집 완료!")
        print(f"  Status - 성공: {status_success}개, 실패: {status_failed}개")
        print(f"  Odds - 성공: {odds_success}개, 실패: {odds_failed}개")
        
    except Exception as e:
        print(f"❌ 수집 중 오류: {e}")
    finally:
        driver.quit()
        print("🔌 Selenium 드라이버 종료")

if __name__ == "__main__":
    main()
