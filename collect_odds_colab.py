#!/usr/bin/env python3
"""
Colab용 멀티스레드 odds 수집 스크립트
기존 JSON 파일에서 odds가 null인 경기들만 선별하여 수집
"""

import json
import requests
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import queue
import os
from datetime import datetime

# Colab용 설정
COLAB_MODE = True  # Colab 환경에서는 True로 설정

def setup_selenium_driver_colab():
    """Colab 환경에 최적화된 Selenium 드라이버 설정"""
    chrome_options = Options()
    
    if COLAB_MODE:
        # Colab 환경 설정
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=512")
    
    try:
        # 최신 Selenium은 자동으로 드라이버 관리
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(10)
        return driver
    except Exception as e:
        print(f"❌ Selenium 드라이버 설정 실패: {e}")
        return None

def extract_odds_from_page(driver, match_link, match_id):
    """경기 페이지에서 odds 정보 추출 (멀티스레드 안전)"""
    try:
        # odds 페이지로 이동 (URL 구조 수정)
        # 기존: .../?mid=ID#/match-summary/match-summary
        # 변경: .../odds/over-under/full-time/?mid=ID
        if '?mid=' in match_link and '#/match-summary/match-summary' in match_link:
            # mid 파라미터 추출
            mid_part = match_link.split('?mid=')[1].split('#')[0]
            # 기본 경로에서 mid 제거
            base_path = match_link.split('?mid=')[0]
            # 올바른 odds URL 구성
            odds_link = f"{base_path}/odds/over-under/full-time/?mid={mid_part}"
        else:
            # fallback: 기존 방식
            odds_link = match_link.replace('#/match-summary/match-summary', '/odds/over-under/full-time/')
        
        print(f"[{match_id}] 접속 URL: {odds_link}")
        
        driver.get(odds_link)
        
        # 동적 로딩 대기
        time.sleep(5)
        
        # 페이지 상태 확인
        page_title = driver.title
        print(f"[{match_id}] 페이지 제목: {page_title}")
        
        # 페이지 내용 일부 확인
        page_content = driver.page_source[:500]
        print(f"[{match_id}] 페이지 내용 (처음 500자): {page_content}")
        
        # 에러 페이지 확인
        if "error" in page_title.lower() or "404" in page_title or "not found" in page_title.lower():
            print(f"[{match_id}] ❌ 에러 페이지 감지")
            return None
        
        # Over/Under odds 찾기
        odds_data = {
            "over-under": []
        }
        
        # Over/Under odds 테이블 찾기
        try:
            # 다양한 선택자로 odds 테이블 찾기
            table_selectors = [
                '.ui-table__row',
                '.odds-table tbody tr',
                '.oddsTable tbody tr',
                'table tbody tr'
            ]
            
            print(f"[{match_id}] odds 테이블 선택자 시도 중...")
            found_rows = False
            
            for selector in table_selectors:
                try:
                    rows = driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"[{match_id}] {selector}: {len(rows)}개 행 발견")
                    if rows:
                        found_rows = True
                        print(f"[{match_id}] ✅ {len(rows)}개 odds 행 발견 - {selector} 사용")
                        
                        # 기준점별로 배당률을 그룹화
                        odds_by_handicap = {}
                        
                        for row in rows:
                            try:
                                # 북메이커명 추출
                                bookmaker_elements = row.find_elements(By.CSS_SELECTOR, 'img[title], img[alt], .bookmaker')
                                bookmaker_name = "Unknown"
                                if bookmaker_elements:
                                    bookmaker_name = bookmaker_elements[0].get_attribute('title') or bookmaker_elements[0].get_attribute('alt') or "Unknown"
                                
                                # 기준점 추출
                                handicap_elements = row.find_elements(By.CSS_SELECTOR, 'span[data-testid="wcl-oddsValue"], .handicap, .line')
                                if not handicap_elements:
                                    continue
                                
                                handicap_text = handicap_elements[0].text.strip()
                                if not handicap_text.replace('.', '').replace(',', '').isdigit():
                                    continue
                                
                                handicap = float(handicap_text)
                                
                                # Over/Under 배당률 추출
                                odds_cells = row.find_elements(By.CSS_SELECTOR, 'a.oddsCell__odd, .odds-cell, .odd')
                                
                                if len(odds_cells) >= 2:
                                    over_text = odds_cells[0].text.strip().split('\n')[0]
                                    under_text = odds_cells[1].text.strip().split('\n')[0]
                                    
                                    # 숫자 확인
                                    try:
                                        over_odds = float(over_text)
                                        under_odds = float(under_text)
                                        
                                        if handicap not in odds_by_handicap:
                                            odds_by_handicap[handicap] = []
                                        
                                        odds_by_handicap[handicap].append({
                                            "bookmaker": bookmaker_name,
                                            "over": str(over_odds),
                                            "under": str(under_odds)
                                        })
                                    except ValueError:
                                        continue
                            
                            except Exception as row_error:
                                continue
                        
                        # 각 기준점별로 평균 계산
                        for handicap, bookmaker_odds in odds_by_handicap.items():
                            if len(bookmaker_odds) > 0:
                                # 평균 계산
                                over_values = [float(b['over']) for b in bookmaker_odds]
                                under_values = [float(b['under']) for b in bookmaker_odds]
                                
                                avg_over = sum(over_values) / len(over_values)
                                avg_under = sum(under_values) / len(under_values)
                                
                                odds_entry = {
                                    "handicap": str(handicap),
                                    "average": {
                                        "over": f"{avg_over:.2f}",
                                        "under": f"{avg_under:.2f}"
                                    },
                                    "bookmakers": bookmaker_odds
                                }
                                odds_data["over-under"].append(odds_entry)
                        
                        break  # 첫 번째로 찾은 테이블 사용
                
                except Exception as e:
                    print(f"[{match_id}] {selector} 실패: {e}")
                    continue
            
            if not found_rows:
                print(f"[{match_id}] ❌ 모든 선택자에서 odds 테이블을 찾을 수 없음")
                # 페이지의 모든 테이블 요소 확인
                all_tables = driver.find_elements(By.TAG_NAME, 'table')
                all_divs_with_odds = driver.find_elements(By.XPATH, "//div[contains(@class, 'odds') or contains(@class, 'table')]")
                print(f"[{match_id}] 전체 테이블: {len(all_tables)}개, odds 관련 div: {len(all_divs_with_odds)}개")
                return None
                    
        except Exception as e:
            print(f"[{match_id}] odds 테이블 파싱 실패: {e}")
        
        return odds_data if odds_data["over-under"] else None
        
    except Exception as e:
        print(f"[{match_id}] odds 추출 실패: {e}")
        return None

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

def process_match_worker(match_data, thread_id):
    """개별 경기 처리 워커 함수 (멀티스레드용)"""
    match_id, match_info = match_data
    
    # 각 스레드마다 독립적인 드라이버 생성
    driver = setup_selenium_driver_colab()
    if not driver:
        return None
    
    try:
        match_link = match_info.get('match_link')
        if not match_link:
            return None
        
        print(f"[Thread-{thread_id}] Processing {match_id}...")
        
        # odds 수집
        odds_data = extract_odds_from_page(driver, match_link, match_id)
        
        if odds_data and odds_data["over-under"]:
            best_odds = find_best_odds(odds_data["over-under"])
            if best_odds:
                result = {
                    'match_id': match_id,
                    'odds': odds_data,
                    'best_benchmark': best_odds['handicap'],
                    'best_over_odds': best_odds['average']['over'],
                    'best_under_odds': best_odds['average']['under'],
                    'success': True
                }
                print(f"[Thread-{thread_id}] ✅ {match_id}: {len(odds_data['over-under'])}개 odds 수집")
                return result
        
        print(f"[Thread-{thread_id}] ❌ {match_id}: odds 수집 실패")
        return {'match_id': match_id, 'success': False}
        
    except Exception as e:
        print(f"[Thread-{thread_id}] ❌ {match_id}: {e}")
        return {'match_id': match_id, 'success': False, 'error': str(e)}
    
    finally:
        try:
            driver.quit()
        except:
            pass

def load_existing_json(file_path):
    """기존 JSON 파일 로드"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ JSON 파일 로드 실패: {e}")
        return None

def save_updated_json(data, file_path, backup=True):
    """업데이트된 JSON 파일 저장"""
    try:
        if backup:
            backup_path = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            import shutil
            shutil.copy2(file_path, backup_path)
            print(f"📁 백업 파일 생성: {backup_path}")
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 업데이트된 JSON 파일 저장: {file_path}")
        return True
    except Exception as e:
        print(f"❌ JSON 파일 저장 실패: {e}")
        return False

def main():
    """메인 함수 - 멀티스레드 odds 수집"""
    
    # 파일 경로 설정
    json_file_path = "src/data/soccer_england_championship-2025-2026.json"
    
    print("🚀 멀티스레드 odds 수집 시작")
    print(f"📁 대상 파일: {json_file_path}")
    
    # 기존 JSON 로드
    existing_data = load_existing_json(json_file_path)
    if not existing_data:
        return
    
    # odds가 null인 경기들만 필터링
    matches_to_process = []
    for match_id, match_data in existing_data.items():
        if 'odds' not in match_data or match_data['odds'] is None:
            matches_to_process.append((match_id, match_data))
    
    print(f"📊 처리 대상: {len(matches_to_process)}개 경기")
    print(f"📊 총 경기 수: {len(existing_data)}개")
    
    if not matches_to_process:
        print("✅ 모든 경기의 odds가 이미 수집되었습니다!")
        return
    
    # 멀티스레드 설정 (디버깅을 위해 1개 스레드로 시작)
    MAX_WORKERS = 1  # 디버깅용: 1개 스레드로 시작
    results = []
    
    print(f"🔄 {MAX_WORKERS}개 스레드로 처리 시작...")
    print(f"📝 디버깅 모드: 상세한 로그 출력")
    
    # ThreadPoolExecutor로 멀티스레드 처리
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 작업 제출
        future_to_match = {
            executor.submit(process_match_worker, match_data, i % MAX_WORKERS): match_data 
            for i, match_data in enumerate(matches_to_process)
        }
        
        # 결과 수집
        completed = 0
        for future in as_completed(future_to_match):
            result = future.result()
            if result:
                results.append(result)
            
            completed += 1
            progress = (completed / len(matches_to_process)) * 100
            print(f"📈 진행률: {completed}/{len(matches_to_process)} ({progress:.1f}%)")
    
    # 결과 분석
    successful_results = [r for r in results if r.get('success', False)]
    failed_results = [r for r in results if not r.get('success', False)]
    
    print(f"\n📊 수집 완료!")
    print(f"  ✅ 성공: {len(successful_results)}개")
    print(f"  ❌ 실패: {len(failed_results)}개")
    
    # 기존 데이터 업데이트
    updated_count = 0
    for result in successful_results:
        match_id = result['match_id']
        if match_id in existing_data:
            existing_data[match_id]['odds'] = result['odds']
            updated_count += 1
    
    # 업데이트된 JSON 저장
    if save_updated_json(existing_data, json_file_path):
        print(f"🎉 {updated_count}개 경기의 odds 데이터 업데이트 완료!")
    
    # 실패한 경기들 출력
    if failed_results:
        print(f"\n❌ 실패한 경기들:")
        for result in failed_results[:10]:  # 처음 10개만 출력
            print(f"  - {result['match_id']}: {result.get('error', 'Unknown error')}")
        if len(failed_results) > 10:
            print(f"  ... 및 {len(failed_results) - 10}개 더")

if __name__ == "__main__":
    main()
