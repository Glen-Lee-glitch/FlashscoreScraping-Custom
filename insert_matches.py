#!/usr/bin/env python3
"""
Flashscore JSON 데이터를 PostgreSQL matches 테이블에 삽입하는 스크립트
"""

import json
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, timezone, timedelta
import sys

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

def parse_match_time(date_str):
    """날짜 문자열을 PostgreSQL TIMESTAMPTZ 형식으로 변환 (로컬 시간 → UTC 변환)"""
    try:
        # "03.10.2025 19:00" 형식 처리 (DD.MM.YYYY HH:MM)
        if '.' in date_str and len(date_str.split('.')[0]) <= 2:
            # DD.MM.YYYY HH:MM 형식 - 로컬 시간으로 파싱 후 UTC로 변환
            dt = datetime.strptime(date_str, '%d.%m.%Y %H:%M')
            # 로컬 시간(한국 시간)으로 가정하고 UTC로 변환 (-9시간)
            kst = timezone(timedelta(hours=9))
            dt = dt.replace(tzinfo=kst)  # KST로 설정
            dt = dt.astimezone(timezone.utc)  # UTC로 변환
            return dt
        
        # "2024-12-21T15:30:00+00:00" 형식 처리
        elif 'T' in date_str:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt
        
        # "2024-12-21 15:30:00" 형식 처리
        else:
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            return dt
        
    except Exception as e:
        print(f"⚠️ 날짜 파싱 실패 ({date_str}): {e}")
        return None

def extract_season_from_filename(filename):
    """파일명에서 season 정보 추출"""
    # "soccer_england_championship-2025-2026.json" -> "england_championship-2025-2026"
    basename = os.path.basename(filename)
    # .json 확장자 제거
    name_without_ext = basename.replace('.json', '')
    # "soccer_" 접두사 제거
    if name_without_ext.startswith('soccer_'):
        season = name_without_ext[7:]  # "soccer_" (7글자) 제거
    else:
        season = name_without_ext
    
    return season

def find_best_odds(over_under_odds_list):
    """
    최적의 배당률을 찾는 함수
    1. 오버배당률과 언더배당률의 차이의 절댓값이 가장 작은 것
    2. 차이가 같으면 오버 배당률이 더 높은 것
    3. 그것도 같으면 아무거나
    """
    if not over_under_odds_list or len(over_under_odds_list) == 0:
        return None
    
    best_odds = None
    min_difference = float('inf')
    max_over_odds = -1
    
    for odds_data in over_under_odds_list:
        try:
            over_odds = float(odds_data['average']['over'])
            under_odds = float(odds_data['average']['under'])
            
            # 오버배당률과 언더배당률의 차이 절댓값
            difference = abs(over_odds - under_odds)
            
            # 조건 1: 차이가 가장 작은 것
            if difference < min_difference:
                min_difference = difference
                max_over_odds = over_odds
                best_odds = odds_data
            # 조건 2: 차이가 같으면 오버 배당률이 더 높은 것
            elif difference == min_difference and over_odds > max_over_odds:
                max_over_odds = over_odds
                best_odds = odds_data
                
        except (ValueError, KeyError, TypeError):
            # 숫자 변환 실패 시 해당 데이터 건너뛰기
            continue
    
    return best_odds

def insert_matches_from_json(json_file_path):
    """JSON 파일에서 경기 데이터를 읽어서 matches 테이블에 삽입"""
    
    # 파일명에서 season 추출
    season = extract_season_from_filename(json_file_path)
    print(f"🎯 추출된 season: {season}")
    
    # 데이터베이스 연결
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # JSON 파일 읽기
        print(f"📁 JSON 파일 읽는 중: {json_file_path}")
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"📊 총 {len(data)}개 경기 데이터 발견")
        
        # 삽입 통계
        inserted_count = 0
        skipped_count = 0
        error_count = 0
        
        # 각 경기 데이터 처리 (개별 트랜잭션으로 처리)
        for match_id, match_data in data.items():
            try:
                # 필수 데이터 확인
                if not all(key in match_data for key in ['date', 'home', 'away']):
                    print(f"⚠️ 필수 데이터 누락: {match_id}")
                    skipped_count += 1
                    continue
                
                # 경기 시간 파싱
                match_time = parse_match_time(match_data['date'])
                if not match_time:
                    skipped_count += 1
                    continue
                
                # 팀 정보 추출
                home_team_id = match_data['home'].get('id')
                away_team_id = match_data['away'].get('id')
                
                if not home_team_id or not away_team_id:
                    print(f"⚠️ 팀 ID 누락: {match_id}")
                    skipped_count += 1
                    continue
                
                # 경기 상태 추출
                status = match_data.get('status', 'Unknown')
                
                # match_link 추출
                match_link = match_data.get('match_link')
                
                # 점수 추출 (result 객체에서)
                home_score = None
                away_score = None
                if 'result' in match_data and match_data['result']:
                    result = match_data['result']
                    if 'home' in result and 'away' in result:
                        # status가 없을 때 result가 이상하게 저장되는 문제 해결
                        try:
                            home_score = int(result['home']) if result['home'].isdigit() else None
                            away_score = int(result['away']) if result['away'].isdigit() else None
                        except (ValueError, AttributeError):
                            # 숫자가 아닌 값이면 None으로 설정
                            home_score = None
                            away_score = None
                
                # 최적 배당률 추출 (odds에서)
                best_benchmark = None
                best_over_odds = None
                best_under_odds = None
                
                if 'odds' in match_data and match_data['odds'] and 'over-under' in match_data['odds']:
                    over_under_odds = match_data['odds']['over-under']
                    if over_under_odds and len(over_under_odds) > 0:
                        best_odds = find_best_odds(over_under_odds)
                        if best_odds:
                            best_benchmark = best_odds['handicap']
                            best_over_odds = best_odds['average']['over']
                            best_under_odds = best_odds['average']['under']
                
                # 개별 트랜잭션으로 처리
                try:
                    cursor.execute("BEGIN;")
                    
                    # SQL 삽입 쿼리 (best odds 컬럼 포함)
                    insert_query = """
                    INSERT INTO matches (id, match_link, match_time, status, home_team_id, away_team_id, home_score, away_score, season, best_benchmark, best_over_odds, best_under_odds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        match_link = EXCLUDED.match_link,
                        match_time = EXCLUDED.match_time,
                        status = EXCLUDED.status,
                        home_team_id = EXCLUDED.home_team_id,
                        away_team_id = EXCLUDED.away_team_id,
                        home_score = EXCLUDED.home_score,
                        away_score = EXCLUDED.away_score,
                        season = EXCLUDED.season,
                        best_benchmark = EXCLUDED.best_benchmark,
                        best_over_odds = EXCLUDED.best_over_odds,
                        best_under_odds = EXCLUDED.best_under_odds
                    """
                    
                    cursor.execute(insert_query, (
                        match_id,
                        match_link,
                        match_time,
                        status,
                        home_team_id,
                        away_team_id,
                        home_score,
                        away_score,
                        season,
                        best_benchmark,
                        best_over_odds,
                        best_under_odds
                    ))
                    
                    cursor.execute("COMMIT;")
                    inserted_count += 1
                    
                    # 진행 상황 출력 (100개마다)
                    if inserted_count % 100 == 0:
                        print(f"📈 진행: {inserted_count}개 삽입 완료")
                        
                except Exception as db_error:
                    cursor.execute("ROLLBACK;")
                    print(f"❌ 경기 {match_id} DB 삽입 실패: {db_error}")
                    error_count += 1
                    continue
                
            except Exception as e:
                print(f"❌ 경기 {match_id} 처리 실패: {e}")
                error_count += 1
                continue
        
        # 결과 출력
        print(f"\n📊 삽입 완료!")
        print(f"  ✅ 성공: {inserted_count}개")
        print(f"  ⚠️ 건너뜀: {skipped_count}개")
        print(f"  ❌ 오류: {error_count}개")
        print(f"  📈 총 처리: {len(data)}개")
        
        return True
        
    except Exception as e:
        print(f"❌ 데이터 삽입 중 오류: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()
        print("🔌 데이터베이스 연결 종료")

def main():
    """메인 함수"""
    
    # JSON 파일 경로 확인
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
    else:
        # 기본 파일 경로들 시도
        possible_files = [
            "src/data/soccer_england_championship-2025-2026.json",
            "src/data/soccer_england_championship-2024-2025.json",
            "soccer_england_championship-2025-2026.json"
        ]
        
        json_file = None
        for file_path in possible_files:
            if os.path.exists(file_path):
                json_file = file_path
                break
        
        if not json_file:
            print("❌ JSON 파일을 찾을 수 없습니다.")
            print("사용법: python insert_matches.py <json_file_path>")
            print("또는 src/data/ 폴더에 JSON 파일을 넣어주세요.")
            return
    
    if not os.path.exists(json_file):
        print(f"❌ 파일을 찾을 수 없습니다: {json_file}")
        return
    
    print(f"🚀 경기 데이터 삽입 시작")
    print(f"📁 파일: {json_file}")
    
    # 데이터 삽입 실행
    success = insert_matches_from_json(json_file)
    
    if success:
        print("🎉 모든 작업이 성공적으로 완료되었습니다!")
    else:
        print("💥 작업 중 오류가 발생했습니다.")

if __name__ == "__main__":
    main()
