#!/usr/bin/env python3
"""
Flashscore JSON 데이터에서 배당률 정보를 추출하여 odds 테이블에 삽입하는 스크립트
"""

import json
import psycopg2
import os
import sys
from decimal import Decimal

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

def insert_odds_from_json(json_file_path):
    """JSON 파일에서 배당률 정보 추출하여 삽입"""
    
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
        
        total_matches = len(data)
        processed_matches = 0
        inserted_handicaps = 0
        inserted_bookmakers = 0
        skipped_matches = 0
        
        print(f"📊 총 {total_matches}개 경기 처리 시작...")
        
        for match_id, match_data in data.items():
            try:
                # 경기가 matches 테이블에 존재하는지 확인
                cursor.execute("SELECT id FROM matches WHERE id = %s", (match_id,))
                if not cursor.fetchone():
                    print(f"⚠️ 경기 {match_id}가 matches 테이블에 없어서 스킵")
                    skipped_matches += 1
                    continue
                
                # odds 데이터 확인
                if 'odds' not in match_data or match_data['odds'] is None or 'over-under' not in match_data['odds']:
                    print(f"⚠️ 경기 {match_id}에 over-under 배당률 없음")
                    skipped_matches += 1
                    continue
                
                over_under_odds = match_data['odds']['over-under']
                if not over_under_odds:
                    print(f"⚠️ 경기 {match_id}에 over-under 배당률 배열이 비어있음")
                    skipped_matches += 1
                    continue
                
                # 메타데이터 삽입
                cursor.execute("BEGIN;")
                
                # 기존 odds_metadata가 있으면 삭제 (새로 수집)
                cursor.execute("DELETE FROM odds_metadata WHERE match_id = %s", (match_id,))
                
                bookmaker_count = sum(len(handicap.get('bookmakers', [])) for handicap in over_under_odds)
                handicap_count = len(over_under_odds)
                
                cursor.execute("""
                    INSERT INTO odds_metadata (match_id, bookmaker_count, handicap_count, source)
                    VALUES (%s, %s, %s, %s)
                """, (match_id, bookmaker_count, handicap_count, 'flashscore'))
                
                # 각 핸디캡별 배당률 처리
                for handicap_data in over_under_odds:
                    handicap = handicap_data.get('handicap')
                    if not handicap:
                        continue
                    
                    try:
                        # 핸디캡을 숫자로 변환 (문자열 "2.5" -> 2.5)
                        handicap_value = float(handicap.replace(',', '.'))
                    except (ValueError, AttributeError):
                        print(f"⚠️ 핸디캡 변환 실패: {handicap}")
                        continue
                    
                    # 평균 배당률 추출
                    avg_over = None
                    avg_under = None
                    if 'average' in handicap_data:
                        try:
                            avg_over = float(handicap_data['average'].get('over', 0)) if handicap_data['average'].get('over') else None
                            avg_under = float(handicap_data['average'].get('under', 0)) if handicap_data['average'].get('under') else None
                        except (ValueError, TypeError):
                            pass
                    
                    # 기존 handicap_odds 삭제 (새로 수집)
                    cursor.execute("DELETE FROM handicap_odds WHERE match_id = %s AND handicap = %s", 
                                 (match_id, handicap_value))
                    
                    # handicap_odds 삽입
                    cursor.execute("""
                        INSERT INTO handicap_odds (match_id, handicap, avg_over, avg_under)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id
                    """, (match_id, handicap_value, avg_over, avg_under))
                    
                    handicap_odds_id = cursor.fetchone()[0]
                    inserted_handicaps += 1
                    
                    # 북메이커별 상세 배당률 삽입
                    bookmakers = handicap_data.get('bookmakers', [])
                    for bookmaker_data in bookmakers:
                        bookmaker_name = bookmaker_data.get('bookmaker')
                        if not bookmaker_name:
                            continue
                        
                        try:
                            over_odds = float(bookmaker_data.get('over', 0)) if bookmaker_data.get('over') else None
                            under_odds = float(bookmaker_data.get('under', 0)) if bookmaker_data.get('under') else None
                        except (ValueError, TypeError):
                            continue
                        
                        # 북메이커 배당률 삽입
                        cursor.execute("""
                            INSERT INTO bookmaker_odds (handicap_id, bookmaker, over_odds, under_odds)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (handicap_id, bookmaker) DO UPDATE SET
                                over_odds = EXCLUDED.over_odds,
                                under_odds = EXCLUDED.under_odds,
                                updated_at = NOW()
                        """, (handicap_odds_id, bookmaker_name, over_odds, under_odds))
                        
                        inserted_bookmakers += 1
                
                cursor.execute("COMMIT;")
                processed_matches += 1
                
                if processed_matches % 100 == 0:
                    print(f"📊 진행상황: {processed_matches}/{total_matches} 경기 처리 완료")
                
            except Exception as e:
                cursor.execute("ROLLBACK;")
                print(f"❌ 경기 {match_id} 처리 실패: {e}")
                skipped_matches += 1
                continue
        
        print(f"\n📊 배당률 삽입 완료!")
        print(f"  ✅ 처리된 경기: {processed_matches}개")
        print(f"  📈 삽입된 핸디캡: {inserted_handicaps}개")
        print(f"  🎯 삽입된 북메이커 배당률: {inserted_bookmakers}개")
        print(f"  ⚠️ 스킵된 경기: {skipped_matches}개")
        
        return True
        
    except Exception as e:
        print(f"❌ 배당률 데이터 처리 중 오류: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()
        print("🔌 데이터베이스 연결 종료")

def main():
    """메인 함수"""
    
    # JSON 파일 경로 확인
    if len(sys.argv) < 2:
        print("사용법: python insert_odds.py <json_file_path>")
        print("예시: python insert_odds.py src/data/soccer_greece_super-league-2-2025-2026.json")
        return
    
    json_file_path = sys.argv[1]
    
    if not os.path.exists(json_file_path):
        print(f"❌ 파일을 찾을 수 없습니다: {json_file_path}")
        return
    
    print(f"🚀 배당률 삽입 시작: {json_file_path}")
    
    success = insert_odds_from_json(json_file_path)
    
    if success:
        print("🎉 배당률 삽입 성공!")
    else:
        print("💥 배당률 삽입 실패!")

if __name__ == "__main__":
    main()
