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

def select_best_odds(bookmakers_data, method='average'):
    """
    북메이커별 배당률에서 하나의 대표값 선택
    
    Args:
        bookmakers_data: 북메이커별 배당률 리스트
        method: 선택 방법 ('average', 'median', 'mode', 'best_over', 'best_under')
    
    Returns:
        tuple: (over_odds, under_odds)
    """
    if not bookmakers_data:
        return None, None
    
    # 유효한 배당률만 필터링
    valid_odds = []
    for bookmaker in bookmakers_data:
        try:
            over = float(bookmaker.get('over', 0)) if bookmaker.get('over') else None
            under = float(bookmaker.get('under', 0)) if bookmaker.get('under') else None
            if over and under and over > 1.0 and under > 1.0:
                valid_odds.append((over, under))
        except (ValueError, TypeError):
            continue
    
    if not valid_odds:
        return None, None
    
    if method == 'average':
        # 평균값 계산
        avg_over = sum(odds[0] for odds in valid_odds) / len(valid_odds)
        avg_under = sum(odds[1] for odds in valid_odds) / len(valid_odds)
        return round(avg_over, 2), round(avg_under, 2)
    
    elif method == 'median':
        # 중앙값 계산
        over_odds = sorted([odds[0] for odds in valid_odds])
        under_odds = sorted([odds[1] for odds in valid_odds])
        
        n = len(over_odds)
        if n % 2 == 0:
            median_over = (over_odds[n//2-1] + over_odds[n//2]) / 2
            median_under = (under_odds[n//2-1] + under_odds[n//2]) / 2
        else:
            median_over = over_odds[n//2]
            median_under = under_odds[n//2]
        
        return round(median_over, 2), round(median_under, 2)
    
    elif method == 'best_over':
        # 오버 배당률이 가장 높은 것 선택
        best_odds = max(valid_odds, key=lambda x: x[0])
        return round(best_odds[0], 2), round(best_odds[1], 2)
    
    elif method == 'best_under':
        # 언더 배당률이 가장 높은 것 선택
        best_odds = max(valid_odds, key=lambda x: x[1])
        return round(best_odds[0], 2), round(best_odds[1], 2)
    
    else:
        # 기본값: 평균
        return select_best_odds(bookmakers_data, 'average')

def insert_odds_from_json(json_file_path, odds_method='average'):
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
        skipped_matches = 0
        
        print(f"📊 총 {total_matches}개 경기 처리 시작...")
        print(f"🎯 배당률 선택 방법: {odds_method}")
        
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
                
                # 트랜잭션 시작
                cursor.execute("BEGIN;")
                
                # 기존 handicap_odds 삭제 (새로 수집)
                cursor.execute("DELETE FROM handicap_odds WHERE match_id = %s", (match_id,))
                
                # 메타데이터 업데이트
                bookmaker_count = sum(len(handicap.get('bookmakers', [])) for handicap in over_under_odds)
                handicap_count = len(over_under_odds)
                
                cursor.execute("""
                    INSERT INTO odds_metadata (match_id, bookmaker_count, handicap_count, source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (match_id) DO UPDATE SET
                        bookmaker_count = EXCLUDED.bookmaker_count,
                        handicap_count = EXCLUDED.handicap_count,
                        collected_at = NOW()
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
                    
                    # 북메이커별 배당률에서 대표값 선택
                    bookmakers = handicap_data.get('bookmakers', [])
                    selected_over, selected_under = select_best_odds(bookmakers, odds_method)
                    
                    # JSON에 average가 있으면 그것을 우선 사용, 없으면 선택된 값 사용
                    if 'average' in handicap_data and handicap_data['average']:
                        try:
                            avg_over = float(handicap_data['average'].get('over', 0)) if handicap_data['average'].get('over') else selected_over
                            avg_under = float(handicap_data['average'].get('under', 0)) if handicap_data['average'].get('under') else selected_under
                        except (ValueError, TypeError):
                            avg_over, avg_under = selected_over, selected_under
                    else:
                        avg_over, avg_under = selected_over, selected_under
                    
                    # handicap_odds 삽입
                    cursor.execute("""
                        INSERT INTO handicap_odds (match_id, handicap, avg_over, avg_under)
                        VALUES (%s, %s, %s, %s)
                    """, (match_id, handicap_value, avg_over, avg_under))
                    
                    inserted_handicaps += 1
                
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
    
    # 인자 확인
    if len(sys.argv) < 2:
        print("사용법: python insert_odds.py <json_file_path> [odds_method]")
        print("예시: python insert_odds.py src/data/soccer_greece_super-league-2-2025-2026.json")
        print("")
        print("배당률 선택 방법:")
        print("  average     - 평균값 (기본값)")
        print("  median      - 중앙값")
        print("  best_over   - 오버 배당률이 가장 높은 것")
        print("  best_under  - 언더 배당률이 가장 높은 것")
        return
    
    json_file_path = sys.argv[1]
    odds_method = sys.argv[2] if len(sys.argv) > 2 else 'average'
    
    # 유효한 방법인지 확인
    valid_methods = ['average', 'median', 'best_over', 'best_under']
    if odds_method not in valid_methods:
        print(f"❌ 잘못된 배당률 선택 방법: {odds_method}")
        print(f"사용 가능한 방법: {', '.join(valid_methods)}")
        return
    
    if not os.path.exists(json_file_path):
        print(f"❌ 파일을 찾을 수 없습니다: {json_file_path}")
        return
    
    print(f"🚀 배당률 삽입 시작: {json_file_path}")
    
    success = insert_odds_from_json(json_file_path, odds_method)
    
    if success:
        print("🎉 배당률 삽입 성공!")
    else:
        print("💥 배당률 삽입 실패!")

if __name__ == "__main__":
    main()
