#!/usr/bin/env python3
"""
삽입된 odds 데이터를 확인하는 스크립트
"""

import psycopg2

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

def verify_odds_data():
    """삽입된 odds 데이터 확인"""
    
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        print("📊 Odds 데이터 현황 확인\n")
        
        # 1. 기본 통계
        print("1️⃣ 기본 통계:")
        stats_queries = [
            ("경기 수", "SELECT COUNT(*) FROM matches"),
            ("핸디캡 배당률 수", "SELECT COUNT(*) FROM handicap_odds"),
            ("북메이커 배당률 수", "SELECT COUNT(*) FROM bookmaker_odds"),
            ("메타데이터 수", "SELECT COUNT(*) FROM odds_metadata")
        ]
        
        for label, query in stats_queries:
            cursor.execute(query)
            count = cursor.fetchone()[0]
            print(f"  {label}: {count:,}개")
        
        # 2. 핸디캡별 분포
        print("\n2️⃣ 핸디캡별 분포:")
        cursor.execute("""
            SELECT handicap, COUNT(*) as count
            FROM handicap_odds 
            GROUP BY handicap 
            ORDER BY handicap
        """)
        
        handicap_stats = cursor.fetchall()
        for handicap, count in handicap_stats:
            print(f"  {handicap}: {count}개")
        
        # 3. 북메이커별 분포
        print("\n3️⃣ 북메이커별 분포:")
        cursor.execute("""
            SELECT bookmaker, COUNT(*) as count
            FROM bookmaker_odds 
            GROUP BY bookmaker 
            ORDER BY count DESC
        """)
        
        bookmaker_stats = cursor.fetchall()
        for bookmaker, count in bookmaker_stats:
            print(f"  {bookmaker}: {count}개")
        
        # 4. 뷰 테스트
        print("\n4️⃣ match_odds_summary 뷰 테스트:")
        cursor.execute("""
            SELECT match_id, home_team, away_team, season, 
                   handicap_count, bookmaker_count, last_collected
            FROM match_odds_summary 
            LIMIT 5
        """)
        
        view_results = cursor.fetchall()
        for row in view_results:
            match_id, home_team, away_team, season, handicap_count, bookmaker_count, last_collected = row
            print(f"  {home_team} vs {away_team} ({season})")
            print(f"    핸디캡: {handicap_count}개, 북메이커: {bookmaker_count}개")
        
        # 5. 인기 핸디캡 뷰 테스트
        print("\n5️⃣ popular_handicaps 뷰 테스트:")
        cursor.execute("""
            SELECT match_id, handicap, avg_over, avg_under,
                   json_array_length(bookmaker_details::json) as bookmaker_count
            FROM popular_handicaps 
            LIMIT 3
        """)
        
        popular_results = cursor.fetchall()
        for row in popular_results:
            match_id, handicap, avg_over, avg_under, bookmaker_count = row
            print(f"  경기 {match_id}: {handicap} (Over: {avg_over}, Under: {avg_under}, 북메이커: {bookmaker_count}개)")
        
        # 6. 샘플 상세 데이터
        print("\n6️⃣ 샘플 상세 데이터 (2.5 핸디캡):")
        cursor.execute("""
            SELECT m.id, t1.team as home_team, t2.team as away_team,
                   ho.avg_over, ho.avg_under,
                   json_agg(
                       json_build_object(
                           'bookmaker', bo.bookmaker,
                           'over_odds', bo.over_odds,
                           'under_odds', bo.under_odds
                       ) ORDER BY bo.bookmaker
                   ) as bookmaker_details
            FROM matches m
            JOIN teams t1 ON m.home_team_id = t1.team_id
            JOIN teams t2 ON m.away_team_id = t2.team_id
            JOIN handicap_odds ho ON m.id = ho.match_id
            JOIN bookmaker_odds bo ON ho.id = bo.handicap_id
            WHERE ho.handicap = 2.5
            GROUP BY m.id, t1.team, t2.team, ho.avg_over, ho.avg_under
            LIMIT 2
        """)
        
        sample_results = cursor.fetchall()
        for row in sample_results:
            match_id, home_team, away_team, avg_over, avg_under, bookmaker_details = row
            print(f"  {home_team} vs {away_team}")
            print(f"    평균 배당률 - Over: {avg_over}, Under: {avg_under}")
            print(f"    북메이커별 배당률:")
            
            import json
            # bookmaker_details가 이미 리스트인 경우와 문자열인 경우 처리
            if isinstance(bookmaker_details, str):
                bookmakers = json.loads(bookmaker_details)
            else:
                bookmakers = bookmaker_details
            for bookmaker_data in bookmakers:
                bookmaker = bookmaker_data['bookmaker']
                over_odds = bookmaker_data['over_odds']
                under_odds = bookmaker_data['under_odds']
                print(f"      {bookmaker}: Over {over_odds}, Under {under_odds}")
            print()
        
        print("✅ 데이터 검증 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 데이터 검증 중 오류: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()
        print("🔌 데이터베이스 연결 종료")

def main():
    """메인 함수"""
    print("🔍 Odds 데이터 검증 시작...")
    
    success = verify_odds_data()
    
    if success:
        print("\n🎉 데이터 검증 성공!")
    else:
        print("\n💥 데이터 검증 실패!")

if __name__ == "__main__":
    main()
