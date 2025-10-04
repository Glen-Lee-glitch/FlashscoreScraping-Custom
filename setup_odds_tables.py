#!/usr/bin/env python3
"""
Odds 테이블들을 생성하는 스크립트
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

def create_odds_tables():
    """Odds 관련 테이블들 생성"""
    
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # 1. handicap_odds 테이블 생성
        print("📋 handicap_odds 테이블 생성 중...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS handicap_odds (
                id BIGSERIAL PRIMARY KEY,
                match_id TEXT NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
                handicap NUMERIC(4, 2) NOT NULL,
                avg_over NUMERIC(8, 2),
                avg_under NUMERIC(8, 2),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(match_id, handicap)
            )
        """)
        
        # 2. bookmaker_odds 테이블 생성
        print("📋 bookmaker_odds 테이블 생성 중...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bookmaker_odds (
                id BIGSERIAL PRIMARY KEY,
                handicap_id BIGINT NOT NULL REFERENCES handicap_odds(id) ON DELETE CASCADE,
                bookmaker TEXT NOT NULL,
                over_odds NUMERIC(8, 2),
                under_odds NUMERIC(8, 2),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(handicap_id, bookmaker)
            )
        """)
        
        # 3. odds_metadata 테이블 생성
        print("📋 odds_metadata 테이블 생성 중...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS odds_metadata (
                id BIGSERIAL PRIMARY KEY,
                match_id TEXT NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
                collected_at TIMESTAMPTZ DEFAULT NOW(),
                source TEXT DEFAULT 'flashscore',
                bookmaker_count INTEGER,
                handicap_count INTEGER,
                notes TEXT
            )
        """)
        
        # 4. 인덱스 생성
        print("📋 인덱스 생성 중...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_handicap_odds_match_handicap ON handicap_odds(match_id, handicap)",
            "CREATE INDEX IF NOT EXISTS idx_handicap_odds_handicap ON handicap_odds(handicap)",
            "CREATE INDEX IF NOT EXISTS idx_bookmaker_odds_handicap_bookmaker ON bookmaker_odds(handicap_id, bookmaker)",
            "CREATE INDEX IF NOT EXISTS idx_bookmaker_odds_bookmaker ON bookmaker_odds(bookmaker)",
            "CREATE INDEX IF NOT EXISTS idx_odds_metadata_match_collected ON odds_metadata(match_id, collected_at)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # 5. 트리거 함수 생성
        print("📋 트리거 함수 생성 중...")
        cursor.execute("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ language 'plpgsql'
        """)
        
        # 6. 트리거 생성
        print("📋 트리거 생성 중...")
        triggers = [
            """
            DROP TRIGGER IF EXISTS update_handicap_odds_updated_at ON handicap_odds;
            CREATE TRIGGER update_handicap_odds_updated_at 
                BEFORE UPDATE ON handicap_odds 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
            """,
            """
            DROP TRIGGER IF EXISTS update_bookmaker_odds_updated_at ON bookmaker_odds;
            CREATE TRIGGER update_bookmaker_odds_updated_at 
                BEFORE UPDATE ON bookmaker_odds 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
            """
        ]
        
        for trigger_sql in triggers:
            cursor.execute(trigger_sql)
        
        # 7. 뷰 생성
        print("📋 뷰 생성 중...")
        
        # match_odds_summary 뷰
        cursor.execute("DROP VIEW IF EXISTS match_odds_summary CASCADE")
        cursor.execute("""
            CREATE VIEW match_odds_summary AS
            SELECT 
                m.id as match_id,
                m.match_time,
                m.season,
                t1.team as home_team,
                t2.team as away_team,
                t1.nation as home_nation,
                t2.nation as away_nation,
                t1.league as home_league,
                t2.league as away_league,
                m.best_benchmark,
                m.best_over_odds,
                m.best_under_odds,
                COUNT(DISTINCT ho.handicap) as handicap_count,
                COUNT(DISTINCT bo.bookmaker) as bookmaker_count,
                MAX(om.collected_at) as last_collected
            FROM matches m
            LEFT JOIN teams t1 ON m.home_team_id = t1.team_id
            LEFT JOIN teams t2 ON m.away_team_id = t2.team_id
            LEFT JOIN handicap_odds ho ON m.id = ho.match_id
            LEFT JOIN bookmaker_odds bo ON ho.id = bo.handicap_id
            LEFT JOIN odds_metadata om ON m.id = om.match_id
            GROUP BY m.id, m.match_time, m.season, t1.team, t2.team, t1.nation, t2.nation, 
                     t1.league, t2.league, m.best_benchmark, m.best_over_odds, m.best_under_odds
        """)
        
        # popular_handicaps 뷰
        cursor.execute("DROP VIEW IF EXISTS popular_handicaps CASCADE")
        cursor.execute("""
            CREATE VIEW popular_handicaps AS
            SELECT 
                ho.match_id,
                ho.handicap,
                ho.avg_over,
                ho.avg_under,
                json_agg(
                    json_build_object(
                        'bookmaker', bo.bookmaker,
                        'over_odds', bo.over_odds,
                        'under_odds', bo.under_odds
                    ) ORDER BY bo.bookmaker
                ) as bookmaker_details
            FROM handicap_odds ho
            LEFT JOIN bookmaker_odds bo ON ho.id = bo.handicap_id
            WHERE ho.handicap IN (0.5, 1.5, 2.5, 3.5)
            GROUP BY ho.match_id, ho.handicap, ho.avg_over, ho.avg_under
        """)
        
        # 변경사항 커밋
        cursor.execute("COMMIT;")
        
        print("✅ 모든 odds 테이블 및 뷰 생성 완료!")
        
        # 테이블 정보 확인
        print("\n📊 생성된 테이블 목록:")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('handicap_odds', 'bookmaker_odds', 'odds_metadata')
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        for table in tables:
            print(f"  ✅ {table[0]}")
        
        print("\n📊 생성된 뷰 목록:")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public' 
            AND table_name IN ('match_odds_summary', 'popular_handicaps')
            ORDER BY table_name
        """)
        
        views = cursor.fetchall()
        for view in views:
            print(f"  ✅ {view[0]}")
        
        return True
        
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(f"❌ 테이블 생성 중 오류: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()
        print("🔌 데이터베이스 연결 종료")

def main():
    """메인 함수"""
    print("🚀 Odds 테이블 설정 시작...")
    
    success = create_odds_tables()
    
    if success:
        print("\n🎉 Odds 테이블 설정 완료!")
        print("\n다음 단계:")
        print("1. python insert_odds.py <json_file_path> 로 배당률 데이터 삽입")
        print("2. SELECT * FROM match_odds_summary LIMIT 10; 로 결과 확인")
    else:
        print("\n💥 Odds 테이블 설정 실패!")

if __name__ == "__main__":
    main()
