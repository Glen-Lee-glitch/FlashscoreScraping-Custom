#!/usr/bin/env python3
"""
matches 테이블에 match_link 컬럼을 추가하는 스크립트
"""

import psycopg2
import os
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

def add_match_link_column():
    """matches 테이블에 match_link 컬럼 추가"""
    
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # match_link 컬럼 추가 (개별 트랜잭션)
        print("🔧 match_link 컬럼 추가 중...")
        try:
            cursor.execute("BEGIN;")
            cursor.execute("""
                ALTER TABLE matches 
                ADD COLUMN match_link TEXT
            """)
            cursor.execute("COMMIT;")
            print("✅ match_link 컬럼 추가 완료")
        except psycopg2.errors.DuplicateColumn:
            print("ℹ️ match_link 컬럼이 이미 존재합니다.")
            cursor.execute("ROLLBACK;")
        except Exception as e:
            print(f"⚠️ match_link 컬럼 추가 중 오류: {e}")
            cursor.execute("ROLLBACK;")
        
        print("🎉 match_link 컬럼 추가 완료!")
        return True
        
    except Exception as e:
        print(f"❌ match_link 컬럼 추가 중 오류: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()
        print("🔌 데이터베이스 연결 종료")

def main():
    """메인 함수"""
    
    print("🚀 matches 테이블 match_link 컬럼 추가 시작")
    
    # match_link 컬럼 추가
    success = add_match_link_column()
    
    if success:
        print("🎉 match_link 컬럼 추가가 완료되었습니다!")
        print("💡 이제 새로운 JSON 데이터를 삽입하면 match_link가 포함됩니다.")
    else:
        print("💥 match_link 컬럼 추가 실패")

if __name__ == "__main__":
    main()
