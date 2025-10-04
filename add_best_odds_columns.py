#!/usr/bin/env python3
"""
matches 테이블에 best odds 컬럼들을 추가하는 스크립트
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

def add_best_odds_columns():
    """matches 테이블에 best odds 컬럼들 추가"""
    
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # best odds 컬럼들 추가 (개별 트랜잭션)
        columns_to_add = [
            ("best_benchmark", "NUMERIC(4, 2)"),
            ("best_over_odds", "NUMERIC(8, 2)"),
            ("best_under_odds", "NUMERIC(8, 2)")
        ]
        
        for column_name, column_type in columns_to_add:
            print(f"🔧 {column_name} 컬럼 추가 중...")
            try:
                cursor.execute("BEGIN;")
                cursor.execute(f"""
                    ALTER TABLE matches 
                    ADD COLUMN {column_name} {column_type}
                """)
                cursor.execute("COMMIT;")
                print(f"✅ {column_name} 컬럼 추가 완료")
            except psycopg2.errors.DuplicateColumn:
                print(f"ℹ️ {column_name} 컬럼이 이미 존재합니다.")
                cursor.execute("ROLLBACK;")
            except Exception as e:
                print(f"⚠️ {column_name} 컬럼 추가 중 오류: {e}")
                cursor.execute("ROLLBACK;")
        
        print("🎉 best odds 컬럼들 추가 완료!")
        return True
        
    except Exception as e:
        print(f"❌ best odds 컬럼 추가 중 오류: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()
        print("🔌 데이터베이스 연결 종료")

def main():
    """메인 함수"""
    
    print("🚀 matches 테이블 best odds 컬럼들 추가 시작")
    
    # best odds 컬럼들 추가
    success = add_best_odds_columns()
    
    if success:
        print("🎉 best odds 컬럼들 추가가 완료되었습니다!")
        print("💡 이제 새로운 JSON 데이터를 삽입하면 best odds가 포함됩니다.")
    else:
        print("💥 best odds 컬럼들 추가 실패")

if __name__ == "__main__":
    main()
