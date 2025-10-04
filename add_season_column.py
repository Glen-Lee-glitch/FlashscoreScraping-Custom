#!/usr/bin/env python3
"""
matches 테이블에 season 컬럼을 추가하고 기존 데이터를 업데이트하는 스크립트
"""

import psycopg2
import os
import sys
import re

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

def add_season_column():
    """matches 테이블에 season 컬럼 추가"""
    
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # 1. season 컬럼 추가 (개별 트랜잭션)
        print("🔧 season 컬럼 추가 중...")
        try:
            cursor.execute("BEGIN;")
            cursor.execute("""
                ALTER TABLE matches 
                ADD COLUMN season TEXT
            """)
            cursor.execute("COMMIT;")
            print("✅ season 컬럼 추가 완료")
        except psycopg2.errors.DuplicateColumn:
            print("ℹ️ season 컬럼이 이미 존재합니다.")
            cursor.execute("ROLLBACK;")
        except Exception as e:
            print(f"⚠️ season 컬럼 추가 중 오류: {e}")
            cursor.execute("ROLLBACK;")
        
        # 2. 기존 데이터의 season 업데이트 (개별 트랜잭션)
        print("🔄 기존 데이터 season 업데이트 중...")
        try:
            cursor.execute("BEGIN;")
            cursor.execute("""
                UPDATE matches 
                SET season = 'england_championship-2025-2026'
                WHERE season IS NULL
            """)
            updated_count = cursor.rowcount
            cursor.execute("COMMIT;")
            print(f"✅ {updated_count}개 경기의 season 업데이트 완료")
        except Exception as e:
            print(f"❌ season 업데이트 중 오류: {e}")
            cursor.execute("ROLLBACK;")
            return False
        
        print("🎉 season 컬럼 추가 및 데이터 업데이트 완료!")
        return True
        
    except Exception as e:
        print(f"❌ season 컬럼 추가 중 오류: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()
        print("🔌 데이터베이스 연결 종료")

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

def update_season_from_json(json_file_path):
    """JSON 파일명 기반으로 해당 경기들의 season 업데이트"""
    
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # 파일명에서 season 추출
        season = extract_season_from_filename(json_file_path)
        print(f"📁 파일명: {json_file_path}")
        print(f"🎯 추출된 season: {season}")
        
        # 해당 season으로 업데이트 (개별 트랜잭션)
        try:
            cursor.execute("BEGIN;")
            cursor.execute("""
                UPDATE matches 
                SET season = %s
                WHERE season IS NULL OR season != %s
            """, (season, season))
            
            updated_count = cursor.rowcount
            cursor.execute("COMMIT;")
            print(f"✅ {updated_count}개 경기의 season을 '{season}'으로 업데이트 완료")
            
        except Exception as e:
            cursor.execute("ROLLBACK;")
            print(f"❌ season 업데이트 중 오류: {e}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ season 업데이트 중 오류: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()
        print("🔌 데이터베이스 연결 종료")

def main():
    """메인 함수"""
    
    print("🚀 matches 테이블 season 컬럼 추가 및 업데이트 시작")
    
    # 1. season 컬럼 추가
    success1 = add_season_column()
    
    if not success1:
        print("💥 season 컬럼 추가 실패")
        return
    
    # 2. JSON 파일이 제공된 경우 해당 파일의 season으로 업데이트
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
        if os.path.exists(json_file):
            print(f"\n🔄 JSON 파일 기반 season 업데이트")
            success2 = update_season_from_json(json_file)
            
            if success2:
                print("🎉 모든 작업이 성공적으로 완료되었습니다!")
            else:
                print("💥 season 업데이트 실패")
        else:
            print(f"❌ 파일을 찾을 수 없습니다: {json_file}")
    else:
        print("🎉 season 컬럼 추가가 완료되었습니다!")
        print("💡 특정 JSON 파일의 season으로 업데이트하려면:")
        print("   python add_season_column.py <json_file_path>")

if __name__ == "__main__":
    main()
