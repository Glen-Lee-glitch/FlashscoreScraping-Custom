#!/usr/bin/env python3
"""
Flashscore JSON 데이터에서 팀 정보를 추출하여 teams 테이블에 삽입하는 스크립트
"""

import json
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

def extract_teams_from_json(json_file_path):
    """JSON 파일에서 팀 정보 추출"""
    
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
        
        # 팀 정보 추출
        teams = set()  # 중복 제거를 위해 set 사용
        
        for match_id, match_data in data.items():
            # 홈팀 정보
            if 'home' in match_data:
                home_team = match_data['home']
                if 'id' in home_team and 'name' in home_team:
                    teams.add((
                        home_team['id'],
                        home_team['name'],
                        'soccer',  # 기본값
                        'england'  # 기본값 (파일명에서 추출 가능)
                    ))
            
            # 어웨이팀 정보
            if 'away' in match_data:
                away_team = match_data['away']
                if 'id' in away_team and 'name' in away_team:
                    teams.add((
                        away_team['id'],
                        away_team['name'],
                        'soccer',  # 기본값
                        'england'  # 기본값
                    ))
        
        print(f"📊 총 {len(teams)}개 팀 발견")
        
        # 팀 데이터 삽입
        inserted_count = 0
        skipped_count = 0
        
        for team_id, team_name, sport_type, nation in teams:
            try:
                cursor.execute("BEGIN;")
                
                insert_query = """
                INSERT INTO teams (team_id, team, sport_type, nation)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (team_id) DO UPDATE SET
                    team = EXCLUDED.team,
                    sport_type = EXCLUDED.sport_type,
                    nation = EXCLUDED.nation
                """
                
                cursor.execute(insert_query, (team_id, team_name, sport_type, nation))
                cursor.execute("COMMIT;")
                inserted_count += 1
                
            except Exception as e:
                cursor.execute("ROLLBACK;")
                print(f"❌ 팀 {team_name} ({team_id}) 삽입 실패: {e}")
                skipped_count += 1
        
        print(f"\n📊 팀 삽입 완료!")
        print(f"  ✅ 성공: {inserted_count}개")
        print(f"  ❌ 실패: {skipped_count}개")
        
        return True
        
    except Exception as e:
        print(f"❌ 팀 데이터 처리 중 오류: {e}")
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
            print("사용법: python insert_teams.py <json_file_path>")
            return
    
    if not os.path.exists(json_file):
        print(f"❌ 파일을 찾을 수 없습니다: {json_file}")
        return
    
    print(f"🚀 팀 데이터 삽입 시작")
    print(f"📁 파일: {json_file}")
    
    # 팀 데이터 삽입 실행
    success = extract_teams_from_json(json_file)
    
    if success:
        print("🎉 모든 팀 데이터가 성공적으로 삽입되었습니다!")
    else:
        print("💥 팀 데이터 삽입 중 오류가 발생했습니다.")

if __name__ == "__main__":
    main()
