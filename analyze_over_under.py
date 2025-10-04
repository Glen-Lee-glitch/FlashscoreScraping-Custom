#!/usr/bin/env python3
"""
그리스 Super League 2 데이터의 오버/언더 분석 스크립트
best_benchmark와 실제 총점수를 비교하여 5가지 결과 분석
"""

import json
import psycopg2
from psycopg2.extras import RealDictCursor
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

def analyze_over_under_results():
    """오버/언더 결과 분석"""
    
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # 그리스 Super League 2 경기 데이터 조회
        cursor.execute("""
            SELECT id, home_score, away_score, best_benchmark, best_over_odds, best_under_odds
            FROM matches 
            WHERE season = 'greece_super-league-2-2025-2026'
            AND home_score IS NOT NULL 
            AND away_score IS NOT NULL
            AND best_benchmark IS NOT NULL
            ORDER BY match_time
        """)
        
        matches = cursor.fetchall()
        
        if not matches:
            print("❌ 분석할 데이터가 없습니다.")
            return
        
        print(f"📊 총 {len(matches)}개 경기 분석 시작")
        print("=" * 80)
        
        # 결과 카운터
        results = {
            'over': 0,
            'under': 0,
            'half_under': 0,
            'half_over': 0,
            'push': 0
        }
        
        # 상세 결과 저장
        detailed_results = []
        
        for match in matches:
            match_id = match['id']
            home_score = int(match['home_score'])
            away_score = int(match['away_score'])
            total_score = home_score + away_score
            benchmark = float(match['best_benchmark'])
            
            # 결과 판정
            result_type = classify_result(total_score, benchmark)
            results[result_type] += 1
            
            # 상세 정보 저장
            detailed_results.append({
                'match_id': match_id,
                'home_score': home_score,
                'away_score': away_score,
                'total_score': total_score,
                'benchmark': benchmark,
                'result': result_type
            })
            
            print(f"[{match_id[:8]}] {home_score}-{away_score} (총 {total_score}) vs 기준점 {benchmark} → {result_type}")
        
        # 결과 출력
        print("=" * 80)
        print("📈 분석 결과")
        print("=" * 80)
        
        total_matches = len(matches)
        
        for result_type, count in results.items():
            percentage = (count / total_matches) * 100
            print(f"{result_type:12}: {count:2d}개 ({percentage:5.1f}%)")
        
        print("=" * 80)
        
        # 결과 타입별 상세 분석
        print("\n📋 상세 분석:")
        for result_type in ['over', 'under', 'half_over', 'half_under', 'push']:
            matches_of_type = [r for r in detailed_results if r['result'] == result_type]
            if matches_of_type:
                print(f"\n🔸 {result_type.upper()} ({len(matches_of_type)}개):")
                for match in matches_of_type:
                    print(f"  [{match['match_id'][:8]}] {match['home_score']}-{match['away_score']} vs {match['benchmark']}")
        
        # 통계 요약
        print("\n📊 통계 요약:")
        print(f"  총 경기 수: {total_matches}개")
        print(f"  오버율: {(results['over'] + results['half_over']) / total_matches * 100:.1f}%")
        print(f"  언더율: {(results['under'] + results['half_under']) / total_matches * 100:.1f}%")
        print(f"  적특율: {results['push'] / total_matches * 100:.1f}%")
        
    except Exception as e:
        print(f"❌ 분석 중 오류: {e}")
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 데이터베이스 연결 종료")

def classify_result(total_score, benchmark):
    """
    총점수와 기준점을 비교하여 결과 분류
    
    Args:
        total_score: 실제 총점수 (home_score + away_score)
        benchmark: best_benchmark 값
    
    Returns:
        'over', 'under', 'half_over', 'half_under', 'push' 중 하나
    """
    
    # 기준점이 정수인 경우
    if benchmark == int(benchmark):
        benchmark_int = int(benchmark)
        
        if total_score > benchmark_int:
            return 'over'
        elif total_score < benchmark_int:
            return 'under'
        else:
            return 'push'  # 적특
    
    # 기준점이 소수점인 경우
    benchmark_str = str(benchmark)
    
    if benchmark_str.endswith('.25'):
        # 반언더: x.25에서 총점수가 x인 경우
        benchmark_int = int(benchmark)
        if total_score == benchmark_int:
            return 'half_under'
        elif total_score > benchmark_int:
            return 'over'
        else:
            return 'under'
    
    elif benchmark_str.endswith('.75'):
        # 반오버: x.75에서 총점수가 x+1인 경우
        benchmark_int = int(benchmark)
        if total_score == benchmark_int + 1:
            return 'half_over'
        elif total_score > benchmark_int + 1:
            return 'over'
        else:
            return 'under'
    
    elif benchmark_str.endswith('.5'):
        # 0.5 기준점 (일반적인 경우)
        benchmark_int = int(benchmark)
        if total_score > benchmark:
            return 'over'
        else:
            return 'under'
    
    else:
        # 기타 소수점 기준점
        if total_score > benchmark:
            return 'over'
        elif total_score < benchmark:
            return 'under'
        else:
            return 'push'

def main():
    """메인 함수"""
    print("🚀 그리스 Super League 2 오버/언더 분석 시작")
    print("📅 시즌: 2025-2026")
    print()
    
    analyze_over_under_results()

if __name__ == "__main__":
    main()
