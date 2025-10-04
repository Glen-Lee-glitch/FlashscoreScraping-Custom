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
        
        # 그리스 Super League 2 경기 데이터 조회 (두 시즌 모두 포함)
        cursor.execute("""
            SELECT id, home_score, away_score, best_benchmark, best_over_odds, best_under_odds
            FROM matches 
            WHERE season IN ('greece_super-league-2-2024-2025', 'greece_super-league-2-2025-2026')
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

def analyze_team_scoring_patterns():
    """팀별 득점 패턴 분석 (2득점 또는 3득점이 아닌 경기)"""
    
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # 그리스 Super League 2 경기 데이터 조회 (팀별 분석용)
        cursor.execute("""
            SELECT 
                m.home_team_id, m.away_team_id, m.home_score, m.away_score,
                (m.home_score + m.away_score) as total_score,
                ht.team as home_team_name, at.team as away_team_name
            FROM matches m
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            WHERE m.season IN ('greece_super-league-2-2024-2025', 'greece_super-league-2-2025-2026')
            AND m.home_score IS NOT NULL 
            AND m.away_score IS NOT NULL
            ORDER BY m.match_time
        """)
        
        matches = cursor.fetchall()
        
        if not matches:
            print("❌ 분석할 데이터가 없습니다.")
            return
        
        print(f"📊 총 {len(matches)}개 경기로 팀별 득점 패턴 분석 시작")
        print("=" * 80)
        
        # 팀별 통계 수집
        team_stats = {}
        
        for match in matches:
            home_team_name = match['home_team_name']
            away_team_name = match['away_team_name']
            total_score = match['total_score']
            
            # 홈팀 통계
            if home_team_name not in team_stats:
                team_stats[home_team_name] = {
                    'total_matches': 0,
                    'non_2_3_matches': 0,
                    'scores': []
                }
            
            team_stats[home_team_name]['total_matches'] += 1
            team_stats[home_team_name]['scores'].append(total_score)
            
            if total_score != 2 and total_score != 3:
                team_stats[home_team_name]['non_2_3_matches'] += 1
            
            # 어웨이팀 통계
            if away_team_name not in team_stats:
                team_stats[away_team_name] = {
                    'total_matches': 0,
                    'non_2_3_matches': 0,
                    'scores': []
                }
            
            team_stats[away_team_name]['total_matches'] += 1
            team_stats[away_team_name]['scores'].append(total_score)
            
            if total_score != 2 and total_score != 3:
                team_stats[away_team_name]['non_2_3_matches'] += 1
        
        # 2득점 또는 3득점이 아닌 경기 비율 계산 및 정렬
        team_ratios = []
        for team, stats in team_stats.items():
            if stats['total_matches'] > 0:
                ratio = stats['non_2_3_matches'] / stats['total_matches'] * 100
                team_ratios.append({
                    'team': team,
                    'total_matches': stats['total_matches'],
                    'non_2_3_matches': stats['non_2_3_matches'],
                    'ratio': ratio,
                    'scores': stats['scores']
                })
        
        # 비율 기준으로 내림차순 정렬
        team_ratios.sort(key=lambda x: x['ratio'], reverse=True)
        
        # 상위 3개 팀 출력
        print("🏆 2득점 또는 3득점이 '아닌' 경기 비율이 높은 상위 3개 팀:")
        print("=" * 80)
        
        for i, team_data in enumerate(team_ratios[:3]):
            team = team_data['team']
            total_matches = team_data['total_matches']
            non_2_3_matches = team_data['non_2_3_matches']
            ratio = team_data['ratio']
            scores = team_data['scores']
            
            print(f"{i+1}. {team}")
            print(f"   총 경기 수: {total_matches}개")
            print(f"   2득점 또는 3득점이 아닌 경기: {non_2_3_matches}개")
            print(f"   비율: {ratio:.1f}%")
            
            # 득점 분포 분석
            score_distribution = {}
            for score in scores:
                score_distribution[score] = score_distribution.get(score, 0) + 1
            
            print(f"   득점 분포: {dict(sorted(score_distribution.items()))}")
            print()
        
        # 전체 통계
        print("📊 전체 통계:")
        print(f"   분석된 팀 수: {len(team_ratios)}개")
        print(f"   평균 2득점/3득점 비율: {sum(t['non_2_3_matches'] for t in team_ratios) / sum(t['total_matches'] for t in team_ratios) * 100:.1f}%")
        
    except Exception as e:
        print(f"❌ 분석 중 오류: {e}")
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 데이터베이스 연결 종료")

def main():
    """메인 함수"""
    print("🚀 그리스 Super League 2 오버/언더 분석 시작")
    print("📅 시즌: 2024-2025, 2025-2026 (통합 분석)")
    print()
    
    analyze_over_under_results()
    
    print("\n" + "="*80)
    print("🏟️ 팀별 득점 패턴 분석")
    print("="*80)
    
    analyze_team_scoring_patterns()

if __name__ == "__main__":
    main()
