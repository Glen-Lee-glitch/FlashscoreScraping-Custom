import pkg from 'pg';
const { Pool } = pkg;

// Supabase 데이터베이스 설정
const DB_CONFIG = {
  host: "aws-1-ap-northeast-2.pooler.supabase.com",
  database: "postgres",
  user: "postgres.dvwwcmhzlllaukscjuya",
  password: "!Qdhdbrclf56",
  port: "6543"
};

let pool = null;

export const initializeDatabase = () => {
  if (!pool) {
    pool = new Pool(DB_CONFIG);
    console.log('✅ 데이터베이스 연결 풀 초기화 완료');
  }
  return pool;
};

export const getDatabasePool = () => {
  if (!pool) {
    initializeDatabase();
  }
  return pool;
};

export const checkMatchExists = async (matchId) => {
  const pool = getDatabasePool();
  
  try {
    const query = 'SELECT id FROM matches WHERE id = $1';
    const result = await pool.query(query, [matchId]);
    
    return result.rows.length > 0;
  } catch (error) {
    console.error(`❌ 매치 존재 확인 실패 (${matchId}): ${error.message}`);
    // 에러 발생 시 안전하게 false 반환 (스크래핑 계속 진행)
    return false;
  }
};

export const getExistingMatchIds = async () => {
  const pool = getDatabasePool();
  
  try {
    const query = 'SELECT id FROM matches';
    const result = await pool.query(query);
    
    const matchIds = new Set(result.rows.map(row => row.id));
    console.log(`📊 데이터베이스에서 ${matchIds.size}개의 기존 매치 ID 로드 완료`);
    
    return matchIds;
  } catch (error) {
    console.error(`❌ 기존 매치 ID 로드 실패: ${error.message}`);
    // 에러 발생 시 빈 Set 반환 (스크래핑 계속 진행)
    return new Set();
  }
};

export const insertMatchesBatch = async (matchesData) => {
  const pool = getDatabasePool();
  
  if (!matchesData || Object.keys(matchesData).length === 0) {
    return { success: 0, errors: [] };
  }

  const results = { success: 0, errors: [] };
  
  for (const [matchId, matchInfo] of Object.entries(matchesData)) {
    try {
      // 팀 정보 먼저 삽입 (중복 시 무시)
      if (matchInfo.home?.id && matchInfo.home?.name) {
        await pool.query(
          'INSERT INTO teams (team_id, team, sport_type, nation) VALUES ($1, $2, $3, $4) ON CONFLICT (team_id) DO NOTHING',
          [matchInfo.home.id, matchInfo.home.name, 'soccer', 'italy']
        );
      }
      
      if (matchInfo.away?.id && matchInfo.away?.name) {
        await pool.query(
          'INSERT INTO teams (team_id, team, sport_type, nation) VALUES ($1, $2, $3, $4) ON CONFLICT (team_id) DO NOTHING',
          [matchInfo.away.id, matchInfo.away.name, 'soccer', 'italy']
        );
      }

      // 경기 시간 파싱
      let matchTime = null;
      if (matchInfo.date) {
        try {
          // 다양한 날짜 형식 지원
          const dateMatch = matchInfo.date.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
          if (dateMatch) {
            const [, day, month, year] = dateMatch;
            matchTime = new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T00:00:00Z`);
          }
        } catch (e) {
          console.log(`⚠️ 날짜 파싱 실패 (${matchId}): ${matchInfo.date}`);
        }
      }

      // 베스트 오버/언더 배당률 추출
      let bestBenchmark = null;
      let bestOverOdds = null;
      let bestUnderOdds = null;
      
      if (matchInfo.odds?.['over-under']) {
        const overUnderOdds = matchInfo.odds['over-under'];
        // 2.5 기준점 찾기 (가장 일반적)
        const benchmark25 = overUnderOdds.find(odds => odds.handicap === '2.5' || odds.handicap === '2,5');
        if (benchmark25 && benchmark25.average) {
          bestBenchmark = parseFloat(benchmark25.average.over) > parseFloat(benchmark25.average.under) ? 2.5 : 2.5;
          bestOverOdds = benchmark25.average.over;
          bestUnderOdds = benchmark25.average.under;
        }
      }

      // 매치 삽입
      const insertMatchQuery = `
        INSERT INTO matches (
          id, match_link, match_time, status, 
          home_team_id, away_team_id, home_score, away_score, 
          season, best_benchmark, best_over_odds, best_under_odds
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (id) DO UPDATE SET
          match_link = EXCLUDED.match_link,
          status = EXCLUDED.status,
          home_score = EXCLUDED.home_score,
          away_score = EXCLUDED.away_score,
          best_benchmark = EXCLUDED.best_benchmark,
          best_over_odds = EXCLUDED.best_over_odds,
          best_under_odds = EXCLUDED.best_under_odds
      `;

      const homeScore = matchInfo.result?.home ? parseInt(matchInfo.result.home) : null;
      const awayScore = matchInfo.result?.away ? parseInt(matchInfo.result.away) : null;
      const season = extractSeasonFromData(matchInfo);

      await pool.query(insertMatchQuery, [
        matchId,
        matchInfo.match_link || null,
        matchTime,
        matchInfo.status || null,
        matchInfo.home?.id || null,
        matchInfo.away?.id || null,
        homeScore,
        awayScore,
        season,
        bestBenchmark,
        bestOverOdds,
        bestUnderOdds
      ]);

      results.success++;
      console.log(`✅ 매치 삽입 성공: ${matchId}`);

    } catch (error) {
      const errorMsg = `매치 ${matchId} 삽입 실패: ${error.message}`;
      console.error(`❌ ${errorMsg}`);
      results.errors.push(errorMsg);
    }
  }

  console.log(`📊 배치 삽입 완료: 성공 ${results.success}개, 실패 ${results.errors.length}개`);
  return results;
};

const extractSeasonFromData = (matchInfo) => {
  // matchInfo에서 시즌 정보 추출 로직
  // 기본적으로 현재 년도 기반으로 계산
  const currentYear = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1;
  
  if (currentMonth >= 8) {
    return `${currentYear}-${currentYear + 1}`;
  } else {
    return `${currentYear - 1}-${currentYear}`;
  }
};

export const closeDatabase = async () => {
  if (pool) {
    await pool.end();
    pool = null;
    console.log('🔌 데이터베이스 연결 종료');
  }
};
