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

export const insertMatchesBatch = async (matchesData, seasonYear = null, baseUrl = '', countryCode = '', leagueCode = '') => {
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
        
        // 1. 오버-언더 차이의 절댓값이 가장 작은 기준점 찾기
        // 2. 같은 절댓값이면 오버배당률이 더 높은 것 선택
        let bestHandicap = null;
        let minDifference = Infinity;
        let maxOverOdds = 0;
        
        overUnderOdds.forEach(odds => {
          if (odds.average && odds.average.over && odds.average.under) {
            const overOdds = parseFloat(odds.average.over);
            const underOdds = parseFloat(odds.average.under);
            const difference = Math.abs(overOdds - underOdds);
            
            // 차이가 더 작거나, 같은 차이면 오버배당률이 더 높은 경우 선택
            if (difference < minDifference || 
                (difference === minDifference && overOdds > maxOverOdds)) {
              minDifference = difference;
              maxOverOdds = overOdds;
              bestHandicap = odds;
            }
          }
        });
        
        if (bestHandicap) {
          bestBenchmark = parseFloat(bestHandicap.handicap.replace(',', '.'));
          bestOverOdds = bestHandicap.average.over;
          bestUnderOdds = bestHandicap.average.under;
        }
      }

      // 매치 삽입
      const insertMatchQuery = `
        INSERT INTO matches (
          id, match_link, match_time, status, 
          home_team_id, away_team_id, home_score, away_score, 
          season, nation, league, best_benchmark, best_over_odds, best_under_odds
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (id) DO UPDATE SET
          match_link = EXCLUDED.match_link,
          status = EXCLUDED.status,
          home_score = EXCLUDED.home_score,
          away_score = EXCLUDED.away_score,
          season = EXCLUDED.season,
          nation = EXCLUDED.nation,
          league = EXCLUDED.league,
          best_benchmark = EXCLUDED.best_benchmark,
          best_over_odds = EXCLUDED.best_over_odds,
          best_under_odds = EXCLUDED.best_under_odds
      `;

      const homeScore = matchInfo.result?.home ? parseInt(matchInfo.result.home) : null;
      const awayScore = matchInfo.result?.away ? parseInt(matchInfo.result.away) : null;
      const matchUrl = matchInfo.match_link || `${baseUrl}/match/${matchId}/`;
      const season = seasonYear || extractSeasonFromData(matchInfo, matchUrl);

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
        countryCode,
        leagueCode,
        bestBenchmark,
        bestOverOdds,
        bestUnderOdds
      ]);

      results.success++;
      console.log(`✅ 매치 삽입 성공: ${matchId}`);

      // 경기 이벤트 삽입 (match_events 테이블)
      if (matchInfo.events && matchInfo.events.events && matchInfo.events.events.length > 0) {
        try {
          // JSONB 형태로 이벤트 데이터 구성
          const eventsData = {
            events: matchInfo.events.events.map(event => ({
              type: event.eventType,
              time: event.time,
              team: event.team,
              description: event.description,
              player: event.player || null,
              assist: event.assist || null,
              player_out: event.player_out || null,
              player_in: event.player_in || null,
              card_type: event.card_type || null
            }))
          };

          const insertEventsQuery = `
            INSERT INTO match_events (
              match_id, first_half_score, second_half_score, events
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT (match_id) DO UPDATE SET
              first_half_score = EXCLUDED.first_half_score,
              second_half_score = EXCLUDED.second_half_score,
              events = EXCLUDED.events,
              updated_at = NOW()
          `;

          await pool.query(insertEventsQuery, [
            matchId,
            matchInfo.events.firstHalfScore,
            matchInfo.events.secondHalfScore,
            JSON.stringify(eventsData)
          ]);

          console.log(`✅ 경기 이벤트 삽입 성공: ${matchId} (${matchInfo.events.events.length}개 이벤트)`);
        } catch (eventsError) {
          const errorMsg = `경기 이벤트 ${matchId} 삽입 실패: ${eventsError.message}`;
          console.error(`❌ ${errorMsg}`);
          results.errors.push(errorMsg);
        }
      } else {
        console.log(`ℹ️ 매치 ${matchId}에는 이벤트 데이터가 없습니다.`);
      }

    } catch (error) {
      const errorMsg = `매치 ${matchId} 삽입 실패: ${error.message}`;
      console.error(`❌ ${errorMsg}`);
      results.errors.push(errorMsg);
    }
  }

  console.log(`📊 배치 삽입 완료: 성공 ${results.success}개, 실패 ${results.errors.length}개`);
  return results;
};

const extractSeasonFromData = (matchInfo, matchUrl = null) => {
  // URL에서 시즌 추출 시도 (우선순위 1)
  if (matchUrl) {
    const seasonMatch = matchUrl.match(/(\d{4}-\d{4})/);
    if (seasonMatch) {
      return seasonMatch[1];
    }
  }
  
  // match_link에서 시즌 추출 시도 (우선순위 2)
  if (matchInfo.match_link) {
    const seasonMatch = matchInfo.match_link.match(/(\d{4}-\d{4})/);
    if (seasonMatch) {
      return seasonMatch[1];
    }
  }
  
  // stage 정보에서 시즌 추출 시도 (우선순위 3)
  if (matchInfo.stage && matchInfo.stage.includes('2025-2026')) {
    return '2025-2026';
  }
  if (matchInfo.stage && matchInfo.stage.includes('2024-2025')) {
    return '2024-2025';
  }
  
  // 기본적으로 현재 년도 기반으로 계산
  const currentYear = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1;
  
  if (currentMonth >= 8) {
    return `${currentYear}-${currentYear + 1}`;
  } else {
    return `${currentYear - 1}-${currentYear}`;
  }
};

export const logMatchError = async (matchId, errorType, errorMessage, errorDetails = null, matchUrl = null, stage = null) => {
  const pool = getDatabasePool();
  
  try {
    await pool.query(`
      INSERT INTO matches_error (
        match_id, error_type, error_message, error_details, 
        match_url, stage, attempted_at
      ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
      ON CONFLICT DO NOTHING
    `, [
      matchId, 
      errorType, 
      errorMessage, 
      errorDetails ? JSON.stringify(errorDetails) : null,
      matchUrl,
      stage
    ]);
    
    console.log(`📝 오류 기록: ${matchId} - ${errorType}: ${errorMessage}`);
  } catch (error) {
    console.error(`❌ 오류 기록 실패 (${matchId}): ${error.message}`);
  }
};

export const closeDatabase = async () => {
  if (pool) {
    await pool.end();
    pool = null;
    console.log('🔌 데이터베이스 연결 종료');
  }
};
