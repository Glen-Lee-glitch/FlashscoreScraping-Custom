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

export const closeDatabase = async () => {
  if (pool) {
    await pool.end();
    pool = null;
    console.log('🔌 데이터베이스 연결 종료');
  }
};
