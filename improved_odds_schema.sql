-- 개선된 Odds 테이블 설계
-- 기존 스키마를 기반으로 최적화

-- 주의: 실제 teams 테이블 구조는 team_id, team, sport_type, nation 컬럼을 사용합니다
-- create_tables.sql의 teams 구조와 다릅니다

-- 1. handicap_odds 테이블 개선
CREATE TABLE handicap_odds (
    id BIGSERIAL PRIMARY KEY,
    match_id TEXT NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    handicap NUMERIC(4, 2) NOT NULL,
    avg_over NUMERIC(8, 2),
    avg_under NUMERIC(8, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    -- 복합 유니크 제약조건 추가
    UNIQUE(match_id, handicap)
);

-- 2. bookmaker_odds 테이블 개선
CREATE TABLE bookmaker_odds (
    id BIGSERIAL PRIMARY KEY,
    handicap_id BIGINT NOT NULL REFERENCES handicap_odds(id) ON DELETE CASCADE,
    bookmaker TEXT NOT NULL,
    over_odds NUMERIC(8, 2),
    under_odds NUMERIC(8, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    -- 복합 유니크 제약조건 추가
    UNIQUE(handicap_id, bookmaker)
);

-- 3. odds_metadata 테이블 추가 (배당률 수집 시점 관리)
CREATE TABLE odds_metadata (
    id BIGSERIAL PRIMARY KEY,
    match_id TEXT NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    source TEXT DEFAULT 'flashscore',
    bookmaker_count INTEGER,
    handicap_count INTEGER,
    notes TEXT
);

-- 4. 인덱스 최적화
CREATE INDEX idx_handicap_odds_match_handicap ON handicap_odds(match_id, handicap);
CREATE INDEX idx_handicap_odds_handicap ON handicap_odds(handicap);
CREATE INDEX idx_bookmaker_odds_handicap_bookmaker ON bookmaker_odds(handicap_id, bookmaker);
CREATE INDEX idx_bookmaker_odds_bookmaker ON bookmaker_odds(bookmaker);
CREATE INDEX idx_odds_metadata_match_collected ON odds_metadata(match_id, collected_at);

-- 5. 업데이트 트리거 함수
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 6. 트리거 적용
CREATE TRIGGER update_handicap_odds_updated_at 
    BEFORE UPDATE ON handicap_odds 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_bookmaker_odds_updated_at 
    BEFORE UPDATE ON bookmaker_odds 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 7. 뷰 생성 (편의성) - 실제 테이블 구조에 맞춤
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
         t1.league, t2.league, m.best_benchmark, m.best_over_odds, m.best_under_odds;

-- 8. 자주 사용할 배당률 조회용 뷰
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
WHERE ho.handicap IN (0.5, 1.5, 2.5, 3.5) -- 인기 핸디캡만
GROUP BY ho.match_id, ho.handicap, ho.avg_over, ho.avg_under;
