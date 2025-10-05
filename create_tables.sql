-- PostgreSQL 테이블 생성 스크립트
-- Flashscore 데이터를 위한 완전한 스키마

-- 1. teams 테이블 (팀 정보)
CREATE TABLE teams (
    team_id TEXT PRIMARY KEY,
    team TEXT NOT NULL,
    sport_type TEXT DEFAULT 'soccer',
    nation TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. matches 테이블 (경기 정보)
CREATE TABLE matches (
    id TEXT PRIMARY KEY,
    match_link TEXT,
    match_time TIMESTAMPTZ NOT NULL,
    status TEXT,
    home_team_id TEXT REFERENCES teams(team_id),
    away_team_id TEXT REFERENCES teams(team_id),
    home_score SMALLINT,
    away_score SMALLINT,
    season TEXT,
    best_benchmark NUMERIC(4, 2),
    best_over_odds NUMERIC(8, 2),
    best_under_odds NUMERIC(8, 2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. match_statistics 테이블 (경기 통계)
CREATE TABLE match_statistics (
    id BIGSERIAL PRIMARY KEY,
    match_id TEXT NOT NULL REFERENCES matches(id),
    category TEXT NOT NULL,
    home_value TEXT NOT NULL,
    away_value TEXT NOT NULL,
    UNIQUE(match_id, category)
);

-- 4. handicap_odds 테이블 (핸디캡 배당률)
CREATE TABLE handicap_odds (
    id BIGSERIAL PRIMARY KEY,
    match_id TEXT NOT NULL REFERENCES matches(id),
    handicap NUMERIC(4, 2) NOT NULL,
    avg_over NUMERIC(8, 2),
    avg_under NUMERIC(8, 2)
);

-- 5. bookmaker_odds 테이블 (북메이커별 배당률)
CREATE TABLE bookmaker_odds (
    id BIGSERIAL PRIMARY KEY,
    handicap_id BIGINT NOT NULL REFERENCES handicap_odds(id),
    bookmaker TEXT NOT NULL,
    over_odds NUMERIC(8, 2),
    under_odds NUMERIC(8, 2)
);

-- 인덱스 생성 (성능 향상)
CREATE INDEX idx_matches_match_time ON matches(match_time);
CREATE INDEX idx_matches_home_team ON matches(home_team_id);
CREATE INDEX idx_matches_away_team ON matches(away_team_id);
CREATE INDEX idx_match_statistics_match_id ON match_statistics(match_id);
CREATE INDEX idx_handicap_odds_match_id ON handicap_odds(match_id);
CREATE INDEX idx_bookmaker_odds_handicap_id ON bookmaker_odds(handicap_id);
