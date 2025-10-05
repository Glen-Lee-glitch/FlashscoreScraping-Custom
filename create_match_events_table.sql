-- 경기 이벤트 테이블 생성 스크립트
-- Supabase PostgreSQL용

-- 1. match_events 테이블 (경기 이벤트 정보)
CREATE TABLE match_events (
    id BIGSERIAL PRIMARY KEY,
    match_id TEXT NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL CHECK (event_type IN ('골', '교체', '카드', '기타')),
    event_time INTEGER, -- 분 단위 (예: 29, 46, 90+4)
    team TEXT CHECK (team IN ('홈', '어웨이')),
    description TEXT,
    player_name TEXT,
    assisting_player TEXT, -- 골의 경우 어시스트 선수
    card_type TEXT CHECK (card_type IN ('Yellow Card', 'Red Card')), -- 카드 타입
    substitution_out TEXT, -- 교체된 선수
    substitution_in TEXT, -- 교체된 선수
    first_half_score TEXT, -- 전반전 점수 (예: "1 - 0")
    second_half_score TEXT, -- 후반전 점수 (예: "2 - 0")
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- 이벤트 시간과 타입의 유니크 제약조건 (같은 시간에 같은 타입의 이벤트가 중복되지 않도록)
    UNIQUE(match_id, event_time, event_type, team, player_name)
);

-- 2. 인덱스 생성 (성능 향상)
CREATE INDEX idx_match_events_match_id ON match_events(match_id);
CREATE INDEX idx_match_events_type ON match_events(event_type);
CREATE INDEX idx_match_events_time ON match_events(event_time);
CREATE INDEX idx_match_events_team ON match_events(team);
CREATE INDEX idx_match_events_player ON match_events(player_name);
CREATE INDEX idx_match_events_match_type_time ON match_events(match_id, event_type, event_time);

-- 3. 업데이트 트리거 함수 (이미 존재할 경우 무시)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 4. 트리거 생성
DROP TRIGGER IF EXISTS update_match_events_updated_at ON match_events;
CREATE TRIGGER update_match_events_updated_at
    BEFORE UPDATE ON match_events
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 5. RLS (Row Level Security) 정책 (선택사항)
-- ALTER TABLE match_events ENABLE ROW LEVEL SECURITY;

-- 6. 뷰 생성 (편의를 위한 이벤트 요약)
CREATE OR REPLACE VIEW match_events_summary AS
SELECT 
    me.match_id,
    m.match_time,
    m.status,
    ht.team as home_team,
    at.team as away_team,
    me.first_half_score,
    me.second_half_score,
    COUNT(CASE WHEN me.event_type = '골' THEN 1 END) as goal_count,
    COUNT(CASE WHEN me.event_type = '교체' THEN 1 END) as substitution_count,
    COUNT(CASE WHEN me.event_type = '카드' THEN 1 END) as card_count,
    ARRAY_AGG(
        CASE WHEN me.event_type = '골' 
        THEN json_build_object(
            'time', me.event_time,
            'team', me.team,
            'player', me.player_name,
            'assist', me.assisting_player
        )
        END
    ) FILTER (WHERE me.event_type = '골') as goals,
    ARRAY_AGG(
        CASE WHEN me.event_type = '카드' 
        THEN json_build_object(
            'time', me.event_time,
            'team', me.team,
            'player', me.player_name,
            'card_type', me.card_type
        )
        END
    ) FILTER (WHERE me.event_type = '카드') as cards
FROM match_events me
JOIN matches m ON me.match_id = m.id
LEFT JOIN teams ht ON m.home_team_id = ht.team_id
LEFT JOIN teams at ON m.away_team_id = at.team_id
GROUP BY me.match_id, m.match_time, m.status, ht.team, at.team, me.first_half_score, me.second_half_score;

-- 7. 함수: 경기 이벤트 삽입 (중복 방지)
CREATE OR REPLACE FUNCTION insert_match_events(
    p_match_id TEXT,
    p_events JSONB
) RETURNS INTEGER AS $$
DECLARE
    event JSONB;
    inserted_count INTEGER := 0;
BEGIN
    -- 각 이벤트를 순회하며 삽입
    FOR event IN SELECT * FROM jsonb_array_elements(p_events)
    LOOP
        BEGIN
            INSERT INTO match_events (
                match_id,
                event_type,
                event_time,
                team,
                description,
                player_name,
                assisting_player,
                card_type,
                substitution_out,
                substitution_in,
                first_half_score,
                second_half_score
            ) VALUES (
                p_match_id,
                event->>'type',
                (event->>'time')::INTEGER,
                event->>'team',
                event->>'description',
                event->>'player_name',
                event->>'assisting_player',
                event->>'card_type',
                event->>'substitution_out',
                event->>'substitution_in',
                p_events->0->>'firstHalfScore',
                p_events->0->>'secondHalfScore'
            ) ON CONFLICT (match_id, event_time, event_type, team, player_name) 
            DO UPDATE SET
                description = EXCLUDED.description,
                player_name = EXCLUDED.player_name,
                assisting_player = EXCLUDED.assisting_player,
                card_type = EXCLUDED.card_type,
                substitution_out = EXCLUDED.substitution_out,
                substitution_in = EXCLUDED.substitution_in,
                updated_at = NOW();
                
            inserted_count := inserted_count + 1;
        EXCEPTION WHEN OTHERS THEN
            -- 개별 이벤트 삽입 실패는 로그만 남기고 계속 진행
            RAISE NOTICE 'Failed to insert event: %', event;
        END;
    END LOOP;
    
    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;

-- 8. 테이블 코멘트 추가
COMMENT ON TABLE match_events IS '경기 이벤트 정보 (골, 교체, 카드 등)';
COMMENT ON COLUMN match_events.event_time IS '이벤트 발생 시간 (분 단위, 예: 29, 46, 90+4는 94로 저장)';
COMMENT ON COLUMN match_events.team IS '이벤트 발생 팀 (홈/어웨이)';
COMMENT ON COLUMN match_events.player_name IS '주요 선수명 (골득점자, 교체된 선수, 카드 받은 선수)';
COMMENT ON COLUMN match_events.assisting_player IS '어시스트 선수 (골의 경우)';
COMMENT ON COLUMN match_events.card_type IS '카드 타입 (Yellow Card, Red Card)';
COMMENT ON COLUMN match_events.substitution_out IS '교체된 선수 (나간 선수)';
COMMENT ON COLUMN match_events.substitution_in IS '교체된 선수 (들어온 선수)';
