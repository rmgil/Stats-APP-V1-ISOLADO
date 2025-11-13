-- Schema para guardar histórico de processamentos no Supabase
-- Execute este SQL no SQL Editor do Supabase Dashboard

-- Tabela principal: histórico de processamentos
CREATE TABLE IF NOT EXISTS processing_history (
    id BIGSERIAL PRIMARY KEY,
    token VARCHAR(12) UNIQUE NOT NULL,
    user_id VARCHAR(255),
    filename VARCHAR(500) NOT NULL,
    file_size_bytes BIGINT,
    
    -- Status do processamento
    status VARCHAR(50) NOT NULL DEFAULT 'processing',
    
    -- Timestamp
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    
    -- Estatísticas gerais
    total_hands INTEGER DEFAULT 0,
    total_sites INTEGER DEFAULT 0,
    sites_processed JSONB,
    
    -- Classificação
    pko_count INTEGER DEFAULT 0,
    mystery_count INTEGER DEFAULT 0,
    nonko_count INTEGER DEFAULT 0,
    
    -- Score geral
    overall_score DECIMAL(5,2),
    
    -- Monthly tracking (NEW: Nov 2025)
    months_summary JSONB,  -- Monthly manifest: {"total_months": 2, "months": [{"month": "2025-05", "hand_count": 100, ...}, ...]}
    monthly_scores JSONB,  -- Stored weighted scores per month for future analytics

    -- Dados completos em JSON (backup)
    full_result JSONB,
    
    -- Índices
    CONSTRAINT valid_status CHECK (status IN ('processing', 'completed', 'failed', 'cancelled'))
);

-- Tabela de estatísticas detalhadas por site/mesa/stat
CREATE TABLE IF NOT EXISTS poker_stats_detail (
    id BIGSERIAL PRIMARY KEY,
    processing_id BIGINT NOT NULL REFERENCES processing_history(id) ON DELETE CASCADE,
    token VARCHAR(12) NOT NULL,
    month VARCHAR(7),  -- Month in YYYY-MM format (e.g., '2025-05'). NULL = aggregate data (all months)
    
    -- Identificação
    site VARCHAR(50) NOT NULL,
    table_format VARCHAR(50) NOT NULL,
    stat_name VARCHAR(100) NOT NULL,
    
    -- Estatísticas
    opportunities INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    percentage DECIMAL(5,2),
    
    -- Timestamp
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices para melhor performance
CREATE INDEX IF NOT EXISTS idx_processing_history_user_id ON processing_history(user_id);
CREATE INDEX IF NOT EXISTS idx_processing_history_created_at ON processing_history(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_processing_history_token ON processing_history(token);
CREATE INDEX IF NOT EXISTS idx_processing_history_status ON processing_history(status);

CREATE INDEX IF NOT EXISTS idx_poker_stats_processing_id ON poker_stats_detail(processing_id);
CREATE INDEX IF NOT EXISTS idx_poker_stats_token ON poker_stats_detail(token);
CREATE INDEX IF NOT EXISTS idx_poker_stats_site ON poker_stats_detail(site);
CREATE INDEX IF NOT EXISTS idx_poker_stats_stat_name ON poker_stats_detail(stat_name);

-- Monthly tracking indexes (NEW: Nov 2025)
-- Composite index for dashboard queries by (token, month)
CREATE INDEX IF NOT EXISTS idx_poker_stats_token_month ON poker_stats_detail(token, month) WHERE month IS NOT NULL;
-- Standalone month index for month-focused analytics
CREATE INDEX IF NOT EXISTS idx_poker_stats_month ON poker_stats_detail(month) WHERE month IS NOT NULL;

-- Comentários para documentação
COMMENT ON TABLE processing_history IS 'Histórico de todos os processamentos de arquivos de poker';
COMMENT ON TABLE poker_stats_detail IS 'Estatísticas detalhadas de poker por processamento';

COMMENT ON COLUMN processing_history.token IS 'Token único gerado para cada processamento';
COMMENT ON COLUMN processing_history.user_id IS 'ID do utilizador que fez o upload (pode ser email ou ID do Flask-Login)';
COMMENT ON COLUMN processing_history.sites_processed IS 'Array JSON com lista de sites processados';
COMMENT ON COLUMN processing_history.months_summary IS 'Monthly manifest with list of months processed and metadata. NULL for single-month or legacy uploads';
COMMENT ON COLUMN processing_history.monthly_scores IS 'Weighted score breakdown per month for historical analysis';
COMMENT ON COLUMN processing_history.full_result IS 'Resultado completo do pipeline em JSON (backup)';

COMMENT ON COLUMN poker_stats_detail.month IS 'Month in YYYY-MM format (e.g., 2025-05). NULL = aggregate data (all months combined)';

-- View útil: últimos processamentos com contagem de stats
CREATE OR REPLACE VIEW recent_processing_summary AS
SELECT 
    ph.id,
    ph.token,
    ph.user_id,
    ph.filename,
    ph.status,
    ph.created_at,
    ph.completed_at,
    ph.total_hands,
    ph.total_sites,
    ph.overall_score,
    COUNT(DISTINCT psd.stat_name) as unique_stats_count,
    SUM(psd.opportunities) as total_opportunities
FROM processing_history ph
LEFT JOIN poker_stats_detail psd ON ph.id = psd.processing_id
GROUP BY ph.id, ph.token, ph.user_id, ph.filename, ph.status, 
         ph.created_at, ph.completed_at, ph.total_hands, 
         ph.total_sites, ph.overall_score
ORDER BY ph.created_at DESC;

-- Grant permissions (ajustar conforme necessidade)
-- Se usar anon key com RLS, configure as políticas adequadas
-- Se usar service_role key, estas tabelas já têm acesso total
