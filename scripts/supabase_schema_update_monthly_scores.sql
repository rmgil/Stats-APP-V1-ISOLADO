-- Adiciona coluna monthly_scores à tabela processing_history para armazenar notas por mês
ALTER TABLE processing_history
    ADD COLUMN IF NOT EXISTS monthly_scores JSONB;

COMMENT ON COLUMN processing_history.monthly_scores IS 'Weighted score breakdown per month for historical analysis';
