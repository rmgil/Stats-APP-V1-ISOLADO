-- Add file_hash column to processing_history table for deduplication
-- Execute this in Supabase SQL Editor

-- Add file_hash column
ALTER TABLE processing_history 
ADD COLUMN IF NOT EXISTS file_hash VARCHAR(64);

-- Add index for fast duplicate detection
CREATE INDEX IF NOT EXISTS idx_processing_history_file_hash 
ON processing_history(file_hash);

-- Add storage_path column to store location in Supabase Storage
ALTER TABLE processing_history 
ADD COLUMN IF NOT EXISTS storage_path VARCHAR(500);

-- Add comment
COMMENT ON COLUMN processing_history.file_hash IS 'SHA256 hash of uploaded file for deduplication';
COMMENT ON COLUMN processing_history.storage_path IS 'Path to original file in Supabase Storage';
