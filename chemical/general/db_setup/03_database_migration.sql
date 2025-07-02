-- Complete database migration script for existing databases
-- This script safely adds all new tables and columns for enhanced tracking and ratings
-- Run with: psql -d your_database -f 03_database_migration.sql

-- Ensure UUID extension exists
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Add password reset table if it doesn't exist
CREATE TABLE IF NOT EXISTS password_reset (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(255) NOT NULL UNIQUE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Enhance api_usage_logs table if columns don't exist
DO $$
BEGIN
    -- Make user_id and organisation_id nullable for unauthenticated requests
    BEGIN
        ALTER TABLE api_usage_logs ALTER COLUMN user_id DROP NOT NULL;
    EXCEPTION WHEN OTHERS THEN
        -- Column might already be nullable, ignore error
    END;

    BEGIN
        ALTER TABLE api_usage_logs ALTER COLUMN organisation_id DROP NOT NULL;
    EXCEPTION WHEN OTHERS THEN
        -- Column might already be nullable, ignore error
    END;

    -- Add enhanced tracking columns
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'api_usage_logs' AND column_name = 'session_id') THEN
        ALTER TABLE api_usage_logs ADD COLUMN session_id VARCHAR(255);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'api_usage_logs' AND column_name = 'request_id') THEN
        ALTER TABLE api_usage_logs ADD COLUMN request_id VARCHAR(255);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'api_usage_logs' AND column_name = 'user_agent') THEN
        ALTER TABLE api_usage_logs ADD COLUMN user_agent VARCHAR(500);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'api_usage_logs' AND column_name = 'ip_address') THEN
        ALTER TABLE api_usage_logs ADD COLUMN ip_address VARCHAR(45);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'api_usage_logs' AND column_name = 'request_size') THEN
        ALTER TABLE api_usage_logs ADD COLUMN request_size VARCHAR(20);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'api_usage_logs' AND column_name = 'response_size') THEN
        ALTER TABLE api_usage_logs ADD COLUMN response_size VARCHAR(20);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'api_usage_logs' AND column_name = 'service_type') THEN
        ALTER TABLE api_usage_logs ADD COLUMN service_type VARCHAR(50);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'api_usage_logs' AND column_name = 'template_used') THEN
        ALTER TABLE api_usage_logs ADD COLUMN template_used VARCHAR(100);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'api_usage_logs' AND column_name = 'error_message') THEN
        ALTER TABLE api_usage_logs ADD COLUMN error_message VARCHAR(1000);
    END IF;
END $$;

-- Create api_response_metadata table if it doesn't exist
CREATE TABLE IF NOT EXISTS api_response_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    usage_log_id UUID NOT NULL REFERENCES api_usage_logs(id) ON DELETE CASCADE,
    -- Response metadata
    response_status_code VARCHAR(10) NOT NULL,
    response_size_bytes INTEGER,
    response_time_ms VARCHAR(50),
    -- Content metadata (without storing full content)
    content_type VARCHAR(100),
    content_preview VARCHAR(500),  -- First 500 chars for debugging
    has_images BOOLEAN DEFAULT false,
    image_count INTEGER DEFAULT 0,
    -- Service-specific metadata
    service_response_id VARCHAR(255),
    processing_steps VARCHAR(1000),  -- For semantic search: embedding→search→rank
    -- Error information
    error_type VARCHAR(100),
    error_details VARCHAR(1000),
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create user_response_ratings table if it doesn't exist
CREATE TABLE IF NOT EXISTS user_response_ratings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    response_metadata_id UUID REFERENCES api_response_metadata(id) ON DELETE CASCADE,  -- Nullable for WebSocket responses
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Rating information
    rating_type VARCHAR(20) NOT NULL,  -- 'thumbs_up', 'thumbs_down'
    rating_value INTEGER NOT NULL,     -- 1 for up, -1 for down
    -- Optional feedback
    feedback_text VARCHAR(1000),
    -- Context information
    session_id VARCHAR(255),
    message_context VARCHAR(500),      -- The question/query that led to this response
    -- Timestamps
    rated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add usage_log_id column to existing user_response_ratings table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'user_response_ratings' AND column_name = 'usage_log_id') THEN
        ALTER TABLE user_response_ratings ADD COLUMN usage_log_id UUID;
        ALTER TABLE user_response_ratings ADD CONSTRAINT fk_response_ratings_usage_log FOREIGN KEY (usage_log_id) REFERENCES api_usage_logs(id) ON DELETE SET NULL;
    END IF;
END $$;

-- Add event_id column to existing user_response_ratings table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'user_response_ratings' AND column_name = 'event_id') THEN
        ALTER TABLE user_response_ratings ADD COLUMN event_id VARCHAR(255);
    END IF;
END $$;

-- Add indexes safely (only essential ones)
CREATE INDEX IF NOT EXISTS idx_password_reset_token ON password_reset(token);
CREATE INDEX IF NOT EXISTS idx_password_reset_user_id ON password_reset(user_id);
CREATE INDEX IF NOT EXISTS idx_password_reset_expires_at ON password_reset(expires_at);
CREATE INDEX IF NOT EXISTS idx_api_usage_logs_user_id ON api_usage_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_api_usage_logs_organisation_id ON api_usage_logs(organisation_id);
CREATE INDEX IF NOT EXISTS idx_api_usage_logs_timestamp ON api_usage_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_user_response_ratings_user_id ON user_response_ratings(user_id);
CREATE INDEX IF NOT EXISTS idx_user_response_ratings_event_id ON user_response_ratings(event_id);

-- Add table comments
COMMENT ON TABLE password_reset IS 'Password reset tokens for user authentication';
COMMENT ON TABLE api_response_metadata IS 'Response metadata for quality monitoring and debugging';
COMMENT ON TABLE user_response_ratings IS 'User ratings and feedback for response quality tracking';
COMMENT ON COLUMN user_response_ratings.response_metadata_id IS 'Links to response metadata - nullable for WebSocket responses';

-- Update existing table comments
COMMENT ON TABLE api_usage_logs IS 'Enhanced API call tracking for billing, analytics, and user behavior analysis';

-- Success message
SELECT 'Database migration completed successfully!' as status,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'password_reset') THEN 'EXISTS' ELSE 'MISSING' END as password_reset_table,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'api_response_metadata') THEN 'EXISTS' ELSE 'MISSING' END as response_metadata_table,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'user_response_ratings') THEN 'EXISTS' ELSE 'MISSING' END as ratings_table;
