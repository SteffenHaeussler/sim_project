-- Complete database initialization script for industrial process monitoring application
-- This script creates all tables including authentication, usage tracking, response metadata, and ratings
-- Run with: psql -d your_database -f 01_database_init_complete.sql

-- Enable UUID extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Drop tables if they exist (for clean reinstall)
DROP TABLE IF EXISTS user_response_ratings CASCADE;
DROP TABLE IF EXISTS api_response_metadata CASCADE;
DROP TABLE IF EXISTS password_reset CASCADE;
DROP TABLE IF EXISTS api_usage_logs CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS organisation CASCADE;

-- Organisations table (simplified for user limits and billing)
CREATE TABLE organisation (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    display_name VARCHAR(255) NOT NULL,
    max_users INTEGER DEFAULT 50,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- Billing/invoicing info
    billing_email VARCHAR(255),
    billing_company VARCHAR(255)
);

-- Users table (simplified)
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    organisation_id UUID NOT NULL REFERENCES organisation(id),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Password reset table
CREATE TABLE password_reset (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    is_used BOOLEAN NOT NULL DEFAULT FALSE,
    token VARCHAR(255) NOT NULL UNIQUE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- API usage logs table (for billing and enhanced tracking)
CREATE TABLE api_usage_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),  -- Nullable for unauthenticated requests
    organisation_id UUID REFERENCES organisation(id),  -- Nullable for unauthenticated requests
    -- API call details for billing
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL,
    status_code VARCHAR(10),
    -- Enhanced tracking details
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    duration_ms VARCHAR(50),
    -- Session and user context
    session_id VARCHAR(255),
    request_id VARCHAR(255),
    event_id VARCHAR(255),
    user_agent VARCHAR(500),
    ip_address VARCHAR(45),  -- IPv4/IPv6 address
    -- Request details
    query_params VARCHAR(1000),
    request_size VARCHAR(20),  -- Request body size in bytes
    response_size VARCHAR(20),  -- Response body size in bytes
    -- Service usage tracking
    service_type VARCHAR(50),  -- ask-agent, lookup-service, auth, etc.
    template_used VARCHAR(100),  -- Which template was clicked (if any)
    -- Error tracking
    error_message VARCHAR(1000)  -- Store error details if request failed
);

-- API response metadata table (for response analysis)
CREATE TABLE api_response_metadata (
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

-- User response ratings table (for thumbs up/down tracking)
CREATE TABLE user_response_ratings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    usage_log_id UUID REFERENCES api_usage_logs(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Rating information
    rating_type VARCHAR(20) NOT NULL,  -- 'thumbs_up', 'thumbs_down'
    rating_value INTEGER NOT NULL,     -- 1 for up, -1 for down
    -- Optional feedback
    feedback_text VARCHAR(1000),
    -- Context information
    session_id VARCHAR(255),
    event_id VARCHAR(255),
    message_context VARCHAR(500),      -- The question/query that led to this response
    -- Timestamps
    rated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance (only essential ones)
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_organisation_id ON users(organisation_id);
CREATE INDEX idx_password_reset_token ON password_reset(token);
CREATE INDEX idx_password_reset_user_id ON password_reset(user_id);
CREATE INDEX idx_password_reset_expires_at ON password_reset(expires_at);
CREATE INDEX idx_api_usage_logs_user_id ON api_usage_logs(user_id);
CREATE INDEX idx_api_usage_logs_organisation_id ON api_usage_logs(organisation_id);
CREATE INDEX idx_api_usage_logs_timestamp ON api_usage_logs(timestamp);
CREATE INDEX idx_user_response_ratings_user_id ON user_response_ratings(user_id);
CREATE INDEX idx_user_response_ratings_event_id ON user_response_ratings(event_id);

-- Comments for documentation
COMMENT ON TABLE organisation IS 'Organisations for user management and billing';
COMMENT ON TABLE users IS 'Application users with simplified authentication';
COMMENT ON TABLE password_reset IS 'Password reset tokens for user authentication';
COMMENT ON TABLE api_usage_logs IS 'Enhanced API call tracking for billing, analytics, and user behavior analysis';
COMMENT ON TABLE api_response_metadata IS 'Response metadata for quality monitoring and debugging';
COMMENT ON TABLE user_response_ratings IS 'User ratings and feedback for response quality tracking';
COMMENT ON COLUMN user_response_ratings.response_metadata_id IS 'Links to response metadata - nullable for WebSocket responses';

-- Success message
SELECT 'Complete database initialization completed successfully!' as status;
