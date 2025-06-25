-- Simplified database initialization script for industrial process monitoring application
-- This script creates the simplified authentication and usage tracking tables

-- Create database (run this manually if needed)
-- CREATE DATABASE organisation;

-- Connect to the database
-- \c organisation;

-- Enable UUID extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

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
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    organisation_id UUID NOT NULL REFERENCES organisation(id),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- API usage logs table (for billing and invoicing)
CREATE TABLE api_usage_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id),
    organisation_id UUID NOT NULL REFERENCES organisation(id),
    -- API call details for billing
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL,
    status_code VARCHAR(10),
    -- Billing details
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    duration_ms VARCHAR(50),
    -- Optional: detailed request info for debugging
    query_params VARCHAR(1000)
);

-- Indexes for performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_organisation_id ON users(organisation_id);
CREATE INDEX idx_api_usage_logs_user_id ON api_usage_logs(user_id);
CREATE INDEX idx_api_usage_logs_organisation_id ON api_usage_logs(organisation_id);
CREATE INDEX idx_api_usage_logs_timestamp ON api_usage_logs(timestamp);
CREATE INDEX idx_api_usage_logs_endpoint ON api_usage_logs(endpoint);

-- Function to generate usage reports for billing
CREATE OR REPLACE FUNCTION get_monthly_usage_report(
    org_id UUID,
    report_month DATE DEFAULT DATE_TRUNC('month', CURRENT_DATE)
)
RETURNS TABLE(
    endpoint VARCHAR,
    call_count BIGINT,
    avg_duration_ms NUMERIC,
    total_calls_for_month BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        api_usage_logs.endpoint,
        COUNT(*) as call_count,
        ROUND(AVG(CAST(api_usage_logs.duration_ms AS NUMERIC)), 2) as avg_duration_ms,
        (SELECT COUNT(*) FROM api_usage_logs al2
         WHERE al2.organisation_id = org_id
         AND al2.timestamp >= report_month
         AND al2.timestamp < report_month + INTERVAL '1 month') as total_calls_for_month
    FROM api_usage_logs
    WHERE organisation_id = org_id
    AND timestamp >= report_month
    AND timestamp < report_month + INTERVAL '1 month'
    GROUP BY api_usage_logs.endpoint
    ORDER BY call_count DESC;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON TABLE organisation IS 'Organisations for user management and billing';
COMMENT ON TABLE users IS 'Application users with simplified authentication';
COMMENT ON TABLE api_usage_logs IS 'API call tracking for billing and invoicing';
COMMENT ON FUNCTION get_monthly_usage_report IS 'Generate monthly usage reports for billing';
