-- Complete seed data script for industrial process monitoring application
-- This script adds initial data including default organisation and test users
-- Run with: psql -d your_database -f 02_seed_data_complete.sql

-- Insert a default organisation
INSERT INTO organisation (name, display_name, max_users, billing_email, billing_company, is_active)
VALUES ('demo', 'demo', 100, 'admin@demo.ai', 'Demo', true)
ON CONFLICT (name) DO NOTHING;

-- Get the organisation ID for reference
DO $$
DECLARE
    org_id UUID;
BEGIN
    SELECT id INTO org_id FROM organisation WHERE name = 'demo';

    -- Insert a test admin user (password: 'admin123' - change this in production!)
    -- Note: In production, use your app's registration endpoint instead
    INSERT INTO users (email, password_hash, first_name, last_name, organisation_id, is_active)
    VALUES (
        'admin@demo.ai',
        '$2b$12$3AwDxWqkvWX0Lqn5vr9exufbIVyjnO5mZuN79NZERNRi2SLcg8Cy2',  -- This is 'example' hashed
        'Admin',
        'User',
        org_id,
        true
    ) ON CONFLICT (email) DO NOTHING;

    -- Insert a test regular user (password: 'user123' - change this in production!)
    INSERT INTO users (email, password_hash, first_name, last_name, organisation_id, is_active)
    VALUES (
        'user@demo.ai',
        '$2b$12$3AwDxWqkvWX0Lqn5vr9exufbIVyjnO5mZuN79NZERNRi2SLcg8Cy2',  -- This is 'example' hashed
        'Test',
        'User',
        org_id,
        true
    ) ON CONFLICT (email) DO NOTHING;
END $$;

-- -- Create some sample API usage data (optional - for testing)
-- DO $$
-- DECLARE
--     org_id UUID;
--     admin_user_id UUID;
--     test_user_id UUID;
-- BEGIN
--     -- Get IDs for sample data
--     SELECT id INTO org_id FROM organisation WHERE name = 'demo';
--     SELECT id INTO admin_user_id FROM users WHERE email = 'admin@demo.ai';
--     SELECT id INTO test_user_id FROM users WHERE email = 'user@demo.ai';

--     -- Sample API usage logs
--     INSERT INTO api_usage_logs (
--         user_id, organisation_id, endpoint, method, status_code,
--         timestamp, duration_ms, session_id, request_id,
--         service_type, query_params
--     ) VALUES
--     (admin_user_id, org_id, '/agent', 'GET', '200', NOW() - INTERVAL '1 hour', '1250.5', 'session-1', 'req-1', 'ask-agent', '{"question": "What is the temperature?"}'),
--     (test_user_id, org_id, '/lookout/semantic', 'POST', '200', NOW() - INTERVAL '30 minutes', '2100.2', 'session-2', 'req-2', 'semantic-search', '{"semantic_query": "pressure data"}'),
--     (admin_user_id, org_id, '/lookup/assets', 'GET', '200', NOW() - INTERVAL '15 minutes', '450.1', 'session-3', 'req-3', 'lookup-service', '{}');

-- END $$;

-- -- Success message
-- SELECT 'Seed data inserted successfully!' as status,
--        (SELECT COUNT(*) FROM organisation) as organisations_count,
--        (SELECT COUNT(*) FROM users) as users_count,
--        (SELECT COUNT(*) FROM api_usage_logs) as sample_logs_count;
