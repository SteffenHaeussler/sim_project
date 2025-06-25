-- Simplified seed data for initial setup
-- This script creates Demo organisation and one admin user

-- Insert Demo organisation
INSERT INTO organisation (id, name, display_name, max_users, is_active, billing_email, billing_company) VALUES
(
    '660e8400-e29b-41d4-a716-446655440001',
    'demo',
    'Demo',
    50,
    true,
    'admin@demo.ai',
    'Demo Company'
);

-- Insert admin user (password: example)
-- Note: This is a bcrypt hash of "example" - change this in production!
INSERT INTO users (
    id,
    email,
    password_hash,
    first_name,
    last_name,
    organisation_id,
    is_active
) VALUES (
    '770e8400-e29b-41d4-a716-446655440001',
    'admin@demo.ai',
    '$2b$12$3AwDxWqkvWX0Lqn5vr9exufbIVyjnO5mZuN79NZERNRi2SLcg8Cy2',
    'Admin',
    'User',
    '660e8400-e29b-41d4-a716-446655440001',
    true
);

-- Display summary
SELECT 'Simplified database initialization completed successfully!' as status;
SELECT 'Created ' || COUNT(*) || ' organisations' as organisations_summary FROM organisation;
SELECT 'Created ' || COUNT(*) || ' users' as users_summary FROM users;

-- Display login credentials for testing
SELECT
    'Login Credentials for Testing:' as info,
    '' as separator;

SELECT
    email,
    'admin (password: example)' as credentials,
    first_name,
    last_name,
    (SELECT display_name FROM organisation WHERE id = users.organisation_id) as organisation
FROM users
ORDER BY email;
