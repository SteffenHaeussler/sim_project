-- Seed data for initial setup
-- This script populates the database with default roles and a demo organization

-- Insert default roles
INSERT INTO roles (id, name, display_name, description, permissions) VALUES
(
    '550e8400-e29b-41d4-a716-446655440001',
    'admin',
    'System Administrator',
    'Full system access with user management and configuration capabilities',
    '{
        "system": ["read", "write", "delete", "configure"],
        "users": ["read", "write", "delete", "manage"],
        "organizations": ["read", "write", "delete"],
        "audit": ["read"],
        "api": ["full_access"],
        "monitoring": ["read", "write", "configure"],
        "assets": ["read", "write", "delete"]
    }'::jsonb
),
(
    '550e8400-e29b-41d4-a716-446655440002',
    'manager',
    'Manager',
    'Operational management with user oversight and full monitoring access',
    '{
        "users": ["read", "write"],
        "monitoring": ["read", "write", "configure"],
        "assets": ["read", "write"],
        "reports": ["read", "write", "export"],
        "api": ["read", "write"]
    }'::jsonb
),
(
    '550e8400-e29b-41d4-a716-446655440003',
    'engineer',
    'Process Engineer',
    'Full monitoring and analysis capabilities for industrial processes',
    '{
        "monitoring": ["read", "write"],
        "assets": ["read", "write"],
        "analysis": ["read", "write"],
        "reports": ["read", "write", "export"],
        "api": ["read", "write"]
    }'::jsonb
),
(
    '550e8400-e29b-41d4-a716-446655440004',
    'operator',
    'Plant Operator',
    'Basic monitoring and operational capabilities',
    '{
        "monitoring": ["read"],
        "assets": ["read"],
        "alerts": ["read", "acknowledge"],
        "reports": ["read"],
        "api": ["read"]
    }'::jsonb
),
(
    '550e8400-e29b-41d4-a716-446655440005',
    'viewer',
    'Read-Only Viewer',
    'Read-only access to monitoring data and reports',
    '{
        "monitoring": ["read"],
        "assets": ["read"],
        "reports": ["read"]
    }'::jsonb
);

-- Insert demo organization
INSERT INTO organizations (id, name, display_name, subscription_tier, max_users, is_active) VALUES
(
    '660e8400-e29b-41d4-a716-446655440001',
    'demo',
    'Demo Industrial Solutions',
    'enterprise',
    100,
    true
),
(
    '660e8400-e29b-41d4-a716-446655440002',
    'acme_corp',
    'ACME Corporation',
    'professional',
    50,
    true
);

-- Insert demo admin user (password: admin123!)
-- Note: This is a bcrypt hash of "admin123!" - change this in production!
INSERT INTO users (
    id,
    email,
    password_hash,
    first_name,
    last_name,
    role_id,
    organization_id,
    is_active,
    is_verified
) VALUES (
    '770e8400-e29b-41d4-a716-446655440001',
    'admin@demo.ai',
    '$2b$12$YDMApXR2t2hw255XFSn09O7NQ/K831.6Tzg8nBJko3sKCCW5ChpGq',
    'System',
    'Administrator',
    '550e8400-e29b-41d4-a716-446655440001',
    '660e8400-e29b-41d4-a716-446655440001',
    true,
    true
);

-- Insert demo manager user (password: manager123!)
INSERT INTO users (
    id,
    email,
    password_hash,
    first_name,
    last_name,
    role_id,
    organization_id,
    is_active,
    is_verified
) VALUES (
    '770e8400-e29b-41d4-a716-446655440002',
    'manager@demo.ai',
    '$2b$12$YDMApXR2t2hw255XFSn09O7NQ/K831.6Tzg8nBJko3sKCCW5ChpGq',
    'John',
    'Manager',
    '550e8400-e29b-41d4-a716-446655440002',
    '660e8400-e29b-41d4-a716-446655440001',
    true,
    true
);

-- Insert demo engineer user (password: engineer123!)
INSERT INTO users (
    id,
    email,
    password_hash,
    first_name,
    last_name,
    role_id,
    organization_id,
    is_active,
    is_verified
) VALUES (
    '770e8400-e29b-41d4-a716-446655440003',
    'engineer@demo.ai',
    '$2b$12$YDMApXR2t2hw255XFSn09O7NQ/K831.6Tzg8nBJko3sKCCW5ChpGq',
    'Jane',
    'Smith',
    '550e8400-e29b-41d4-a716-446655440003',
    '660e8400-e29b-41d4-a716-446655440001',
    true,
    true
);

-- Insert demo operator user (password: operator123!)
INSERT INTO users (
    id,
    email,
    password_hash,
    first_name,
    last_name,
    role_id,
    organization_id,
    is_active,
    is_verified
) VALUES (
    '770e8400-e29b-41d4-a716-446655440004',
    'operator@demo.ai',
    '$2b$12$YDMApXR2t2hw255XFSn09O7NQ/K831.6Tzg8nBJko3sKCCW5ChpGq',
    'Mike',
    'Operator',
    '550e8400-e29b-41d4-a716-446655440004',
    '660e8400-e29b-41d4-a716-446655440001',
    true,
    true
);

-- Log the setup in audit logs
INSERT INTO audit_logs (
    user_id,
    organization_id,
    action,
    resource,
    details,
    success
) VALUES (
    '770e8400-e29b-41d4-a716-446655440001',
    '660e8400-e29b-41d4-a716-446655440001',
    'database_initialization',
    'system',
    '{"event": "initial_setup", "roles_created": 5, "users_created": 4, "organizations_created": 2}'::jsonb,
    true
);

-- Display summary
SELECT 'Database initialization completed successfully!' as status;
SELECT 'Created ' || COUNT(*) || ' roles' as roles_summary FROM roles;
SELECT 'Created ' || COUNT(*) || ' organizations' as organizations_summary FROM organizations;
SELECT 'Created ' || COUNT(*) || ' users' as users_summary FROM users;

-- Display login credentials for testing
SELECT
    'Login Credentials for Testing:' as info,
    '' as separator;

SELECT
    email,
    CASE role_id
        WHEN '550e8400-e29b-41d4-a716-446655440001' THEN 'admin (password: admin123!)'
        WHEN '550e8400-e29b-41d4-a716-446655440002' THEN 'manager (password: manager123!)'
        WHEN '550e8400-e29b-41d4-a716-446655440003' THEN 'engineer (password: engineer123!)'
        WHEN '550e8400-e29b-41d4-a716-446655440004' THEN 'operator (password: operator123!)'
    END as role_and_password,
    first_name,
    last_name
FROM users
ORDER BY role_id;
