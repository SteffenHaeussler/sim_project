-- Sample Queries for International Sales Database
-- These queries demonstrate common business operations

-- ===== BASIC DATA EXPLORATION =====

-- Show all tables and their row counts
SELECT 
    schemaname,
    tablename,
    n_tup_ins as total_rows
FROM pg_stat_user_tables
ORDER BY n_tup_ins DESC;

-- Database size and table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(tablename))) AS size
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(tablename)) DESC;

-- ===== CURRENCY AND EXCHANGE RATES =====

-- Show all currencies
SELECT currency_code, name, symbol 
FROM currencies 
ORDER BY currency_code;

-- Latest exchange rates (when data is populated)
SELECT 
    from_currency,
    to_currency,
    rate,
    effective_date
FROM exchange_rates er1
WHERE effective_date = (
    SELECT MAX(effective_date) 
    FROM exchange_rates er2 
    WHERE er2.from_currency = er1.from_currency 
    AND er2.to_currency = er1.to_currency
)
ORDER BY from_currency, to_currency;

-- ===== GEOGRAPHICAL DATA =====

-- Countries and their territories
SELECT 
    c.name as country,
    c.code,
    c.region,
    c.currency_code,
    COUNT(t.territory_id) as territory_count
FROM countries c
LEFT JOIN territories t ON c.country_id = t.country_id
GROUP BY c.country_id, c.name, c.code, c.region, c.currency_code
ORDER BY c.region, c.name;

-- ===== PRODUCT CATALOG =====

-- Product categories hierarchy
WITH RECURSIVE category_tree AS (
    -- Base case: root categories
    SELECT 
        category_id,
        name,
        parent_category,
        name as path,
        0 as level
    FROM product_categories 
    WHERE parent_category IS NULL
    
    UNION ALL
    
    -- Recursive case: child categories
    SELECT 
        pc.category_id,
        pc.name,
        pc.parent_category,
        ct.path || ' > ' || pc.name as path,
        ct.level + 1
    FROM product_categories pc
    JOIN category_tree ct ON pc.parent_category = ct.category_id
)
SELECT 
    REPEAT('  ', level) || name as category_hierarchy,
    category_id,
    level
FROM category_tree
ORDER BY path;

-- Products with current prices (when data is populated)
SELECT 
    p.name as product_name,
    p.sku,
    pc.name as category,
    pp.price,
    pp.currency_code,
    pp.effective_date
FROM products p
JOIN product_categories pc ON p.category_id = pc.category_id
LEFT JOIN product_prices pp ON p.product_id = pp.product_id
WHERE pp.end_date IS NULL OR pp.end_date > CURRENT_DATE
ORDER BY pc.name, p.name;

-- ===== SUPPLIER ANALYSIS =====

-- Suppliers by country (when data is populated)
SELECT 
    c.name as country,
    COUNT(s.supplier_id) as supplier_count
FROM countries c
LEFT JOIN addresses a ON c.country_id = a.country_id
LEFT JOIN suppliers s ON a.address_id = s.address_id
GROUP BY c.country_id, c.name
HAVING COUNT(s.supplier_id) > 0
ORDER BY supplier_count DESC;

-- ===== INVENTORY STATUS =====

-- Inventory levels by territory (when data is populated)
SELECT 
    t.name as territory,
    c.name as country,
    COUNT(i.product_id) as products_in_stock,
    SUM(i.quantity_on_hand) as total_quantity,
    SUM(CASE WHEN i.quantity_on_hand <= i.reorder_level THEN 1 ELSE 0 END) as products_need_reorder
FROM territories t
JOIN countries c ON t.country_id = c.country_id
LEFT JOIN inventory i ON t.territory_id = i.territory_id
WHERE i.quantity_on_hand > 0
GROUP BY t.territory_id, t.name, c.name
ORDER BY c.name, t.name;

-- ===== SALES ANALYSIS =====

-- Sales performance by employee (when data is populated)
SELECT 
    e.name as employee,
    r.name as role,
    t.name as territory,
    COUNT(o.order_id) as total_orders,
    SUM(o.grand_total_amount) as total_sales,
    o.currency_code,
    AVG(o.grand_total_amount) as avg_order_value
FROM employees e
JOIN roles r ON e.role_id = r.role_id
JOIN territories t ON e.territory_id = t.territory_id
LEFT JOIN orders o ON e.employee_id = o.employee_id
WHERE o.order_id IS NOT NULL
GROUP BY e.employee_id, e.name, r.name, t.name, o.currency_code
ORDER BY total_sales DESC;

-- Monthly sales trends (when data is populated)
SELECT 
    DATE_TRUNC('month', order_date) as month,
    currency_code,
    COUNT(*) as order_count,
    SUM(grand_total_amount) as total_sales,
    AVG(grand_total_amount) as avg_order_value
FROM orders
WHERE order_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', order_date), currency_code
ORDER BY month DESC, currency_code;

-- ===== CUSTOMER ANALYSIS =====

-- Top customers by sales volume (when data is populated)
SELECT 
    c.company_name,
    c.contact_name,
    COUNT(o.order_id) as order_count,
    SUM(o.grand_total_amount) as total_purchases,
    o.currency_code,
    MAX(o.order_date) as last_order_date
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.company_name, c.contact_name, o.currency_code
ORDER BY total_purchases DESC
LIMIT 10;

-- ===== PURCHASE ORDER ANALYSIS =====

-- Purchase orders by status (when data is populated)
SELECT 
    status,
    COUNT(*) as po_count,
    SUM(total_cost) as total_value,
    currency_code
FROM purchase_orders
GROUP BY status, currency_code
ORDER BY status, currency_code;

-- Supplier performance (when data is populated)
SELECT 
    s.company_name,
    COUNT(po.po_id) as total_pos,
    SUM(po.total_cost) as total_value,
    po.currency_code,
    AVG(EXTRACT(days FROM (po.received_date - po.order_date))) as avg_delivery_days
FROM suppliers s
JOIN purchase_orders po ON s.supplier_id = po.supplier_id
WHERE po.received_date IS NOT NULL
GROUP BY s.supplier_id, s.company_name, po.currency_code
ORDER BY total_value DESC;

-- ===== PROFITABILITY ANALYSIS =====

-- Product profitability (when comprehensive data is populated)
WITH product_costs AS (
    SELECT 
        p.product_id,
        p.name,
        SUM(CASE WHEN ct.name = 'PURCHASE' THEN pc.amount ELSE 0 END) as purchase_cost,
        SUM(CASE WHEN ct.name = 'TRANSPORT' THEN pc.amount ELSE 0 END) as transport_cost,
        SUM(CASE WHEN ct.name = 'DUTIES' THEN pc.amount ELSE 0 END) as duties_cost,
        SUM(pc.amount) as total_cost,
        pc.currency_code
    FROM products p
    JOIN product_costs pc ON p.product_id = pc.product_id
    JOIN cost_types ct ON pc.cost_type_id = ct.cost_type_id
    WHERE pc.end_date IS NULL OR pc.end_date > CURRENT_DATE
    GROUP BY p.product_id, p.name, pc.currency_code
),
product_sales AS (
    SELECT 
        p.product_id,
        AVG(od.final_unit_price) as avg_selling_price,
        SUM(od.quantity) as total_quantity_sold,
        o.currency_code
    FROM products p
    JOIN order_details od ON p.product_id = od.product_id
    JOIN orders o ON od.order_id = o.order_id
    GROUP BY p.product_id, o.currency_code
)
SELECT 
    pc.name as product,
    pc.total_cost as cost_per_unit,
    COALESCE(ps.avg_selling_price, 0) as avg_selling_price,
    (COALESCE(ps.avg_selling_price, 0) - pc.total_cost) as profit_per_unit,
    CASE 
        WHEN pc.total_cost > 0 THEN 
            ROUND(((COALESCE(ps.avg_selling_price, 0) - pc.total_cost) / pc.total_cost * 100), 2)
        ELSE 0 
    END as profit_margin_percent,
    pc.currency_code
FROM product_costs pc
LEFT JOIN product_sales ps ON pc.product_id = ps.product_id AND pc.currency_code = ps.currency_code
ORDER BY profit_margin_percent DESC;

-- ===== TAX ANALYSIS =====

-- Current tax rates by country
SELECT 
    c.name as country,
    tt.name as tax_type,
    tr.rate * 100 as tax_rate_percent,
    tr.effective_date
FROM countries c
JOIN tax_rates tr ON c.country_id = tr.country_id
JOIN tax_types tt ON tr.tax_type_id = tt.tax_type_id
WHERE tr.end_date IS NULL OR tr.end_date > CURRENT_DATE
ORDER BY c.name, tt.name;

-- ===== SYSTEM HEALTH CHECKS =====

-- Foreign key constraint verification
SELECT 
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name, kcu.column_name;

-- Index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_tup_read DESC;

-- ===== DATA VALIDATION QUERIES =====

-- Check for orphaned records (when data is populated)
-- Products without categories
SELECT p.product_id, p.name, p.sku
FROM products p
LEFT JOIN product_categories pc ON p.category_id = pc.category_id
WHERE pc.category_id IS NULL;

-- Orders without customers
SELECT o.order_id, o.order_number
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Employees without territories
SELECT e.employee_id, e.name, e.email
FROM employees e
LEFT JOIN territories t ON e.territory_id = t.territory_id
WHERE t.territory_id IS NULL;