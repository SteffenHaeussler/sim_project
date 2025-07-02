# International Sales Database Project Plan

## Overview
Create a comprehensive database schema for an international sales company with synthetic data population.

## Database Schema Design

### Core Tables
1. **Countries & Regions**
   - countries (country_id, name, code, region, currency)
   - territories (territory_id, name, country_id)
   - currencies (currency_code, name, symbol)
   - exchange_rates (from_currency, to_currency, rate, effective_date)
   - tax_types (tax_type_id, name, description) - VAT, Sales Tax, GST, etc.
   - tax_rates (country_id, territory_id, tax_type_id, rate, effective_date, end_date)

2. **Human Resources**
   - roles (role_id, name, description)
   - employees (employee_id, name, email, role_id, territory_id, manager_id, salary, salary_currency_code)

3. **Products & Inventory**
   - product_categories (category_id, name, parent_category)
   - products (product_id, name, sku, category_id, specifications)
   - product_prices(product_id, currency_code, price, effective_date, end_date)
   - suppliers (supplier_id, company_name, tax_id, contact_name, contact_email, contact_phone, address_id)
   - product_suppliers (product_id, supplier_id, supplier_product_code, unit_cost, cost_currency_code, lead_time_days)
   - inventory (product_id, territory_id, quantity_on_hand, quantity_on_order, quantity_reserved, reorder levels)
   - cost_types (cost_type_id, name, description) - Purchase, Transport, Duties, Storage, etc.
   - product_costs (product_id, cost_type_id, amount, currency_code, effective_date)
   - purchase_orders (po_id, supplier_id, order_date, total_cost, currency_code)
   - purchase_order_details (po_detail_id, po_id, product_id, quantity, unit_cost)
   - purchase_order_line_costs(line_cost_id, po_detail_id, cost_type_id, amount, currency_code)

4. **Customers & Sales**
   - customers (customer_id, company_name, tax_id, contact_name, contact_email, contact_phone, credit terms)
   - shipping_methods (shipping_method_id, method_name, carrier, estimated_days, cost_calculation_type, base_cost, currency_code)
   - orders (order_id, customer_id, employee_id, order_date, requested_delivery_date, shipped_date, payment_due_date, subtotal_amount, tax_amount, shipping_cost, grand_total_amount, address_id, shipping_method_id, tracking_number, currency_code)
   - order_details (order_detail_id, order_id, product_id, quantity, unit_price, discount_percentage, final_unit_price, line_item_tax_amount)
   - sales_targets (target_id, employee_id, territory_id, target_year, target_period_type, target_period_value, target_amount, target_currency_code)
   - addresses(address_id, address_line1, address_line2, city, postal_code, territory_id, country_id)
   - customer_addresses(customer_id, address_id, address_type)

### Key Features
- Multi-currency support with historical exchange rates
- Multi-tax system support with historical tax rates (VAT, Sales Tax, GST)
- Comprehensive cost tracking (purchase, transport, duties, storage, handling)
- Landed cost calculation for accurate profitability analysis
- Purchase order management with supplier relationships
- Hierarchical territories and product categories
- Employee management with reporting structure
- Comprehensive order tracking with shipping
- Inventory management across territories
- Sales performance tracking with margin analysis
- Historical exchange rate tracking for accurate financial reporting
- Territory-specific tax rate management with effective date ranges

## Implementation Steps
1. Create SQL schema file with all table definitions
2. Generate synthetic data using Python scripts
3. Create data insertion scripts
4. Add sample queries for common business operations
5. Optional: Create database setup scripts for different DB systems (PostgreSQL, MySQL)

## Data Generation Strategy
- ~50 countries across major regions
- ~200 territories
- ~500 employees with realistic hierarchy
- ~100 product categories
- ~2000 products with full cost breakdowns
- ~200 suppliers with varying cost structures
- ~1000 customers
- ~5000 purchase orders with realistic cost components
- ~10000 sales orders over 2-3 years
- Historical exchange rates (daily rates for 3+ years)
- Historical tax rates with realistic changes over time
- Realistic cost fluctuations (material, transport, duties)
- Realistic business relationships and patterns

## Files to Create
- `schema.sql` - Database schema
- `generate_data.py` - Synthetic data generation
- `insert_data.sql` - Data insertion scripts
- `sample_queries.sql` - Common business queries
- `setup_instructions.md` - Database setup guide
