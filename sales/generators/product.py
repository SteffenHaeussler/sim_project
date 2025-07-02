import random
import json
from psycopg2.extras import execute_batch
from .base import BaseGenerator


class ProductGenerator(BaseGenerator):
    """Generator for product categories, products, pricing, and costs."""
    
    def generate_product_data(self):
        """Generate product categories, products, pricing, and costs."""
        self.logger.info("Generating product catalog...")
        
        # Reset product sequences to start from 1
        try:
            self.cursor.execute("ALTER SEQUENCE product_categories_category_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE products_product_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE cost_types_cost_type_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE product_costs_product_cost_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE product_prices_price_id_seq RESTART WITH 1;")
            self.conn.commit()
            self.logger.debug("Reset product-related sequences to start from 1")
        except Exception as e:
            self.logger.warning(f"Could not reset product sequences: {e}")
        
        # Generate hierarchical product categories
        categories = []
        category_id = 1
        
        # Root categories
        root_categories = [
            'Electronics', 'Industrial Equipment', 'Office Supplies', 'Furniture',
            'Medical Devices', 'Automotive', 'Food & Beverage', 'Chemicals',
            'Textiles', 'Construction Materials'
        ]
        
        for root_name in root_categories[:min(10, self.config.product_categories)]:
            root_cat = {
                'id': category_id,
                'name': root_name,
                'parent_category': None,
                'description': f'{root_name} products and equipment'
            }
            categories.append(root_cat)
            category_id += 1
            
            # Sub-categories with uniqueness tracking
            num_subcats = random.randint(3, 8)
            used_subcat_names = set()
            
            for i in range(min(num_subcats, (self.config.product_categories - len(categories)) // 2)):
                if category_id > self.config.product_categories:
                    break
                
                # Generate unique subcategory name
                attempts = 0
                max_attempts = 20
                while attempts < max_attempts:
                    word = self.fake_us.word().title()
                    suffix = random.choice(['Systems', 'Components', 'Accessories', 'Tools', 'Equipment', 'Products', 'Solutions'])
                    subcat_name = f"{root_name} - {word} {suffix}"
                    
                    if subcat_name not in used_subcat_names:
                        used_subcat_names.add(subcat_name)
                        break
                    attempts += 1
                else:
                    # Fallback with number
                    subcat_name = f"{root_name} - Category {i + 1}"
                    used_subcat_names.add(subcat_name)
                
                sub_cat = {
                    'id': category_id,
                    'name': subcat_name,
                    'parent_category': root_cat['id'],
                    'description': f'Specialized {subcat_name.lower()}'
                }
                categories.append(sub_cat)
                category_id += 1
                
                # Sub-sub categories (limited) with uniqueness tracking
                if random.random() < 0.3 and category_id <= self.config.product_categories:
                    used_subsubcat_names = set()
                    
                    for j in range(random.randint(1, 3)):  # 1-3 sub-sub categories
                        if category_id > self.config.product_categories:
                            break
                            
                        # Generate unique sub-sub category name
                        attempts = 0
                        while attempts < max_attempts:
                            subsub_suffix = random.choice(['Professional', 'Standard', 'Premium', 'Basic', 'Advanced', 'Enterprise'])
                            subsub_name = f"{subcat_name} - {subsub_suffix}"
                            
                            if subsub_name not in used_subsubcat_names:
                                used_subsubcat_names.add(subsub_name)
                                break
                            attempts += 1
                        else:
                            # Fallback with number
                            subsub_name = f"{subcat_name} - Type {j + 1}"
                            used_subsubcat_names.add(subsub_name)
                        
                        subsub_cat = {
                            'id': category_id,
                            'name': subsub_name,
                            'parent_category': sub_cat['id'],
                            'description': f'Specialized {subsub_name.lower()}'
                        }
                        categories.append(subsub_cat)
                        category_id += 1
        
        # Insert categories in hierarchical order to avoid foreign key issues
        
        # First, insert root categories (no parent)
        root_categories = [cat for cat in categories if cat['parent_category'] is None]
        if root_categories:
            root_insert = [
                (cat['name'], None, cat['description'])
                for cat in root_categories
            ]
            
            execute_batch(
                self.cursor,
                "INSERT INTO product_categories (name, parent_category, description) VALUES (%s, %s, %s);",
                root_insert,
                page_size=self.config.batch_size
            )
        
        # Get root category real IDs
        self.cursor.execute("SELECT category_id, name FROM product_categories;")
        category_name_to_real_id = {name: cat_id for cat_id, name in self.cursor.fetchall()}
        
        # Update cache with real IDs for root categories
        for cat in root_categories:
            real_id = category_name_to_real_id.get(cat['name'])
            if real_id:
                cat['real_id'] = real_id
        
        # Insert sub-categories in levels to handle deep hierarchies
        remaining_categories = [cat for cat in categories if cat['parent_category'] is not None]
        inserted_categories = root_categories.copy()
        
        # Insert in levels until all categories are processed
        max_levels = 5  # Prevent infinite loops
        for level in range(max_levels):
            if not remaining_categories:
                break
                
            # Find categories whose parents have been inserted
            ready_to_insert = []
            for cat in remaining_categories:
                parent_cache_id = cat['parent_category']
                # Look for parent in already inserted categories
                for parent_cat in inserted_categories:
                    if parent_cat['id'] == parent_cache_id and 'real_id' in parent_cat:
                        cat['parent_category'] = parent_cat['real_id']
                        ready_to_insert.append(cat)
                        break
            
            if not ready_to_insert:
                # No more categories can be inserted (orphaned or circular references)
                self.logger.warning(f"Skipping {len(remaining_categories)} orphaned categories")
                break
            
            # Insert this level
            level_insert = [
                (cat['name'], cat['parent_category'], cat['description'])
                for cat in ready_to_insert
            ]
            
            execute_batch(
                self.cursor,
                "INSERT INTO product_categories (name, parent_category, description) VALUES (%s, %s, %s);",
                level_insert,
                page_size=self.config.batch_size
            )
            
            # Get the real IDs for this level
            self.cursor.execute("SELECT category_id, name FROM product_categories;")
            current_name_to_id = {name: cat_id for cat_id, name in self.cursor.fetchall()}
            
            # Update the real IDs in our cache
            for cat in ready_to_insert:
                real_id = current_name_to_id.get(cat['name'])
                if real_id:
                    cat['real_id'] = real_id
            
            # Move inserted categories to the inserted list
            inserted_categories.extend(ready_to_insert)
            remaining_categories = [cat for cat in remaining_categories if cat not in ready_to_insert]
        
        # Get all category IDs (final mapping)
        self.cursor.execute("SELECT category_id, name FROM product_categories;")
        category_name_to_id = {name: cat_id for cat_id, name in self.cursor.fetchall()}
        
        # Generate products
        products = []
        product_id = 1
        used_skus = set()
        
        for _ in range(self.config.products):
            category_name = random.choice(list(category_name_to_id.keys()))
            category_id = category_name_to_id[category_name]
            
            # Generate realistic product based on category
            if 'Electronics' in category_name:
                product_names = ['Monitor', 'Laptop', 'Server', 'Router', 'Switch', 'Tablet', 'Smartphone']
                specs = {
                    'brand': random.choice(['Dell', 'HP', 'Lenovo', 'Cisco', 'Apple', 'Samsung']),
                    'model': f"Model-{random.randint(1000, 9999)}",
                    'warranty_months': random.choice([12, 24, 36])
                }
            elif 'Industrial' in category_name:
                product_names = ['Pump', 'Motor', 'Valve', 'Sensor', 'Controller', 'Generator']
                specs = {
                    'power_rating': f"{random.randint(1, 500)}HP",
                    'voltage': random.choice(['120V', '240V', '480V']),
                    'certification': random.choice(['UL', 'CE', 'ISO'])
                }
            elif 'Office' in category_name:
                product_names = ['Desk', 'Chair', 'Cabinet', 'Printer', 'Paper', 'Pen Set']
                specs = {
                    'material': random.choice(['Wood', 'Metal', 'Plastic']),
                    'color': random.choice(['Black', 'White', 'Gray', 'Brown']),
                    'dimensions': f"{random.randint(20, 80)}x{random.randint(20, 80)}x{random.randint(30, 120)}cm"
                }
            else:
                product_names = ['Standard Item', 'Premium Item', 'Professional Item', 'Industrial Item']
                specs = {
                    'type': 'standard',
                    'grade': random.choice(['A', 'B', 'C']),
                    'weight': f"{random.uniform(0.1, 50.0):.1f}kg"
                }
            
            product_name = f"{random.choice(product_names)} {random.choice(['Pro', 'Standard', 'Elite', 'Basic', ''])}"
            
            # Generate unique SKU
            attempts = 0
            max_attempts = 10
            while attempts < max_attempts:
                sku = self.fake_us.product_sku()
                if sku not in used_skus:
                    used_skus.add(sku)
                    break
                attempts += 1
            else:
                sku = f"PRD-{product_id:06d}-{random.randint(100, 999)}"
                used_skus.add(sku)
            
            product = {
                'id': product_id,
                'name': product_name.strip(),
                'sku': sku,
                'category_id': category_id,
                'specifications': json.dumps(specs)
            }
            
            products.append(product)
            self.cache['products'][product_id] = product
            product_id += 1
        
        # Insert products
        products_insert = [
            (prod['name'], prod['sku'], prod['category_id'], prod['specifications'])
            for prod in products
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO products (name, sku, category_id, specifications) VALUES (%s, %s, %s, %s);",
            products_insert,
            page_size=self.config.batch_size
        )
        
        # Get real product IDs
        self.cursor.execute("SELECT product_id, sku FROM products;")
        sku_to_id = {sku: prod_id for prod_id, sku in self.cursor.fetchall()}
        
        for prod in products:
            real_id = sku_to_id.get(prod['sku'])
            if real_id:
                prod['real_id'] = real_id
        
        # Generate product prices
        self.logger.info("Generating product prices...")
        
        # Ensure cost types exist (may not be populated if init script wasn't run with --populate-ref)
        required_cost_types = [
            ('PURCHASE', 'Product Purchase Cost'),
            ('TRANSPORT', 'Transportation Cost'),
            ('DUTIES', 'Import Duties'),
            ('STORAGE', 'Storage and Warehousing'),
            ('HANDLING', 'Handling and Processing'),
            ('INSURANCE', 'Insurance Cost'),
            ('OVERHEAD', 'General Overhead')
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO cost_types (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING;",
            required_cost_types,
            page_size=self.config.batch_size
        )
        
        # Get cost type IDs
        self.cursor.execute("SELECT cost_type_id, name FROM cost_types;")
        cost_types = {name: id for id, name in self.cursor.fetchall()}
        
        prices = []
        costs = []
        
        for prod in products:
            if 'real_id' not in prod:
                continue
                
            real_id = prod['real_id']
            
            # Generate prices in multiple currencies
            base_price_usd = random.uniform(10, 5000)
            
            # Price in USD
            prices.append((real_id, 'USD', round(base_price_usd, 2), self.config.start_date, None))
            
            # Prices in other major currencies
            for currency in ['EUR', 'GBP', 'JPY', 'CAD']:
                if currency in self.cache['currencies']:
                    # Use approximate conversion (simplified)
                    if currency == 'EUR':
                        price = base_price_usd * 0.85
                    elif currency == 'GBP':
                        price = base_price_usd * 0.75
                    elif currency == 'JPY':
                        price = base_price_usd * 110
                    elif currency == 'CAD':
                        price = base_price_usd * 1.25
                    else:
                        price = base_price_usd
                    
                    prices.append((real_id, currency, round(price, 2), self.config.start_date, None))
            
            # Generate costs
            base_cost = base_price_usd * random.uniform(0.4, 0.7)  # 40-70% margin
            
            # Purchase cost
            costs.append((real_id, cost_types['PURCHASE'], round(base_cost, 2), 'USD', self.config.start_date, None))
            
            # Transport cost (5-15% of purchase cost)
            transport_cost = base_cost * random.uniform(0.05, 0.15)
            costs.append((real_id, cost_types['TRANSPORT'], round(transport_cost, 2), 'USD', self.config.start_date, None))
            
            # Duties (2-8% of purchase cost)
            duties_cost = base_cost * random.uniform(0.02, 0.08)
            costs.append((real_id, cost_types['DUTIES'], round(duties_cost, 2), 'USD', self.config.start_date, None))
        
        # Insert prices and costs
        execute_batch(
            self.cursor,
            "INSERT INTO product_prices (product_id, currency_code, price, effective_date, end_date) VALUES (%s, %s, %s, %s, %s);",
            prices,
            page_size=self.config.batch_size
        )
        
        execute_batch(
            self.cursor,
            "INSERT INTO product_costs (product_id, cost_type_id, amount, currency_code, effective_date, end_date) VALUES (%s, %s, %s, %s, %s, %s);",
            costs,
            page_size=self.config.batch_size
        )
        
        self.conn.commit()
        self.logger.info(f"Generated {len(categories)} categories, {len(products)} products, {len(prices)} prices, {len(costs)} costs")