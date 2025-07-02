import random
from psycopg2.extras import execute_batch
from .base import BaseGenerator


class InventoryGenerator(BaseGenerator):
    """Generator for inventory levels and sales targets."""
    
    def generate_inventory_data(self):
        """Generate inventory levels across territories."""
        self.logger.info("Generating inventory data...")
        
        # Reset inventory sequence to start from 1
        try:
            self.cursor.execute("ALTER SEQUENCE inventory_inventory_id_seq RESTART WITH 1;")
            self.conn.commit()
            self.logger.debug("Reset inventory_id sequence to start from 1")
        except Exception as e:
            self.logger.warning(f"Could not reset inventory sequence: {e}")
        
        products_with_ids = [p for p in self.cache['products'].values() if 'real_id' in p]
        territories_with_ids = [t for t in self.cache['territories'].values() if 'id' in t]
        
        inventory_records = []
        
        for territory in territories_with_ids:
            # Not all territories have all products
            num_products = random.randint(
                len(products_with_ids) // 4,
                len(products_with_ids) // 2
            )
            
            territory_products = random.sample(products_with_ids, num_products)
            
            for product in territory_products:
                # Inventory levels based on product and territory
                base_stock = random.randint(10, 500)
                
                # Some variance based on "demand" (simulated)
                demand_factor = random.uniform(0.5, 2.0)
                quantity_on_hand = int(base_stock * demand_factor)
                
                # Reserved quantity (pending orders)
                quantity_reserved = random.randint(0, quantity_on_hand // 3)
                
                # On order (incoming stock)
                quantity_on_order = random.randint(0, base_stock)
                
                # Reorder levels
                max_reorder = max(6, base_stock // 3)  # Ensure minimum range
                reorder_level = random.randint(5, max_reorder)
                max_stock_level = base_stock * 2
                
                inventory_record = {
                    'product_id': product['real_id'],
                    'territory_id': territory['id'],
                    'quantity_on_hand': quantity_on_hand,
                    'quantity_on_order': quantity_on_order,
                    'quantity_reserved': quantity_reserved,
                    'reorder_level': reorder_level,
                    'max_stock_level': max_stock_level
                }
                
                inventory_records.append(inventory_record)
        
        # Insert inventory
        inventory_insert = [
            (inv['product_id'], inv['territory_id'], inv['quantity_on_hand'],
             inv['quantity_on_order'], inv['quantity_reserved'], inv['reorder_level'],
             inv['max_stock_level'])
            for inv in inventory_records
        ]
        
        execute_batch(
            self.cursor,
            """INSERT INTO inventory (product_id, territory_id, quantity_on_hand, quantity_on_order, 
                 quantity_reserved, reorder_level, max_stock_level) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s) 
                 ON CONFLICT (product_id, territory_id) DO NOTHING;""",
            inventory_insert,
            page_size=self.config.batch_size
        )
        
        # Generate sales targets for employees
        self.logger.info("Generating sales targets...")
        
        # Create territory lookup by real database ID
        territory_lookup = {}
        for territory_info in self.cache['territories'].values():
            if 'id' in territory_info:
                territory_lookup[territory_info['id']] = territory_info
        
        sales_targets = []
        sales_employees = [e for e in self.cache['employees'].values() 
                          if 'real_id' in e and 'Sales' in str(e.get('role_id', ''))]
        
        for employee in sales_employees:
            # Annual targets for current and next year
            for target_year in [self.config.end_date.year, self.config.end_date.year + 1]:
                territory_currency = territory_lookup.get(employee['territory_id'], {}).get('currency', 'USD')
                
                # Target amount based on employee level and territory
                base_target = random.uniform(100000, 1000000)
                
                sales_target = {
                    'employee_id': employee['real_id'],
                    'territory_id': None,  # Employee-specific target
                    'target_year': target_year,
                    'target_period_type': 'ANNUAL',
                    'target_period_value': 1,
                    'target_amount': round(base_target, 2),
                    'target_currency_code': territory_currency
                }
                
                sales_targets.append(sales_target)
        
        if sales_targets:
            targets_insert = [
                (st['employee_id'], st['territory_id'], st['target_year'], st['target_period_type'],
                 st['target_period_value'], st['target_amount'], st['target_currency_code'])
                for st in sales_targets
            ]
            
            execute_batch(
                self.cursor,
                """INSERT INTO sales_targets (employee_id, territory_id, target_year, target_period_type, 
                     target_period_value, target_amount, target_currency_code) 
                     VALUES (%s, %s, %s, %s, %s, %s, %s);""",
                targets_insert,
                page_size=self.config.batch_size
            )
        
        self.conn.commit()
        self.logger.info(f"Generated {len(inventory_records)} inventory records and {len(sales_targets)} sales targets")