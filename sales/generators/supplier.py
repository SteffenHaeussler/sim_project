import random
from datetime import timedelta, date, datetime
from psycopg2.extras import execute_batch
from .base import BaseGenerator


class SupplierGenerator(BaseGenerator):
    """Generator for suppliers and purchase orders."""
    
    def generate_supplier_data(self):
        """Generate suppliers and purchase orders."""
        self.logger.info("Generating supplier data...")
        
        # Reset supplier and purchase order sequences to start from 1
        try:
            self.cursor.execute("ALTER SEQUENCE suppliers_supplier_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE purchase_orders_po_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE product_suppliers_product_supplier_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE purchase_order_details_po_detail_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE purchase_order_line_costs_line_cost_id_seq RESTART WITH 1;")
            self.conn.commit()
            self.logger.debug("Reset supplier-related sequences to start from 1")
        except Exception as e:
            self.logger.warning(f"Could not reset supplier sequences: {e}")
        
        # Generate addresses first
        addresses = []
        address_id = 1
        
        # Create addresses distributed across territories
        for _ in range(self.config.suppliers * 2):  # Extra addresses for customers
            territory = random.choice(list(self.cache['territories'].values()))
            if 'id' not in territory:
                continue
                
            faker = territory['faker']
            
            # Safe secondary address generation (not all locales support this)
            secondary_address = None
            if random.random() < 0.3:
                secondary_address = self._safe_secondary_address(faker)
            
            address = {
                'id': address_id,
                'address_line1': faker.street_address(),
                'address_line2': secondary_address,
                'city': faker.city(),
                'postal_code': faker.postcode(),
                'territory_id': territory['id'],
                'country_id': territory['country_id']
            }
            
            addresses.append(address)
            address_id += 1
        
        # Insert addresses
        addresses_insert = [
            (addr['address_line1'], addr['address_line2'], addr['city'], 
             addr['postal_code'], addr['territory_id'], addr['country_id'])
            for addr in addresses
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO addresses (address_line1, address_line2, city, postal_code, territory_id, country_id) VALUES (%s, %s, %s, %s, %s, %s);",
            addresses_insert,
            page_size=self.config.batch_size
        )
        
        # Get address IDs
        self.cursor.execute("SELECT address_id FROM addresses ORDER BY address_id;")
        real_address_ids = [row[0] for row in self.cursor.fetchall()]
        
        # Generate suppliers
        suppliers = []
        supplier_id = 1
        used_company_names = set()
        
        for _ in range(self.config.suppliers):
            # Choose a territory for the supplier
            available_territories = [t for t in self.cache['territories'].values() if 'id' in t]
            if not available_territories:
                self.logger.error("No valid territories available for supplier generation")
                break
                
            territory = random.choice(available_territories)
            faker = territory['faker']
            country_code = territory['country_code']
            
            # Generate unique company name
            attempts = 0
            max_attempts = 10
            while attempts < max_attempts:
                company_name = faker.company()
                if company_name not in used_company_names:
                    used_company_names.add(company_name)
                    break
                attempts += 1
            else:
                # If we can't find a unique name, append a number
                company_name = f"{faker.company()} #{supplier_id}"
                used_company_names.add(company_name)
            
            supplier = {
                'id': supplier_id,
                'company_name': company_name,
                'tax_id': faker.tax_id(country_code),
                'contact_name': faker.name(),
                'contact_email': faker.email(),
                'contact_phone': faker.phone_number(),
                'address_id': random.choice(real_address_ids),
                'territory_id': territory['id'],
                'country_code': country_code
            }
            
            # Ensure this supplier's territory is in the cache with proper structure
            if territory['id'] not in self.cache['territories']:
                self.cache['territories'][territory['id']] = territory
            
            suppliers.append(supplier)
            self.cache['suppliers'][supplier_id] = supplier
            supplier_id += 1
        
        # Insert suppliers
        suppliers_insert = [
            (sup['company_name'], sup['tax_id'], sup['contact_name'], 
             sup['contact_email'], sup['contact_phone'], sup['address_id'])
            for sup in suppliers
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO suppliers (company_name, tax_id, contact_name, contact_email, contact_phone, address_id) VALUES (%s, %s, %s, %s, %s, %s);",
            suppliers_insert,
            page_size=self.config.batch_size
        )
        
        # Get real supplier IDs
        self.cursor.execute("SELECT supplier_id, company_name FROM suppliers;")
        supplier_name_to_id = {name: sup_id for sup_id, name in self.cursor.fetchall()}
        
        for sup in suppliers:
            real_id = supplier_name_to_id.get(sup['company_name'])
            if real_id:
                sup['real_id'] = real_id
        
        # Generate product-supplier relationships
        self.logger.info("Generating product-supplier relationships...")
        
        try:
            product_suppliers = []
            products_with_real_ids = [p for p in self.cache['products'].values() if 'real_id' in p]
            suppliers_with_real_ids = [s for s in suppliers if 'real_id' in s]
            
            if not products_with_real_ids:
                self.logger.error("No products with real IDs found for supplier relationships")
                return
            
            if not suppliers_with_real_ids:
                self.logger.error("No suppliers with real IDs found for supplier relationships")
                return
            
            self.logger.info(f"Creating relationships for {len(products_with_real_ids)} products and {len(suppliers_with_real_ids)} suppliers")
            
            # Create territory lookup by real database ID
            territory_lookup = {}
            for territory_info in self.cache['territories'].values():
                if 'id' in territory_info:
                    territory_lookup[territory_info['id']] = territory_info
            
            for product in products_with_real_ids:
                # Each product has 1-3 suppliers
                num_suppliers = random.randint(1, min(3, len(suppliers_with_real_ids)))
                product_suppliers_list = random.sample(suppliers_with_real_ids, num_suppliers)
                
                for i, supplier in enumerate(product_suppliers_list):
                    # Safely get territory currency
                    territory_id = supplier.get('territory_id')
                    if territory_id in territory_lookup:
                        currency = territory_lookup[territory_id]['currency']
                    else:
                        self.logger.warning(f"Territory {territory_id} not found for supplier {supplier.get('id', 'unknown')}, using USD")
                        currency = 'USD'
                    
                    # Base cost varies by supplier
                    base_cost = random.uniform(50, 2000)
                    cost_variation = random.uniform(0.8, 1.2)  # ±20% variation between suppliers
                    unit_cost = base_cost * cost_variation
                    
                    relationship = {
                        'product_id': product['real_id'],
                        'supplier_id': supplier['real_id'],
                        'supplier_product_code': f"SUP-{supplier['id']}-{product['id']:04d}",
                        'unit_cost': round(unit_cost, 2),
                        'cost_currency_code': currency,
                        'lead_time_days': random.randint(7, 90),
                        'is_preferred': i == 0,  # First supplier is preferred
                        'effective_date': self.config.start_date
                    }
                    
                    product_suppliers.append(relationship)
        
        except Exception as e:
            self.logger.error(f"Error in product-supplier relationship generation: {e}")
            raise
        
        # Insert product-supplier relationships
        if product_suppliers:
            ps_insert = [
                (ps['product_id'], ps['supplier_id'], ps['supplier_product_code'],
                 ps['unit_cost'], ps['cost_currency_code'], ps['lead_time_days'],
                 ps['is_preferred'], ps['effective_date'], None)
                for ps in product_suppliers
            ]
            
            try:
                self.logger.info(f"Inserting {len(ps_insert)} product-supplier relationships")
                execute_batch(
                    self.cursor,
                    "INSERT INTO product_suppliers (product_id, supplier_id, supplier_product_code, unit_cost, cost_currency_code, lead_time_days, is_preferred, effective_date, end_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                    ps_insert,
                    page_size=self.config.batch_size
                )
                self.logger.info("Product-supplier relationships inserted successfully")
            except Exception as e:
                self.logger.error(f"Failed to insert product-supplier relationships: {e}")
                # Log a sample of the data for debugging
                if ps_insert:
                    self.logger.error(f"Sample data: {ps_insert[0]}")
                raise
        else:
            self.logger.warning("No product-supplier relationships to insert")
        
        self.conn.commit()
        self.logger.info(f"Generated {len(suppliers)} suppliers and {len(product_suppliers)} product-supplier relationships")
        
        # Generate purchase orders
        self.generate_purchase_orders()

    def generate_purchase_orders(self):
        """Generate purchase orders with realistic patterns."""
        self.logger.info("Generating purchase orders...")
        
        # Get employees who can create POs (procurement managers, etc.)
        po_employees = [emp for emp in self.cache['employees'].values() if 'real_id' in emp]
        if not po_employees:
            self.logger.warning("No employees available for purchase orders")
            return
        
        # Get suppliers with real IDs
        suppliers_with_ids = [s for s in self.cache['suppliers'].values() if 'real_id' in s]
        if not suppliers_with_ids:
            self.logger.warning("No suppliers available for purchase orders")
            return
        
        purchase_orders = []
        po_details = []
        po_line_costs = []
        used_po_numbers = set()
        
        # Get cost type IDs
        self.cursor.execute("SELECT cost_type_id, name FROM cost_types;")
        cost_types = {name: id for id, name in self.cursor.fetchall()}
        
        # Create territory lookup by real database ID
        territory_lookup = {}
        for territory_info in self.cache['territories'].values():
            if 'id' in territory_info:
                territory_lookup[territory_info['id']] = territory_info
        
        for po_num in range(1, self.config.purchase_orders + 1):
            supplier = random.choice(suppliers_with_ids)
            employee = random.choice(po_employees)
            
            # Generate order date with some business patterns
            order_date = self.fake_us.date_between(
                start_date=self.config.start_date,
                end_date=self.config.end_date - timedelta(days=30)
            )
            
            # Delivery date
            lead_time = random.randint(14, 60)
            expected_delivery = order_date + timedelta(days=lead_time)
            
            # Status based on date
            if order_date < date.today() - timedelta(days=30):
                status = random.choice(['RECEIVED', 'COMPLETED', 'CANCELLED'])
                if status == 'RECEIVED':
                    received_date = expected_delivery + timedelta(days=random.randint(-5, 10))
                else:
                    received_date = None
            else:
                status = random.choice(['PENDING', 'APPROVED', 'SHIPPED'])
                received_date = None
            
            # Generate unique PO number
            attempts = 0
            max_attempts = 10
            while attempts < max_attempts:
                po_number = self.fake_us.po_number()
                if po_number not in used_po_numbers:
                    used_po_numbers.add(po_number)
                    break
                attempts += 1
            else:
                po_number = f"PO-{datetime.now().year}-{po_num:06d}"
                used_po_numbers.add(po_number)
            
            po = {
                'po_number': po_number,
                'supplier_id': supplier['real_id'],
                'employee_id': employee['real_id'],
                'order_date': order_date,
                'expected_delivery_date': expected_delivery,
                'received_date': received_date,
                'status': status,
                'currency_code': territory_lookup.get(supplier['territory_id'], {}).get('currency', 'USD'),
                'notes': f"Purchase order for {supplier['company_name']}"
            }
            
            purchase_orders.append(po)
            
            # Generate PO line items
            num_lines = random.randint(1, self.config.avg_po_lines * 2)
            po_total = 0
            
            # Get products that this supplier can provide
            self.cursor.execute("""
                SELECT ps.product_id, ps.unit_cost, ps.cost_currency_code 
                FROM product_suppliers ps 
                WHERE ps.supplier_id = %s AND ps.end_date IS NULL
            """, (supplier['real_id'],))
            
            supplier_products = self.cursor.fetchall()
            if not supplier_products:
                # Skip this PO if supplier has no products
                continue
            
            for line_num in range(num_lines):
                if not supplier_products:
                    break
                    
                product_id, unit_cost, cost_currency = random.choice(supplier_products)
                quantity = random.randint(1, 100)
                
                line_total = float(unit_cost) * quantity
                po_total += line_total
                
                po_detail = {
                    'po_number': po['po_number'],
                    'product_id': product_id,
                    'quantity': quantity,
                    'unit_cost': unit_cost,
                    'received_quantity': quantity if status == 'RECEIVED' else 0
                }
                
                po_details.append(po_detail)
                
                # Add line costs (transport, duties, etc.)
                # Transport cost (percentage of line total)
                transport_cost = line_total * random.uniform(0.05, 0.12)
                po_line_costs.append({
                    'po_detail_key': (po['po_number'], product_id),
                    'cost_type_id': cost_types['TRANSPORT'],
                    'amount': round(transport_cost, 2),
                    'currency_code': cost_currency
                })
                
                # Duties (if international)
                if random.random() < 0.6:  # 60% chance of duties
                    duties_cost = line_total * random.uniform(0.02, 0.08)
                    po_line_costs.append({
                        'po_detail_key': (po['po_number'], product_id),
                        'cost_type_id': cost_types['DUTIES'],
                        'amount': round(duties_cost, 2),
                        'currency_code': cost_currency
                    })
            
            # Update PO total
            po['total_cost'] = round(po_total, 2)
        
        # Insert purchase orders
        po_insert = [
            (po['po_number'], po['supplier_id'], po['employee_id'], po['order_date'],
             po['expected_delivery_date'], po['received_date'], po['status'],
             po['total_cost'], po['currency_code'], po['notes'])
            for po in purchase_orders
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO purchase_orders (po_number, supplier_id, employee_id, order_date, expected_delivery_date, received_date, status, total_cost, currency_code, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
            po_insert,
            page_size=self.config.batch_size
        )
        
        # Get PO IDs
        self.cursor.execute("SELECT po_id, po_number FROM purchase_orders;")
        po_number_to_id = {po_number: po_id for po_id, po_number in self.cursor.fetchall()}
        
        # Insert PO details
        po_details_insert = [
            (po_number_to_id[detail['po_number']], detail['product_id'], 
             detail['quantity'], detail['unit_cost'], detail['received_quantity'])
            for detail in po_details if detail['po_number'] in po_number_to_id
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO purchase_order_details (po_id, product_id, quantity, unit_cost, received_quantity) VALUES (%s, %s, %s, %s, %s);",
            po_details_insert,
            page_size=self.config.batch_size
        )
        
        # Get PO detail IDs for line costs
        self.cursor.execute("""
            SELECT pod.po_detail_id, po.po_number, pod.product_id 
            FROM purchase_order_details pod 
            JOIN purchase_orders po ON pod.po_id = po.po_id
        """)
        
        po_detail_lookup = {(po_number, product_id): po_detail_id 
                           for po_detail_id, po_number, product_id in self.cursor.fetchall()}
        
        # Insert line costs
        line_costs_insert = [
            (po_detail_lookup[cost['po_detail_key']], cost['cost_type_id'],
             cost['amount'], cost['currency_code'])
            for cost in po_line_costs if cost['po_detail_key'] in po_detail_lookup
        ]
        
        if line_costs_insert:
            execute_batch(
                self.cursor,
                "INSERT INTO purchase_order_line_costs (po_detail_id, cost_type_id, amount, currency_code) VALUES (%s, %s, %s, %s);",
                line_costs_insert,
                page_size=self.config.batch_size
            )
        
        self.conn.commit()
        self.logger.info(f"Generated {len(purchase_orders)} purchase orders with {len(po_details)} line items and {len(line_costs_insert)} cost entries")