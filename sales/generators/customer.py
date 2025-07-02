import random
from psycopg2.extras import execute_batch
from .base import BaseGenerator


class CustomerGenerator(BaseGenerator):
    """Generator for customers and their addresses."""
    
    def generate_customer_data(self):
        """Generate customers and their addresses."""
        self.logger.info("Generating customer data...")
        
        # Reset customer sequences to start from 1
        try:
            self.cursor.execute("ALTER SEQUENCE customers_customer_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE customer_addresses_customer_address_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE shipping_methods_shipping_method_id_seq RESTART WITH 1;")
            self.conn.commit()
            self.logger.debug("Reset customer-related sequences to start from 1")
        except Exception as e:
            self.logger.warning(f"Could not reset customer sequences: {e}")
        
        # Get available addresses (reuse some from suppliers, create new ones)
        self.cursor.execute("SELECT address_id FROM addresses;")
        existing_addresses = [row[0] for row in self.cursor.fetchall()]
        
        # Generate additional customer addresses
        new_addresses = []
        for _ in range(self.config.customers):
            territory = random.choice(list(self.cache['territories'].values()))
            if 'id' not in territory:
                continue
                
            faker = territory['faker']
            
            # Safe secondary address generation (not all locales support this)
            secondary_address = None
            if random.random() < 0.3:
                secondary_address = self._safe_secondary_address(faker)
            
            address = {
                'address_line1': faker.street_address(),
                'address_line2': secondary_address,
                'city': faker.city(),
                'postal_code': faker.postcode(),
                'territory_id': territory['id'],
                'country_id': territory['country_id']
            }
            new_addresses.append(address)
        
        # Insert new addresses
        if new_addresses:
            addresses_insert = [
                (addr['address_line1'], addr['address_line2'], addr['city'], 
                 addr['postal_code'], addr['territory_id'], addr['country_id'])
                for addr in new_addresses
            ]
            
            execute_batch(
                self.cursor,
                "INSERT INTO addresses (address_line1, address_line2, city, postal_code, territory_id, country_id) VALUES (%s, %s, %s, %s, %s, %s);",
                addresses_insert,
                page_size=self.config.batch_size
            )
        
        # Get all addresses
        self.cursor.execute("SELECT address_id FROM addresses ORDER BY address_id;")
        all_addresses = [row[0] for row in self.cursor.fetchall()]
        
        # Generate customers
        customers = []
        customer_addresses = []
        used_customer_names = set()
        used_emails = set()
        
        for customer_id in range(1, self.config.customers + 1):
            # Choose territory for customer locale
            territory = random.choice(list(self.cache['territories'].values()))
            if 'id' not in territory:
                continue
                
            faker = territory['faker']
            country_code = territory['country_code']
            
            # Customer type and size affects credit terms and limits
            customer_type = random.choice(['Small Business', 'Medium Business', 'Enterprise', 'Government'])
            
            if customer_type == 'Small Business':
                credit_limit = random.uniform(5000, 25000)
                credit_terms = random.choice([15, 30])
            elif customer_type == 'Medium Business':
                credit_limit = random.uniform(25000, 100000)
                credit_terms = random.choice([30, 45])
            elif customer_type == 'Enterprise':
                credit_limit = random.uniform(100000, 500000)
                credit_terms = random.choice([30, 45, 60])
            else:  # Government
                credit_limit = random.uniform(50000, 1000000)
                credit_terms = random.choice([45, 60, 90])
            
            # Generate unique company name
            attempts = 0
            max_attempts = 10
            while attempts < max_attempts:
                company_name = faker.company()
                if company_name not in used_customer_names:
                    used_customer_names.add(company_name)
                    break
                attempts += 1
            else:
                company_name = f"{faker.company()} #{customer_id}"
                used_customer_names.add(company_name)
            
            # Generate unique email
            attempts = 0
            while attempts < max_attempts:
                contact_email = faker.email()
                if contact_email not in used_emails:
                    used_emails.add(contact_email)
                    break
                attempts += 1
            else:
                contact_email = f"customer{customer_id}@{faker.domain_name()}"
                used_emails.add(contact_email)
            
            customer = {
                'id': customer_id,
                'company_name': company_name,
                'tax_id': faker.tax_id(country_code),
                'contact_name': faker.name(),
                'contact_email': contact_email,
                'contact_phone': faker.phone_number(),
                'credit_limit': round(credit_limit, 2),
                'credit_terms': credit_terms,
                'territory_id': territory['id'],
                'country_code': country_code
            }
            
            customers.append(customer)
            self.cache['customers'][customer_id] = customer
            
            # Assign addresses (billing and shipping)
            billing_address = random.choice(all_addresses)
            customer_addresses.append((customer_id, billing_address, 'BILLING', True))
            
            # 70% chance of separate shipping address
            if random.random() < 0.7:
                shipping_address = random.choice(all_addresses)
                customer_addresses.append((customer_id, shipping_address, 'SHIPPING', False))
        
        # Insert customers
        customers_insert = [
            (cust['company_name'], cust['tax_id'], cust['contact_name'], 
             cust['contact_email'], cust['contact_phone'], cust['credit_limit'], cust['credit_terms'])
            for cust in customers
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO customers (company_name, tax_id, contact_name, contact_email, contact_phone, credit_limit, credit_terms) VALUES (%s, %s, %s, %s, %s, %s, %s);",
            customers_insert,
            page_size=self.config.batch_size
        )
        
        # Get real customer IDs
        self.cursor.execute("SELECT customer_id, company_name FROM customers;")
        customer_name_to_id = {name: cust_id for cust_id, name in self.cursor.fetchall()}
        
        for cust in customers:
            real_id = customer_name_to_id.get(cust['company_name'])
            if real_id:
                cust['real_id'] = real_id
        
        # Insert customer addresses
        customer_addresses_insert = [
            (customer_name_to_id.get(customers[ca[0]-1]['company_name']), ca[1], ca[2], ca[3])
            for ca in customer_addresses 
            if customers[ca[0]-1]['company_name'] in customer_name_to_id
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO customer_addresses (customer_id, address_id, address_type, is_primary) VALUES (%s, %s, %s, %s);",
            customer_addresses_insert,
            page_size=self.config.batch_size
        )
        
        # Generate shipping methods
        shipping_methods = [
            ('Standard Ground', 'UPS', 5, 'FIXED', 15.00, 'USD'),
            ('Express Air', 'FedEx', 2, 'FIXED', 35.00, 'USD'),
            ('Next Day', 'FedEx', 1, 'FIXED', 75.00, 'USD'),
            ('Economy Ground', 'USPS', 7, 'FIXED', 8.50, 'USD'),
            ('International Express', 'DHL', 3, 'FIXED', 125.00, 'USD'),
            ('Freight', 'Various', 10, 'WEIGHT', 2.50, 'USD')
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO shipping_methods (method_name, carrier, estimated_days, cost_calculation_type, base_cost, currency_code) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            shipping_methods,
            page_size=self.config.batch_size
        )
        
        self.conn.commit()
        self.logger.info(f"Generated {len(customers)} customers and {len(customer_addresses_insert)} customer addresses")