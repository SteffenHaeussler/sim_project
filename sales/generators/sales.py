import random
import numpy as np
from datetime import timedelta, date, datetime
from psycopg2.extras import execute_batch
from .base import BaseGenerator


class SalesGenerator(BaseGenerator):
    """Generator for sales orders and order details."""
    
    def generate_sales_data(self):
        """Generate sales orders with seasonal patterns and realistic business logic."""
        self.logger.info("Generating sales orders...")
        
        # Reset sales order sequences to start from 1
        try:
            self.cursor.execute("ALTER SEQUENCE orders_order_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE order_details_order_detail_id_seq RESTART WITH 1;")
            self.conn.commit()
            self.logger.debug("Reset sales order sequences to start from 1")
        except Exception as e:
            self.logger.warning(f"Could not reset order sequences: {e}")
        
        try:
            # Get necessary data
            customers_with_ids = [c for c in self.cache['customers'].values() if 'real_id' in c]
            employees_with_ids = [e for e in self.cache['employees'].values() if 'real_id' in e]
            products_with_ids = [p for p in self.cache['products'].values() if 'real_id' in p]
            
            self.logger.info(f"Available for sales generation: {len(customers_with_ids)} customers, {len(employees_with_ids)} employees, {len(products_with_ids)} products")
            
            if not customers_with_ids:
                self.logger.error("No customers with real IDs found for sales generation")
                return
            if not employees_with_ids:
                self.logger.error("No employees with real IDs found for sales generation")
                return
            if not products_with_ids:
                self.logger.error("No products with real IDs found for sales generation")
                return
            
            # Create territory lookup by real database ID
            territory_lookup = {}
            for territory_info in self.cache['territories'].values():
                if 'id' in territory_info:
                    territory_lookup[territory_info['id']] = territory_info
            
            self.logger.info(f"Built territory lookup for {len(territory_lookup)} territories")
        
        except Exception as e:
            self.logger.error(f"Error during sales data preparation: {e}")
            raise
        
        try:
            # Get shipping methods
            self.cursor.execute("SELECT shipping_method_id FROM shipping_methods;")
            shipping_methods = [row[0] for row in self.cursor.fetchall()]
            self.logger.info(f"Found {len(shipping_methods)} shipping methods")
            
            # Get addresses for orders
            self.cursor.execute("SELECT address_id FROM addresses;")
            addresses = [row[0] for row in self.cursor.fetchall()]
            self.logger.info(f"Found {len(addresses)} addresses")
            
            if not addresses:
                self.logger.error("No addresses found for order generation")
                return
        
        except Exception as e:
            self.logger.error(f"Error fetching shipping methods or addresses: {e}")
            raise
        
        orders = []
        order_details = []
        used_order_numbers = set()
        
        # Generate orders with seasonal patterns
        try:
            self.logger.info(f"Starting generation of {self.config.sales_orders} sales orders")
            
            for order_num in range(1, self.config.sales_orders + 1):
                if order_num % 1000 == 0:
                    self.logger.info(f"Generated {order_num} orders so far...")
                
                try:
                    # Choose customer and sales rep
                    customer = random.choice(customers_with_ids)
                    employee = random.choice(employees_with_ids)
                    
                    # Generate order date with seasonality
                    # Q4 has higher volume, summer months are slower
                    order_date = self.fake_us.date_between(
                        start_date=self.config.start_date,
                        end_date=self.config.end_date
                    )
                    
                    # Apply seasonal factor
                    month = order_date.month
                    if month in [11, 12]:  # Q4 boost
                        seasonal_multiplier = 1.4
                    elif month in [1, 2]:  # Post-holiday slowdown
                        seasonal_multiplier = 0.7
                    elif month in [6, 7, 8]:  # Summer slowdown
                        seasonal_multiplier = 0.8
                    else:
                        seasonal_multiplier = 1.0
                    
                    # Skip some orders based on seasonality (create realistic volume patterns)
                    if random.random() > seasonal_multiplier:
                        continue
                    
                    # Order details
                    billing_address = random.choice(addresses)
                    shipping_address = random.choice(addresses)
                    shipping_method = random.choice(shipping_methods) if shipping_methods else None
                    
                    # Currency based on customer territory - safely get with fallback
                    territory_id = customer.get('territory_id')
                    if territory_id in territory_lookup:
                        currency = territory_lookup[territory_id]['currency']
                    else:
                        self.logger.warning(f"Territory {territory_id} not found for customer {customer.get('real_id', 'unknown')}, using USD")
                        currency = 'USD'
            
                    # Generate order lines
                    num_lines = np.random.poisson(self.config.avg_order_lines)
                    num_lines = max(1, min(num_lines, 10))  # 1-10 lines per order
                    
                    order_products = random.sample(products_with_ids, min(num_lines, len(products_with_ids)))
            
                    subtotal = 0
                    order_line_details = []
                    
                    for product in order_products:
                        # Get product price in order currency
                        self.cursor.execute("""
                            SELECT price FROM product_prices 
                            WHERE product_id = %s AND currency_code = %s 
                            AND (end_date IS NULL OR end_date > %s)
                            ORDER BY effective_date DESC LIMIT 1
                        """, (product['real_id'], currency, order_date))
                        
                        price_result = self.cursor.fetchone()
                        if not price_result:
                            # Fallback to USD price with approximate conversion
                            self.cursor.execute("""
                                SELECT price FROM product_prices 
                                WHERE product_id = %s AND currency_code = 'USD'
                                AND (end_date IS NULL OR end_date > %s)
                                ORDER BY effective_date DESC LIMIT 1
                            """, (product['real_id'], order_date))
                            
                            usd_price_result = self.cursor.fetchone()
                            if usd_price_result:
                                unit_price = float(usd_price_result[0])
                                # Simple conversion (in real system, use exchange rates)
                                if currency == 'EUR':
                                    unit_price *= 0.85
                                elif currency == 'GBP':
                                    unit_price *= 0.75
                                elif currency == 'JPY':
                                    unit_price *= 110
                                elif currency == 'CAD':
                                    unit_price *= 1.25
                            else:
                                unit_price = random.uniform(50, 1000)  # Fallback
                        else:
                            unit_price = float(price_result[0])
                        
                        # Quantity based on customer type and product
                        if customer.get('credit_limit', 0) > 100000:  # Large customer
                            quantity = random.randint(5, 50)
                        elif customer.get('credit_limit', 0) > 25000:  # Medium customer
                            quantity = random.randint(2, 20)
                        else:  # Small customer
                            quantity = random.randint(1, 10)
                        
                        # Discount (volume discounts for larger orders)
                        if quantity > 20:
                            discount_pct = random.uniform(5, 15)
                        elif quantity > 10:
                            discount_pct = random.uniform(2, 8)
                        else:
                            discount_pct = random.uniform(0, 5)
                        
                        final_unit_price = unit_price * (1 - discount_pct / 100)
                        line_total = final_unit_price * quantity
                        
                        # Calculate line-level tax (simplified)
                        tax_rate = random.uniform(0.05, 0.15)  # 5-15% tax
                        line_tax = line_total * tax_rate
                        
                        subtotal += line_total
                        
                        order_line_details.append({
                            'product_id': product['real_id'],
                            'quantity': quantity,
                            'unit_price': round(unit_price, 2),
                            'discount_percentage': round(discount_pct, 2),
                            'final_unit_price': round(final_unit_price, 2),
                            'line_item_tax_amount': round(line_tax, 2)
                        })
                    
                    # Calculate order totals
                    total_tax = sum(line['line_item_tax_amount'] for line in order_line_details)
                    shipping_cost = random.uniform(10, 100) if shipping_method else 0
                    grand_total = subtotal + total_tax + shipping_cost
                    
                    # Order status and dates
                    if order_date < date.today() - timedelta(days=7):
                        status = random.choice(['COMPLETED', 'SHIPPED', 'CANCELLED'])
                        if status in ['COMPLETED', 'SHIPPED']:
                            shipped_date = order_date + timedelta(days=random.randint(1, 7))
                            payment_due_date = order_date + timedelta(days=customer['credit_terms'])
                        else:
                            shipped_date = None
                            payment_due_date = None
                    else:
                        status = random.choice(['PENDING', 'PROCESSING', 'APPROVED'])
                        shipped_date = None
                        payment_due_date = order_date + timedelta(days=customer['credit_terms'])
                    
                    requested_delivery = order_date + timedelta(days=random.randint(7, 21))
                    
                    # Generate unique order number
                    attempts = 0
                    max_attempts = 10
                    while attempts < max_attempts:
                        order_number = self.fake_us.order_number()
                        if order_number not in used_order_numbers:
                            used_order_numbers.add(order_number)
                            break
                        attempts += 1
                    else:
                        order_number = f"SO-{datetime.now().year}-{order_num:06d}"
                        used_order_numbers.add(order_number)
                    
                    order = {
                        'order_number': order_number,
                        'customer_id': customer['real_id'],
                        'employee_id': employee['real_id'],
                        'order_date': order_date,
                        'requested_delivery_date': requested_delivery,
                        'shipped_date': shipped_date,
                        'payment_due_date': payment_due_date,
                        'status': status,
                        'subtotal_amount': round(subtotal, 2),
                        'tax_amount': round(total_tax, 2),
                        'shipping_cost': round(shipping_cost, 2),
                        'grand_total_amount': round(grand_total, 2),
                        'billing_address_id': billing_address,
                        'shipping_address_id': shipping_address,
                        'shipping_method_id': shipping_method,
                        'tracking_number': f"TRK{random.randint(100000000, 999999999)}" if shipped_date else None,
                        'currency_code': currency,
                        'notes': f"Order for {customer['company_name']}",
                        'line_details': order_line_details
                    }
                    
                    orders.append(order)
                
                except Exception as e:
                    self.logger.warning(f"Error generating order {order_num}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error in sales order generation loop: {e}")
            raise
        
        # Insert orders
        try:
            self.logger.info(f"Inserting {len(orders)} orders into database")
            
            orders_insert = [
                (order['order_number'], order['customer_id'], order['employee_id'], order['order_date'],
                 order['requested_delivery_date'], order['shipped_date'], order['payment_due_date'],
                 order['status'], order['subtotal_amount'], order['tax_amount'], order['shipping_cost'],
                 order['grand_total_amount'], order['billing_address_id'], order['shipping_address_id'],
                 order['shipping_method_id'], order['tracking_number'], order['currency_code'], order['notes'])
                for order in orders
            ]
            
            execute_batch(
                self.cursor,
                """INSERT INTO orders (order_number, customer_id, employee_id, order_date, requested_delivery_date, 
                     shipped_date, payment_due_date, status, subtotal_amount, tax_amount, shipping_cost, 
                     grand_total_amount, billing_address_id, shipping_address_id, shipping_method_id, 
                     tracking_number, currency_code, notes) 
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                orders_insert,
                page_size=self.config.batch_size
            )
            
            self.logger.info("Orders inserted successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to insert orders: {e}")
            if orders_insert:
                self.logger.error(f"Sample order data: {orders_insert[0]}")
            raise
        
        try:
            # Get order IDs
            self.cursor.execute("SELECT order_id, order_number FROM orders;")
            order_number_to_id = {order_number: order_id for order_id, order_number in self.cursor.fetchall()}
            
            self.logger.info(f"Retrieved {len(order_number_to_id)} order IDs")
            
            # Insert order details
            for order in orders:
                order_id = order_number_to_id.get(order['order_number'])
                if not order_id:
                    self.logger.warning(f"Order ID not found for order number {order['order_number']}")
                    continue
                    
                for line in order['line_details']:
                    order_details.append((
                        order_id, line['product_id'], line['quantity'], line['unit_price'],
                        line['discount_percentage'], line['final_unit_price'], line['line_item_tax_amount']
                    ))
            
            self.logger.info(f"Inserting {len(order_details)} order detail lines")
            
            execute_batch(
                self.cursor,
                "INSERT INTO order_details (order_id, product_id, quantity, unit_price, discount_percentage, final_unit_price, line_item_tax_amount) VALUES (%s, %s, %s, %s, %s, %s, %s);",
                order_details,
                page_size=self.config.batch_size
            )
            
            self.logger.info("Order details inserted successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to insert order details: {e}")
            if order_details:
                self.logger.error(f"Sample order detail data: {order_details[0]}")
            raise
        
        self.conn.commit()
        self.logger.info(f"Generated {len(orders)} sales orders with {len(order_details)} line items")