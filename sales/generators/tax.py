import random
from psycopg2.extras import execute_batch
from .base import BaseGenerator


class TaxGenerator(BaseGenerator):
    """Generator for tax types and rates."""
    
    def generate_tax_data(self):
        """Generate tax types and rates with historical changes."""
        self.logger.info("Generating tax data...")
        
        # Reset tax-related sequences to start from 1
        try:
            self.cursor.execute("ALTER SEQUENCE tax_types_tax_type_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE tax_rates_tax_rate_id_seq RESTART WITH 1;")
            self.conn.commit()
            self.logger.debug("Reset tax-related sequences to start from 1")
        except Exception as e:
            self.logger.warning(f"Could not reset tax sequences: {e}")
        
        # Ensure tax types exist (may not be populated if init script wasn't run with --populate-ref)
        required_tax_types = [
            ('VAT', 'Value Added Tax'),
            ('SALES_TAX', 'Sales Tax'),
            ('GST', 'Goods and Services Tax'),
            ('EXCISE', 'Excise Tax'),
            ('CUSTOM_DUTY', 'Custom Duty')
        ]
        
        execute_batch(
            self.cursor,
            "INSERT INTO tax_types (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING;",
            required_tax_types,
            page_size=self.config.batch_size
        )
        
        # Get tax type IDs
        self.cursor.execute("SELECT tax_type_id, name FROM tax_types;")
        tax_type_ids = {name: id for id, name in self.cursor.fetchall()}
        
        # Generate tax rates for countries
        tax_rates = []
        
        for country_code, country_info in self.cache['countries'].items():
            if 'id' not in country_info:
                continue
                
            country_id = country_info['id']
            region = country_info['region']
            
            # Regional tax patterns
            if region == 'Europe':
                # VAT-based system
                vat_rate = random.uniform(0.15, 0.25)  # 15-25% VAT
                tax_rates.append((country_id, None, tax_type_ids['VAT'], vat_rate, self.config.start_date, None))
                
            elif region == 'North America':
                # Sales tax system
                sales_tax_rate = random.uniform(0.05, 0.15)  # 5-15% sales tax
                tax_rates.append((country_id, None, tax_type_ids['SALES_TAX'], sales_tax_rate, self.config.start_date, None))
                
            elif region in ['Asia', 'Oceania']:
                # Mixed GST/VAT system
                gst_rate = random.uniform(0.08, 0.20)  # 8-20% GST
                tax_rates.append((country_id, None, tax_type_ids['GST'], gst_rate, self.config.start_date, None))
                
            else:
                # Default VAT system
                vat_rate = random.uniform(0.10, 0.18)  # 10-18% VAT
                tax_rates.append((country_id, None, tax_type_ids['VAT'], vat_rate, self.config.start_date, None))
            
            # Add customs duty
            duty_rate = random.uniform(0.02, 0.08)  # 2-8% customs duty
            tax_rates.append((country_id, None, tax_type_ids['CUSTOM_DUTY'], duty_rate, self.config.start_date, None))
        
        execute_batch(
            self.cursor,
            "INSERT INTO tax_rates (country_id, territory_id, tax_type_id, rate, effective_date, end_date) VALUES (%s, %s, %s, %s, %s, %s);",
            tax_rates,
            page_size=self.config.batch_size
        )
        
        self.conn.commit()
        self.logger.info(f"Generated {len(tax_rates)} tax rates")