#!/usr/bin/env python3
"""
International Sales Database Data Generator

This script generates comprehensive synthetic data for the international sales database
with realistic business patterns, multi-currency operations, and temporal consistency.

Usage:
    python generate_data.py [options]

Options:
    --preset {small,medium,large,enterprise}  Use predefined data volumes
    --customers N                             Number of customers
    --orders N                                Number of sales orders
    --products N                              Number of products
    --suppliers N                             Number of suppliers
    --employees N                             Number of employees
    --date-range START-END                    Date range (YYYY-YYYY)
    --only TABLE1,TABLE2                      Generate only specific tables
    --validate-only                           Validate existing data only
    --batch-size N                            Batch size for inserts (default: 1000)
    --verbose                                 Enable verbose logging
    --help                                    Show this help message
"""

import os
import sys
import argparse
import logging
import random
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import execute_batch
    import numpy as np
    import pandas as pd
    from faker import Faker
    from faker.providers import BaseProvider
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Error: Missing required dependency: {e}")
    print("Install with: uv sync")
    sys.exit(1)


@dataclass
class GenerationConfig:
    """Configuration for data generation."""
    # Volume settings
    countries: int = 50
    territories: int = 200
    employees: int = 500
    product_categories: int = 100
    products: int = 2000
    suppliers: int = 200
    customers: int = 1000
    purchase_orders: int = 5000
    sales_orders: int = 10000
    
    # Date settings
    start_date: date = date(2022, 1, 1)
    end_date: date = date(2024, 12, 31)
    
    # Database settings
    batch_size: int = 1000
    
    # Generation settings
    exchange_rate_days: int = 1095  # 3 years of daily rates
    seasonal_factor: float = 0.3    # Sales seasonality strength
    hierarchy_depth: int = 4        # Employee hierarchy levels
    
    # Business logic settings
    avg_order_lines: int = 3
    avg_po_lines: int = 5
    inventory_turnover: float = 6.0  # Times per year
    
    @classmethod
    def from_preset(cls, preset: str) -> 'GenerationConfig':
        """Create config from preset."""
        presets = {
            'small': cls(
                countries=25, territories=100, employees=100,
                products=500, suppliers=50, customers=200,
                purchase_orders=1000, sales_orders=2000,
                start_date=date(2023, 7, 1), end_date=date(2024, 12, 31)
            ),
            'medium': cls(
                countries=40, territories=150, employees=300,
                products=1000, suppliers=100, customers=500,
                purchase_orders=2500, sales_orders=5000,
                start_date=date(2023, 1, 1), end_date=date(2024, 12, 31)
            ),
            'large': cls(),  # Default values
            'enterprise': cls(
                countries=75, territories=300, employees=1000,
                products=5000, suppliers=500, customers=2000,
                purchase_orders=15000, sales_orders=25000,
                start_date=date(2020, 1, 1), end_date=date(2024, 12, 31)
            )
        }
        return presets.get(preset, cls())


class BusinessProvider(BaseProvider):
    """Custom Faker provider for business-specific data."""
    
    def product_category(self) -> str:
        """Generate realistic product category names."""
        categories = [
            'Electronics', 'Computers', 'Software', 'Hardware', 'Networking',
            'Furniture', 'Office Supplies', 'Industrial Equipment', 'Tools',
            'Automotive', 'Medical Devices', 'Laboratory Equipment', 'Safety',
            'Food & Beverage', 'Pharmaceuticals', 'Chemicals', 'Textiles',
            'Construction Materials', 'Energy Equipment', 'Telecommunications'
        ]
        return self.random_element(categories)
    
    def product_sku(self) -> str:
        """Generate realistic product SKUs."""
        return f"{self.random_element(['PRD', 'ITM', 'SKU'])}-{self.random_int(1000, 9999)}-{self.lexify('???').upper()}"
    
    def tax_id(self, country_code: str = 'US') -> str:
        """Generate tax ID based on country."""
        if country_code == 'US':
            return f"{self.random_int(10, 99)}-{self.random_int(1000000, 9999999)}"
        elif country_code in ['GB', 'IE']:
            return f"GB{self.random_int(100000000, 999999999)}"
        elif country_code in ['DE', 'FR', 'IT']:
            return f"{country_code}{self.random_int(100000000, 999999999)}"
        else:
            return f"{country_code}{self.random_int(10000000, 99999999)}"
    
    def order_number(self) -> str:
        """Generate realistic order numbers."""
        return f"{self.random_element(['SO', 'ORD', 'INV'])}-{datetime.now().year}-{self.random_int(10000, 99999)}"
    
    def po_number(self) -> str:
        """Generate realistic PO numbers."""
        return f"PO-{datetime.now().year}-{self.random_int(10000, 99999)}"


class SalesDataGenerator:
    """Main class for generating international sales data."""
    
    def __init__(self, config: GenerationConfig, verbose: bool = False):
        self.config = config
        self.verbose = verbose
        self.logger = self._setup_logging()
        
        # Database connection
        self.conn = None
        self.cursor = None
        
        # Faker instances for different locales
        self.fake_us = Faker('en_US')
        self.fake_uk = Faker('en_GB')
        self.fake_de = Faker('de_DE')
        self.fake_fr = Faker('fr_FR')
        self.fake_jp = Faker('ja_JP')
        self.fake_cn = Faker('zh_CN')
        
        # Add custom provider to all fakers
        for fake in [self.fake_us, self.fake_uk, self.fake_de, self.fake_fr, self.fake_jp, self.fake_cn]:
            fake.add_provider(BusinessProvider)
        
        # Caches for generated data
        self.cache = {
            'countries': {},
            'territories': {},
            'currencies': {},
            'employees': {},
            'products': {},
            'suppliers': {},
            'customers': {}
        }
        
        # Business constants
        self.major_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'INR', 'BRL']
        self.regions = ['North America', 'Europe', 'Asia', 'Oceania', 'South America', 'Africa']
    
    def _safe_faker_call(self, faker, method_name, fallback_value, *args, **kwargs):
        """Safely call a faker method with fallback if the method doesn't exist."""
        try:
            method = getattr(faker, method_name)
            return method(*args, **kwargs)
        except AttributeError:
            return fallback_value
    
    def _safe_secondary_address(self, faker):
        """Generate secondary address with locale-safe fallback."""
        return self._safe_faker_call(
            faker, 
            'secondary_address', 
            f"Unit {random.randint(1, 999)}"
        )
    
    def _safe_state(self, faker):
        """Generate state with locale-safe fallback."""
        return self._safe_faker_call(
            faker,
            'state',
            faker.city()  # Fallback to city if state() doesn't exist
        )
        
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        level = logging.DEBUG if self.verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        return logging.getLogger(__name__)
    
    def connect_database(self) -> bool:
        """Connect to the database."""
        try:
            load_dotenv()
            
            self.conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', '5432')),
                database=os.getenv('DB_NAME', 'sales'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('PGPASSWORD', '')
            )
            self.cursor = self.conn.cursor()
            self.logger.info("Database connection established")
            return True
            
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            return False
    
    def disconnect_database(self):
        """Disconnect from the database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Database connection closed")
    
    def clear_existing_data(self, tables: List[str] = None):
        """Clear existing data from specified tables."""
        if tables is None:
            # Clear all tables in dependency order
            tables = [
                'purchase_order_line_costs', 'purchase_order_details', 'purchase_orders',
                'product_costs', 'inventory', 'product_suppliers', 'product_prices',
                'order_details', 'orders', 'customer_addresses', 'sales_targets',
                'employees', 'customers', 'suppliers', 'products', 'product_categories',
                'shipping_methods', 'addresses', 'tax_rates', 'territories',
                'exchange_rates', 'countries', 'currencies', 'tax_types', 'roles', 'cost_types'
            ]
        
        self.logger.info(f"Clearing data from {len(tables)} tables...")
        
        for table in tables:
            try:
                self.cursor.execute(f"DELETE FROM {table} CASCADE;")
                self.conn.commit()
                self.logger.debug(f"Cleared table: {table}")
            except Exception as e:
                self.logger.warning(f"Could not clear table {table}: {e}")
                self.conn.rollback()
    
    def generate_geographic_data(self):
        """Generate countries, territories, and addresses."""
        self.logger.info("Generating geographic data...")
        
        # Country data with realistic distribution
        country_data = [
            # North America
            ('United States', 'USA', 'North America', 'USD', self.fake_us),
            ('Canada', 'CAN', 'North America', 'CAD', self.fake_us),
            ('Mexico', 'MEX', 'North America', 'USD', self.fake_us),
            
            # Europe
            ('United Kingdom', 'GBR', 'Europe', 'GBP', self.fake_uk),
            ('Germany', 'DEU', 'Europe', 'EUR', self.fake_de),
            ('France', 'FRA', 'Europe', 'EUR', self.fake_fr),
            ('Italy', 'ITA', 'Europe', 'EUR', self.fake_de),
            ('Spain', 'ESP', 'Europe', 'EUR', self.fake_de),
            ('Netherlands', 'NLD', 'Europe', 'EUR', self.fake_de),
            ('Switzerland', 'CHE', 'Europe', 'CHF', self.fake_de),
            ('Austria', 'AUT', 'Europe', 'EUR', self.fake_de),
            ('Belgium', 'BEL', 'Europe', 'EUR', self.fake_de),
            ('Sweden', 'SWE', 'Europe', 'EUR', self.fake_de),
            ('Norway', 'NOR', 'Europe', 'EUR', self.fake_de),
            ('Denmark', 'DNK', 'Europe', 'EUR', self.fake_de),
            
            # Asia
            ('Japan', 'JPN', 'Asia', 'JPY', self.fake_jp),
            ('China', 'CHN', 'Asia', 'CNY', self.fake_cn),
            ('India', 'IND', 'Asia', 'INR', self.fake_us),
            ('South Korea', 'KOR', 'Asia', 'USD', self.fake_us),
            ('Singapore', 'SGP', 'Asia', 'USD', self.fake_us),
            ('Hong Kong', 'HKG', 'Asia', 'USD', self.fake_us),
            ('Taiwan', 'TWN', 'Asia', 'USD', self.fake_us),
            ('Thailand', 'THA', 'Asia', 'USD', self.fake_us),
            ('Malaysia', 'MYS', 'Asia', 'USD', self.fake_us),
            ('Indonesia', 'IDN', 'Asia', 'USD', self.fake_us),
            
            # Oceania
            ('Australia', 'AUS', 'Oceania', 'AUD', self.fake_us),
            ('New Zealand', 'NZL', 'Oceania', 'AUD', self.fake_us),
            
            # South America
            ('Brazil', 'BRA', 'South America', 'BRL', self.fake_us),
            ('Argentina', 'ARG', 'South America', 'USD', self.fake_us),
            ('Chile', 'CHL', 'South America', 'USD', self.fake_us),
            ('Colombia', 'COL', 'South America', 'USD', self.fake_us),
            
            # Africa
            ('South Africa', 'ZAF', 'Africa', 'USD', self.fake_us),
            ('Egypt', 'EGY', 'Africa', 'USD', self.fake_us),
            ('Nigeria', 'NGA', 'Africa', 'USD', self.fake_us),
            ('Kenya', 'KEN', 'Africa', 'USD', self.fake_us),
        ]
        
        # Limit to config.countries
        country_data = country_data[:self.config.countries]
        
        # Insert countries
        countries_insert = []
        for name, code, region, currency, faker in country_data:
            countries_insert.append((name, code, region, currency))
            self.cache['countries'][code] = {
                'name': name, 'code': code, 'region': region, 
                'currency': currency, 'faker': faker
            }
        
        execute_batch(
            self.cursor,
            "INSERT INTO countries (name, code, region, currency_code) VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING;",
            countries_insert,
            page_size=self.config.batch_size
        )
        
        # Get country IDs
        self.cursor.execute("SELECT country_id, code FROM countries;")
        for country_id, code in self.cursor.fetchall():
            if code in self.cache['countries']:
                self.cache['countries'][code]['id'] = country_id
        
        # Generate territories
        territories_insert = []
        territory_id = 1
        
        for country_code, country_info in self.cache['countries'].items():
            if 'id' not in country_info:
                continue
                
            faker = country_info['faker']
            country_id = country_info['id']
            
            # Number of territories per country (weighted by region)
            if country_info['region'] == 'North America':
                num_territories = random.randint(8, 15)
            elif country_info['region'] == 'Europe':
                num_territories = random.randint(5, 12)
            elif country_info['region'] == 'Asia':
                num_territories = random.randint(3, 10)
            else:
                num_territories = random.randint(2, 8)
            
            # Limit total territories
            if len(territories_insert) + num_territories > self.config.territories:
                num_territories = self.config.territories - len(territories_insert)
            
            # Track used territory names for this country to avoid duplicates
            used_names = set()
            attempts = 0
            max_attempts = num_territories * 3  # Allow multiple attempts to find unique names
            
            for _ in range(num_territories):
                if attempts >= max_attempts:
                    break
                    
                # Generate unique territory name for this country
                while attempts < max_attempts:
                    attempts += 1
                    
                    # Try different name generation methods
                    if random.random() < 0.7:
                        territory_name = self._safe_state(faker)
                    else:
                        # Fallback to city names with region suffixes
                        base_name = faker.city()
                        suffixes = ['Region', 'District', 'Province', 'Area', 'Zone', 'Territory']
                        territory_name = f"{base_name} {random.choice(suffixes)}"
                    
                    # Ensure uniqueness within this country
                    if territory_name not in used_names:
                        used_names.add(territory_name)
                        break
                else:
                    # If we can't find a unique name, create one with a number
                    territory_name = f"{faker.city()} Territory {len(used_names) + 1}"
                    used_names.add(territory_name)
                
                territories_insert.append((territory_name, country_id))
                self.cache['territories'][territory_id] = {
                    'name': territory_name,
                    'country_id': country_id,
                    'country_code': country_code,
                    'currency': country_info['currency'],
                    'faker': faker
                }
                territory_id += 1
                
                if len(territories_insert) >= self.config.territories:
                    break
            
            if len(territories_insert) >= self.config.territories:
                break
        
        execute_batch(
            self.cursor,
            "INSERT INTO territories (name, country_id) VALUES (%s, %s);",
            territories_insert,
            page_size=self.config.batch_size
        )
        
        # Get territory IDs
        self.cursor.execute("SELECT territory_id, name, country_id FROM territories;")
        territory_lookup = {}
        for territory_id, name, country_id in self.cursor.fetchall():
            territory_lookup[(name, country_id)] = territory_id
        
        # Update cache with real territory IDs
        for cache_id, territory_info in self.cache['territories'].items():
            real_id = territory_lookup.get((territory_info['name'], territory_info['country_id']))
            if real_id:
                territory_info['id'] = real_id
        
        self.conn.commit()
        self.logger.info(f"Generated {len(countries_insert)} countries and {len(territories_insert)} territories")
    
    def generate_currency_data(self):
        """Generate currencies and exchange rates."""
        self.logger.info("Generating currency and exchange rate data...")
        
        # Extended currency list
        currencies_data = [
            ('USD', 'US Dollar', '$'),
            ('EUR', 'Euro', '€'),
            ('GBP', 'British Pound', '£'),
            ('JPY', 'Japanese Yen', '¥'),
            ('CAD', 'Canadian Dollar', 'C$'),
            ('AUD', 'Australian Dollar', 'A$'),
            ('CHF', 'Swiss Franc', 'CHF'),
            ('CNY', 'Chinese Yuan', '¥'),
            ('INR', 'Indian Rupee', '₹'),
            ('BRL', 'Brazilian Real', 'R$'),
            ('KRW', 'South Korean Won', '₩'),
            ('SGD', 'Singapore Dollar', 'S$'),
            ('HKD', 'Hong Kong Dollar', 'HK$'),
            ('SEK', 'Swedish Krona', 'kr'),
            ('NOK', 'Norwegian Krone', 'kr'),
            ('DKK', 'Danish Krone', 'kr'),
            ('MXN', 'Mexican Peso', '$'),
            ('ZAR', 'South African Rand', 'R'),
            ('THB', 'Thai Baht', '฿'),
            ('MYR', 'Malaysian Ringgit', 'RM')
        ]
        
        # Insert currencies
        execute_batch(
            self.cursor,
            "INSERT INTO currencies (currency_code, name, symbol) VALUES (%s, %s, %s) ON CONFLICT (currency_code) DO NOTHING;",
            currencies_data,
            page_size=self.config.batch_size
        )
        
        # Store currency info
        for code, name, symbol in currencies_data:
            self.cache['currencies'][code] = {'name': name, 'symbol': symbol}
        
        # Generate historical exchange rates
        self.logger.info("Generating historical exchange rates...")
        
        # Base exchange rates (approximate realistic values)
        # Note: Keeping rates reasonable to avoid field overflow in DECIMAL(15,6)
        base_rates = {
            ('USD', 'EUR'): 0.85,
            ('USD', 'GBP'): 0.75,
            ('USD', 'JPY'): 110.0,
            ('USD', 'CAD'): 1.25,
            ('USD', 'AUD'): 1.35,
            ('USD', 'CHF'): 0.92,
            ('USD', 'CNY'): 6.8,
            ('USD', 'INR'): 75.0,
            ('USD', 'BRL'): 5.2,
            ('USD', 'SGD'): 1.35,
            ('USD', 'HKD'): 7.8,
            ('USD', 'SEK'): 8.5,
            ('USD', 'NOK'): 8.8,
            ('USD', 'DKK'): 6.3,
            ('USD', 'MXN'): 18.5,
            ('USD', 'ZAR'): 14.2,
            ('USD', 'THB'): 32.5,
            ('USD', 'MYR'): 4.2,
        }
        
        # Generate daily rates with realistic volatility
        exchange_rates = []
        current_date = self.config.start_date - timedelta(days=365)  # Start earlier for history
        end_date = self.config.end_date
        
        # Initialize current rates
        current_rates = base_rates.copy()
        
        while current_date <= end_date:
            for (from_curr, to_curr), base_rate in base_rates.items():
                # Add random walk with mean reversion
                volatility = 0.003  # 0.3% daily volatility (reduced)
                mean_reversion = 0.05  # Stronger mean reversion
                
                random_change = np.random.normal(0, volatility)
                mean_reversion_force = mean_reversion * (base_rate - current_rates[(from_curr, to_curr)])
                
                new_rate = current_rates[(from_curr, to_curr)] * (1 + random_change + mean_reversion_force)
                
                # Additional bounds checking to prevent drift
                # Don't let rates move more than 50% from base rate
                min_rate = base_rate * 0.5
                max_rate = base_rate * 1.5
                new_rate = max(min_rate, min(new_rate, max_rate))
                
                # Ensure rate stays within reasonable bounds to avoid database overflow
                # DECIMAL(15,6) can handle values up to 999,999,999.999999
                # But we'll keep rates within realistic forex ranges
                new_rate = max(0.000001, min(new_rate, 100000.0))  # Max 100,000:1 ratio
                current_rates[(from_curr, to_curr)] = new_rate
                
                exchange_rates.append((from_curr, to_curr, round(new_rate, 6), current_date))
                
                # Add reverse rate with overflow protection
                if new_rate > 0.000001:  # Avoid division by very small numbers
                    reverse_rate = 1.0 / new_rate
                    # Ensure reverse rate also stays within bounds (max 1,000,000:1)
                    reverse_rate = max(0.000001, min(reverse_rate, 1000000.0))
                    exchange_rates.append((to_curr, from_curr, round(reverse_rate, 6), current_date))
            
            current_date += timedelta(days=1)
            
            # Batch insert to avoid memory issues
            if len(exchange_rates) >= 10000:
                execute_batch(
                    self.cursor,
                    "INSERT INTO exchange_rates (from_currency, to_currency, rate, effective_date) VALUES (%s, %s, %s, %s) ON CONFLICT (from_currency, to_currency, effective_date) DO NOTHING;",
                    exchange_rates,
                    page_size=self.config.batch_size
                )
                exchange_rates = []
        
        # Insert remaining rates
        if exchange_rates:
            execute_batch(
                self.cursor,
                "INSERT INTO exchange_rates (from_currency, to_currency, rate, effective_date) VALUES (%s, %s, %s, %s) ON CONFLICT (from_currency, to_currency, effective_date) DO NOTHING;",
                exchange_rates,
                page_size=self.config.batch_size
            )
        
        self.conn.commit()
        self.logger.info(f"Generated {len(currencies_data)} currencies and historical exchange rates")
    
    def generate_tax_data(self):
        """Generate tax types and rates with historical changes."""
        self.logger.info("Generating tax data...")
        
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
    
    def generate_hr_data(self):
        """Generate roles and employees with realistic hierarchy."""
        self.logger.info("Generating HR data...")
        
        # Ensure roles exist (may not be populated if init script wasn't run with --populate-ref)
        required_roles = [
            ('CEO', 'Chief Executive Officer'),
            ('Sales Manager', 'Sales Team Manager'),
            ('Sales Representative', 'Sales Representative'),
            ('Procurement Manager', 'Procurement Team Manager'),
            ('Inventory Manager', 'Inventory Management'),
            ('Customer Service', 'Customer Service Representative'),
            ('Finance Manager', 'Finance Team Manager'),
            ('Operations Manager', 'Operations Team Manager')
        ]
        
        # Insert roles if they don't exist
        execute_batch(
            self.cursor,
            "INSERT INTO roles (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING;",
            required_roles,
            page_size=self.config.batch_size
        )
        
        # Get roles mapping
        self.cursor.execute("SELECT role_id, name FROM roles;")
        roles = {name: id for id, name in self.cursor.fetchall()}
        
        # Verify CEO role exists
        if 'CEO' not in roles:
            self.logger.error("CEO role not found in database")
            raise Exception("Required CEO role not found in roles table")
        
        # Employee generation with hierarchy
        employees = []
        employee_id = 1
        used_emails = set()
        
        # Create CEO first
        ceo_territory = random.choice(list(self.cache['territories'].values()))
        faker = ceo_territory['faker']
        
        ceo_email = faker.email()
        used_emails.add(ceo_email)
        
        ceo = {
            'id': employee_id,
            'name': faker.name(),
            'email': ceo_email,
            'role_id': roles['CEO'],
            'territory_id': ceo_territory['id'],
            'manager_id': None,
            'salary': round(random.uniform(200000, 350000), 2),
            'salary_currency': ceo_territory['currency'],
            'hire_date': self.config.start_date - timedelta(days=random.randint(1000, 2000))
        }
        
        employees.append(ceo)
        self.cache['employees'][employee_id] = ceo
        employee_id += 1
        
        # Create regional managers
        managers = []
        territories_by_region = {}
        
        for territory_info in self.cache['territories'].values():
            if 'id' not in territory_info:
                continue
            region = self.cache['countries'][territory_info['country_code']]['region']
            if region not in territories_by_region:
                territories_by_region[region] = []
            territories_by_region[region].append(territory_info)
        
        # Create managers for each region
        for region, region_territories in territories_by_region.items():
            if not region_territories:
                continue
                
            num_managers = max(1, len(region_territories) // 5)  # 1 manager per 5 territories
            
            for _ in range(min(num_managers, len(region_territories))):
                territory = random.choice(region_territories)
                faker = territory['faker']
                
                # Generate unique email
                attempts = 0
                max_attempts = 10
                while attempts < max_attempts:
                    manager_email = faker.email()
                    if manager_email not in used_emails:
                        used_emails.add(manager_email)
                        break
                    attempts += 1
                else:
                    manager_email = f"manager{employee_id}@{faker.domain_name()}"
                    used_emails.add(manager_email)
                
                manager = {
                    'id': employee_id,
                    'name': faker.name(),
                    'email': manager_email,
                    'role_id': roles.get('Sales Manager', roles.get('Operations Manager', roles['CEO'])),
                    'territory_id': territory['id'],
                    'manager_id': ceo['id'],
                    'salary': round(random.uniform(80000, 150000), 2),
                    'salary_currency': territory['currency'],
                    'hire_date': ceo['hire_date'] + timedelta(days=random.randint(30, 365))
                }
                
                employees.append(manager)
                managers.append(manager)
                self.cache['employees'][employee_id] = manager
                employee_id += 1
                
                if employee_id > self.config.employees:
                    break
            
            if employee_id > self.config.employees:
                break
        
        # Create sales reps and other staff
        remaining_slots = self.config.employees - len(employees)
        
        for _ in range(remaining_slots):
            # Choose territory
            territory = random.choice(list(self.cache['territories'].values()))
            if 'id' not in territory:
                continue
                
            faker = territory['faker']
            
            # Find appropriate manager (prefer same region)
            region = self.cache['countries'][territory['country_code']]['region']
            region_managers = [m for m in managers if self.cache['territories'].get(m['territory_id'], {}).get('country_code') in 
                             [k for k, v in self.cache['countries'].items() if v['region'] == region]]
            
            manager = random.choice(region_managers) if region_managers else random.choice(managers) if managers else ceo
            
            # Choose role based on hierarchy
            role_weights = {
                'Sales Representative': 0.4,
                'Customer Service': 0.2,
                'Procurement Manager': 0.1,
                'Inventory Manager': 0.1,
                'Finance Manager': 0.1,
                'Operations Manager': 0.1
            }
            
            role_name = np.random.choice(list(role_weights.keys()), p=list(role_weights.values()))
            
            # Salary based on role
            salary_ranges = {
                'Sales Representative': (45000, 85000),
                'Customer Service': (35000, 65000),
                'Procurement Manager': (60000, 100000),
                'Inventory Manager': (55000, 90000),
                'Finance Manager': (70000, 120000),
                'Operations Manager': (65000, 110000)
            }
            
            salary_range = salary_ranges.get(role_name, (40000, 80000))
            
            # Generate unique email
            attempts = 0
            max_attempts = 10
            while attempts < max_attempts:
                employee_email = faker.email()
                if employee_email not in used_emails:
                    used_emails.add(employee_email)
                    break
                attempts += 1
            else:
                employee_email = f"employee{employee_id}@{faker.domain_name()}"
                used_emails.add(employee_email)
            
            employee = {
                'id': employee_id,
                'name': faker.name(),
                'email': employee_email,
                'role_id': roles.get(role_name, roles['Sales Representative']),
                'territory_id': territory['id'],
                'manager_id': manager['id'],
                'salary': round(random.uniform(*salary_range), 2),
                'salary_currency': territory['currency'],
                'hire_date': ceo['hire_date'] + timedelta(days=random.randint(0, 1000))
            }
            
            employees.append(employee)
            self.cache['employees'][employee_id] = employee
            employee_id += 1
        
        # Insert employees in hierarchy order to avoid foreign key issues
        
        # First, insert CEO without manager
        ceo_insert = [(
            ceo['name'], ceo['email'], ceo['role_id'], ceo['territory_id'], 
            None, ceo['salary'], ceo['salary_currency'], ceo['hire_date']
        )]
        
        execute_batch(
            self.cursor,
            "INSERT INTO employees (name, email, role_id, territory_id, manager_id, salary, salary_currency_code, hire_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
            ceo_insert,
            page_size=self.config.batch_size
        )
        
        # Get CEO's real database ID
        self.cursor.execute("SELECT employee_id FROM employees WHERE email = %s;", (ceo['email'],))
        ceo_real_id = self.cursor.fetchone()[0]
        ceo['real_id'] = ceo_real_id
        
        # Update manager references to use real CEO ID
        for manager in managers:
            manager['manager_id'] = ceo_real_id
        
        # Insert managers
        if managers:
            managers_insert = [
                (mgr['name'], mgr['email'], mgr['role_id'], mgr['territory_id'], 
                 mgr['manager_id'], mgr['salary'], mgr['salary_currency'], mgr['hire_date'])
                for mgr in managers
            ]
            
            execute_batch(
                self.cursor,
                "INSERT INTO employees (name, email, role_id, territory_id, manager_id, salary, salary_currency_code, hire_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
                managers_insert,
                page_size=self.config.batch_size
            )
        
        # Get manager real IDs
        if managers:
            self.cursor.execute("SELECT employee_id, email FROM employees WHERE email = ANY(%s);", 
                              ([mgr['email'] for mgr in managers],))
            manager_email_to_id = {email: emp_id for emp_id, email in self.cursor.fetchall()}
            
            for mgr in managers:
                real_id = manager_email_to_id.get(mgr['email'])
                if real_id:
                    mgr['real_id'] = real_id
        
        # Update other employees' manager references to use real manager IDs
        other_employees = [emp for emp in employees if emp not in [ceo] + managers]
        for emp in other_employees:
            # Find the manager this employee reports to
            manager_cache_id = emp['manager_id']
            for mgr in managers:
                if mgr['id'] == manager_cache_id and 'real_id' in mgr:
                    emp['manager_id'] = mgr['real_id']
                    break
        
        # Insert other employees
        if other_employees:
            other_employees_insert = [
                (emp['name'], emp['email'], emp['role_id'], emp['territory_id'], 
                 emp['manager_id'], emp['salary'], emp['salary_currency'], emp['hire_date'])
                for emp in other_employees
            ]
            
            execute_batch(
                self.cursor,
                "INSERT INTO employees (name, email, role_id, territory_id, manager_id, salary, salary_currency_code, hire_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
                other_employees_insert,
                page_size=self.config.batch_size
            )
        
        # Update cache with real employee IDs for all employees
        self.cursor.execute("SELECT employee_id, email FROM employees;")
        email_to_id = {email: emp_id for emp_id, email in self.cursor.fetchall()}
        
        for emp in employees:
            real_id = email_to_id.get(emp['email'])
            if real_id:
                emp['real_id'] = real_id
        
        self.conn.commit()
        self.logger.info(f"Generated {len(employees)} employees with hierarchy")
    
    def validate_data_integrity(self) -> bool:
        """Validate generated data for consistency."""
        self.logger.info("Validating data integrity...")
        
        validation_queries = [
            ("Countries without territories", "SELECT COUNT(*) FROM countries c LEFT JOIN territories t ON c.country_id = t.country_id WHERE t.territory_id IS NULL"),
            ("Employees without managers (except CEO)", "SELECT COUNT(*) FROM employees WHERE manager_id IS NULL AND role_id != (SELECT role_id FROM roles WHERE name = 'CEO')"),
            ("Products without categories", "SELECT COUNT(*) FROM products p LEFT JOIN product_categories pc ON p.category_id = pc.category_id WHERE pc.category_id IS NULL"),
            ("Orders without customers", "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL"),
        ]
        
        all_valid = True
        for description, query in validation_queries:
            self.cursor.execute(query)
            count = self.cursor.fetchone()[0]
            if count > 0:
                self.logger.warning(f"{description}: {count} records")
                all_valid = False
            else:
                self.logger.debug(f"{description}: OK")
        
        return all_valid
    
    def generate_product_data(self):
        """Generate product categories, products, pricing, and costs."""
        self.logger.info("Generating product catalog...")
        
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
    
    def generate_supplier_data(self):
        """Generate suppliers and purchase orders."""
        self.logger.info("Generating supplier data...")
        
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
            
            for product in products_with_real_ids:
                # Each product has 1-3 suppliers
                num_suppliers = random.randint(1, min(3, len(suppliers_with_real_ids)))
                product_suppliers_list = random.sample(suppliers_with_real_ids, num_suppliers)
                
                for i, supplier in enumerate(product_suppliers_list):
                    # Safely get territory currency
                    try:
                        territory_id = supplier['territory_id']
                        if territory_id not in self.cache['territories']:
                            self.logger.warning(f"Territory {territory_id} not found for supplier {supplier['id']}")
                            continue
                        currency = self.cache['territories'][territory_id]['currency']
                    except (KeyError, TypeError) as e:
                        self.logger.warning(f"Could not get currency for supplier {supplier.get('id', 'unknown')}: {e}")
                        currency = 'USD'  # Fallback currency
                    
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
                'currency_code': self.cache['territories'][supplier['territory_id']]['currency'],
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
    
    def generate_customer_data(self):
        """Generate customers and their addresses."""
        self.logger.info("Generating customer data...")
        
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
    
    def generate_sales_data(self):
        """Generate sales orders with seasonal patterns and realistic business logic."""
        self.logger.info("Generating sales orders...")
        
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
                    try:
                        currency = self.cache['territories'][customer['territory_id']]['currency']
                    except KeyError:
                        self.logger.warning(f"Territory {customer.get('territory_id', 'unknown')} not found for customer {customer.get('real_id', 'unknown')}, using USD")
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
    
    def generate_inventory_data(self):
        """Generate inventory levels across territories."""
        self.logger.info("Generating inventory data...")
        
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
        
        sales_targets = []
        sales_employees = [e for e in self.cache['employees'].values() 
                          if 'real_id' in e and 'Sales' in str(e.get('role_id', ''))]
        
        for employee in sales_employees:
            # Annual targets for current and next year
            for target_year in [self.config.end_date.year, self.config.end_date.year + 1]:
                territory_currency = self.cache['territories'][employee['territory_id']]['currency']
                
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
    
    def generate_all_data(self, clear_existing: bool = False, tables_to_generate: List[str] = None):
        """Generate complete dataset."""
        self.logger.info("Starting comprehensive data generation...")
        
        start_time = datetime.now()
        
        try:
            if not self.connect_database():
                return False
            
            if clear_existing:
                self.clear_existing_data()
            
            # Default generation order - IMPORTANT: currencies must come before geographic
            # because countries have foreign key references to currencies
            all_generators = [
                ('currency', self.generate_currency_data),
                ('geographic', self.generate_geographic_data),
                ('tax', self.generate_tax_data),
                ('hr', self.generate_hr_data),
                ('products', self.generate_product_data),
                ('suppliers', self.generate_supplier_data),
                ('customers', self.generate_customer_data),
                ('sales', self.generate_sales_data),
                ('inventory', self.generate_inventory_data),
            ]
            
            # Filter generators if specific tables requested
            if tables_to_generate:
                generators = [(name, func) for name, func in all_generators if name in tables_to_generate]
            else:
                generators = all_generators
            
            # Execute generators
            for name, generator_func in generators:
                self.logger.info(f"Generating {name} data...")
                try:
                    generator_func()
                    self.logger.info(f"✓ {name} data generation completed")
                except Exception as e:
                    self.logger.error(f"✗ {name} data generation failed: {e}")
                    raise
            
            # Final validation
            self.logger.info("Running data integrity validation...")
            if self.validate_data_integrity():
                self.logger.info("✓ Data integrity validation passed")
            else:
                self.logger.warning("⚠ Data integrity validation found issues")
            
            elapsed = datetime.now() - start_time
            self.logger.info(f"Data generation completed in {elapsed}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Data generation failed: {e}")
            return False
        finally:
            self.disconnect_database()


def main():
    """Main function to handle command line arguments and run data generation."""
    parser = argparse.ArgumentParser(
        description='Generate synthetic data for the international sales database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Preset configurations
    parser.add_argument(
        '--preset',
        choices=['small', 'medium', 'large', 'enterprise'],
        help='Use predefined data volume preset'
    )
    
    # Volume controls
    parser.add_argument('--countries', type=int, help='Number of countries')
    parser.add_argument('--territories', type=int, help='Number of territories')
    parser.add_argument('--customers', type=int, help='Number of customers')
    parser.add_argument('--orders', type=int, help='Number of sales orders')
    parser.add_argument('--products', type=int, help='Number of products')
    parser.add_argument('--suppliers', type=int, help='Number of suppliers')
    parser.add_argument('--employees', type=int, help='Number of employees')
    parser.add_argument('--purchase-orders', type=int, help='Number of purchase orders')
    
    # Date range
    parser.add_argument(
        '--date-range',
        help='Date range in format YYYY-YYYY (e.g., 2022-2024)'
    )
    
    # Selective generation
    parser.add_argument(
        '--only',
        help='Generate only specific data types (comma-separated): geographic,currency,tax,hr,products,suppliers,customers,sales,inventory'
    )
    
    # Database options
    parser.add_argument(
        '--clear-existing',
        action='store_true',
        help='Clear existing data before generating new data'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for database inserts (default: 1000)'
    )
    
    # Validation
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate existing data, do not generate new data'
    )
    
    # Logging
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    try:
        # Create configuration
        if args.preset:
            config = GenerationConfig.from_preset(args.preset)
            print(f"Using preset: {args.preset}")
        else:
            config = GenerationConfig()
        
        # Override with command line arguments
        if args.countries:
            config.countries = args.countries
        if args.territories:
            config.territories = args.territories
        if args.customers:
            config.customers = args.customers
        if args.orders:
            config.sales_orders = args.orders
        if args.products:
            config.products = args.products
        if args.suppliers:
            config.suppliers = args.suppliers
        if args.employees:
            config.employees = args.employees
        if args.purchase_orders:
            config.purchase_orders = args.purchase_orders
        if args.batch_size:
            config.batch_size = args.batch_size
        
        # Parse date range
        if args.date_range:
            try:
                start_year, end_year = args.date_range.split('-')
                config.start_date = date(int(start_year), 1, 1)
                config.end_date = date(int(end_year), 12, 31)
            except ValueError:
                print("Error: Date range must be in format YYYY-YYYY")
                sys.exit(1)
        
        # Parse selective generation
        tables_to_generate = None
        if args.only:
            tables_to_generate = [table.strip() for table in args.only.split(',')]
            valid_tables = ['geographic', 'currency', 'tax', 'hr', 'products', 'suppliers', 'customers', 'sales', 'inventory']
            invalid_tables = [t for t in tables_to_generate if t not in valid_tables]
            if invalid_tables:
                print(f"Error: Invalid table names: {', '.join(invalid_tables)}")
                print(f"Valid options: {', '.join(valid_tables)}")
                sys.exit(1)
        
        # Display configuration
        print("=" * 60)
        print("International Sales Database Data Generator")
        print("=" * 60)
        print(f"Configuration:")
        print(f"  Countries: {config.countries}")
        print(f"  Territories: {config.territories}")
        print(f"  Employees: {config.employees}")
        print(f"  Products: {config.products}")
        print(f"  Suppliers: {config.suppliers}")
        print(f"  Customers: {config.customers}")
        print(f"  Purchase Orders: {config.purchase_orders}")
        print(f"  Sales Orders: {config.sales_orders}")
        print(f"  Date Range: {config.start_date} to {config.end_date}")
        print(f"  Batch Size: {config.batch_size}")
        
        if tables_to_generate:
            print(f"  Tables to Generate: {', '.join(tables_to_generate)}")
        if args.clear_existing:
            print(f"  ⚠ Will clear existing data")
        
        print("=" * 60)
        
        # Initialize generator
        generator = SalesDataGenerator(config, verbose=args.verbose)
        
        if args.validate_only:
            # Validation only mode
            print("Running data validation...")
            if generator.connect_database():
                try:
                    if generator.validate_data_integrity():
                        print("✓ Data integrity validation passed")
                        sys.exit(0)
                    else:
                        print("✗ Data integrity validation failed")
                        sys.exit(1)
                finally:
                    generator.disconnect_database()
            else:
                print("✗ Could not connect to database")
                sys.exit(1)
        
        # Ask for confirmation for large datasets
        total_records_estimate = (
            config.countries + config.territories + config.employees + 
            config.products + config.suppliers + config.customers + 
            config.purchase_orders + config.sales_orders
        )
        
        if total_records_estimate > 10000 and not args.preset:
            response = input(f"\nThis will generate approximately {total_records_estimate:,} records. Continue? (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("Operation cancelled")
                sys.exit(0)
        
        # Generate data
        print("\nStarting data generation...")
        success = generator.generate_all_data(
            clear_existing=args.clear_existing,
            tables_to_generate=tables_to_generate
        )
        
        if success:
            print("\n" + "=" * 60)
            print("✓ Data generation completed successfully!")
            print("=" * 60)
            
            # Display summary statistics
            if generator.connect_database():
                try:
                    print("\nDatabase Summary:")
                    
                    summary_queries = [
                        ("Countries", "SELECT COUNT(*) FROM countries"),
                        ("Territories", "SELECT COUNT(*) FROM territories"),
                        ("Employees", "SELECT COUNT(*) FROM employees"),
                        ("Products", "SELECT COUNT(*) FROM products"),
                        ("Suppliers", "SELECT COUNT(*) FROM suppliers"),
                        ("Customers", "SELECT COUNT(*) FROM customers"),
                        ("Purchase Orders", "SELECT COUNT(*) FROM purchase_orders"),
                        ("Sales Orders", "SELECT COUNT(*) FROM orders"),
                        ("Inventory Records", "SELECT COUNT(*) FROM inventory"),
                        ("Exchange Rate Records", "SELECT COUNT(*) FROM exchange_rates"),
                    ]
                    
                    for label, query in summary_queries:
                        try:
                            generator.cursor.execute(query)
                            count = generator.cursor.fetchone()[0]
                            print(f"  {label:<20}: {count:>8,}")
                        except:
                            print(f"  {label:<20}: {'-':>8}")
                    
                    # Database size
                    try:
                        generator.cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()));")
                        db_size = generator.cursor.fetchone()[0]
                        print(f"  {'Database Size':<20}: {db_size:>8}")
                    except:
                        pass
                        
                finally:
                    generator.disconnect_database()
            
            print(f"\nNext steps:")
            print(f"  1. Run sample queries: psql -f sample_queries.sql")
            print(f"  2. Test connection: uv run test_connection.py")
            print(f"  3. Validate data: uv run generate_data.py --validate-only")
            
        else:
            print("\n" + "=" * 60)
            print("✗ Data generation failed!")
            print("=" * 60)
            print("Check the logs above for error details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()