import random
import logging
from typing import Dict, List, Any, Optional, Tuple
from faker import Faker
from faker.providers import BaseProvider
from datetime import datetime
from psycopg2.extras import execute_batch

from .config import GenerationConfig


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


class BaseGenerator:
    """Base class for all data generators."""
    
    def __init__(self, config: GenerationConfig, conn, cursor, cache: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.conn = conn
        self.cursor = cursor
        self.cache = cache
        self.logger = logger
        
        # Set up faker instances
        self.fake_us = Faker('en_US')
        self.fake_uk = Faker('en_GB')
        self.fake_de = Faker('de_DE')
        self.fake_fr = Faker('fr_FR')
        self.fake_jp = Faker('ja_JP')
        self.fake_cn = Faker('zh_CN')
        
        # Add custom provider to all fakers
        for fake in [self.fake_us, self.fake_uk, self.fake_de, self.fake_fr, self.fake_jp, self.fake_cn]:
            fake.add_provider(BusinessProvider)
    
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