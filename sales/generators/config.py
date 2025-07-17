from dataclasses import dataclass
from datetime import date


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
    end_date: date = date(2025, 12, 31)
    
    # Database settings
    batch_size: int = 1000
    
    # Generation settings
    exchange_rate_days: int = 1461  # 4 years of daily rates (2022-2025)
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
                start_date=date(2023, 7, 1), end_date=date(2025, 12, 31)
            ),
            'medium': cls(
                countries=40, territories=150, employees=300,
                products=1000, suppliers=100, customers=500,
                purchase_orders=2500, sales_orders=5000,
                start_date=date(2023, 1, 1), end_date=date(2025, 12, 31)
            ),
            'large': cls(),  # Default values
            'enterprise': cls(
                countries=75, territories=300, employees=1000,
                products=5000, suppliers=500, customers=2000,
                purchase_orders=15000, sales_orders=25000,
                start_date=date(2020, 1, 1), end_date=date(2025, 12, 31)
            )
        }
        return presets.get(preset, cls())