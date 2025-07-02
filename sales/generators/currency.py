import numpy as np
from datetime import timedelta
from psycopg2.extras import execute_batch
from .base import BaseGenerator


class CurrencyGenerator(BaseGenerator):
    """Generator for currencies and exchange rates."""
    
    def generate_currency_data(self):
        """Generate currencies and exchange rates."""
        self.logger.info("Generating currency and exchange rate data...")
        
        # Reset exchange rates sequence to start from 1
        try:
            self.cursor.execute("ALTER SEQUENCE exchange_rates_exchange_rate_id_seq RESTART WITH 1;")
            self.conn.commit()
            self.logger.debug("Reset exchange_rate_id sequence to start from 1")
        except Exception as e:
            self.logger.warning(f"Could not reset exchange rates sequence: {e}")
        
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