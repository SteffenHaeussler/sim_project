import random
from psycopg2.extras import execute_batch
from .base import BaseGenerator


class GeographicGenerator(BaseGenerator):
    """Generator for countries, territories, and addresses."""
    
    def generate_geographic_data(self):
        """Generate countries, territories, and addresses."""
        self.logger.info("Generating geographic data...")
        
        # Reset sequences to start from 1
        try:
            self.cursor.execute("ALTER SEQUENCE countries_country_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE territories_territory_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE addresses_address_id_seq RESTART WITH 1;")
            self.conn.commit()
            self.logger.debug("Reset geographic sequences to start from 1")
        except Exception as e:
            self.logger.warning(f"Could not reset geographic sequences: {e}")
        
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