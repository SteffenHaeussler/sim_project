import random
import numpy as np
from datetime import timedelta
from psycopg2.extras import execute_batch
from .base import BaseGenerator


class HRGenerator(BaseGenerator):
    """Generator for roles and employees."""
    
    def generate_hr_data(self):
        """Generate roles and employees with realistic hierarchy."""
        self.logger.info("Generating HR data...")
        
        # Reset HR-related sequences to start from 1
        try:
            self.cursor.execute("ALTER SEQUENCE roles_role_id_seq RESTART WITH 1;")
            self.cursor.execute("ALTER SEQUENCE employees_employee_id_seq RESTART WITH 1;")
            self.conn.commit()
            self.logger.debug("Reset HR-related sequences to start from 1")
        except Exception as e:
            self.logger.warning(f"Could not reset HR sequences: {e}")
        
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