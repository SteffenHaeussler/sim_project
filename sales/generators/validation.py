from .base import BaseGenerator


class ValidationGenerator(BaseGenerator):
    """Generator for data validation and integrity checks."""
    
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