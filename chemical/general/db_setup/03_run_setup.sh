#!/bin/bash

# Complete database setup script for industrial process monitoring application
# This script handles both fresh database initialization and migration of existing databases

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Database connection parameters (get from .env file)
DB_HOST=${auth_host:-localhost}
DB_PORT=${auth_port:-5432}
DB_NAME=${auth_db:-organisation}
DB_USER=${auth_user:-postgres}
DB_PASSWORD=${auth_password:-example}

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if PostgreSQL is running
check_postgres() {
    print_status "Checking PostgreSQL connection..."

    if pg_isready -h $DB_HOST -p $DB_PORT > /dev/null 2>&1; then
        print_success "PostgreSQL is running and accepting connections"
    else
        print_error "PostgreSQL is not running or not accessible at $DB_HOST:$DB_PORT"
        print_status "Make sure your PostgreSQL server is running"
        exit 1
    fi
}

# Create database if it doesn't exist
create_database() {
    print_status "Creating database '$DB_NAME' if it doesn't exist..."

    # Check if database exists
    DB_EXISTS=$(PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -tAc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" 2>/dev/null)

    if [ "$DB_EXISTS" = "1" ]; then
        print_warning "Database '$DB_NAME' already exists"
    else
        PGPASSWORD=$DB_PASSWORD createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME
        if [ $? -eq 0 ]; then
            print_success "Database '$DB_NAME' created successfully"
        else
            print_error "Failed to create database '$DB_NAME'"
            exit 1
        fi
    fi
}

# Run SQL script
run_sql_script() {
    local script_file=$1
    local description=$2

    print_status "$description"

    if [ ! -f "$script_file" ]; then
        print_error "Script file '$script_file' not found"
        exit 1
    fi

    PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$script_file"

    if [ $? -eq 0 ]; then
        print_success "$description completed"
    else
        print_error "$description failed"
        exit 1
    fi
}

# Check if this is a fresh database or needs migration
check_database_type() {
    print_status "Checking database state..."
    
    # Check if users table exists to determine if this is a fresh database
    USERS_TABLE_EXISTS=$(PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -tAc "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'users');" 2>/dev/null)
    
    if [ "$USERS_TABLE_EXISTS" = "t" ]; then
        print_warning "Existing database detected - will run migration script"
        DATABASE_TYPE="migration"
    else
        print_status "Fresh database detected - will run full initialization"
        DATABASE_TYPE="fresh"
    fi
}

# Check for required environment variables
check_environment() {
    print_status "Checking environment variables..."

    if [ -z "$DB_PASSWORD" ]; then
        print_error "Database password not found in environment"
        print_status "Make sure auth_password is set in your .env file"
        exit 1
    fi

    print_status "Using database connection:"
    echo "  Host: $DB_HOST"
    echo "  Port: $DB_PORT"
    echo "  Database: $DB_NAME"
    echo "  User: $DB_USER"
}

# Main execution
main() {
    echo "=============================================="
    echo "  Simplified Database Setup for"
    echo "  Industrial Process Monitoring"
    echo "=============================================="
    echo

    # Parse command line arguments
    FORCE_OPTION=""

    while [[ $# -gt 0 ]]; do
        case $1 in
            --force)
                FORCE_OPTION="--force"
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "This script automatically detects if you have a fresh or existing database:"
                echo "  - Fresh database: Runs full initialization + seed data"
                echo "  - Existing database: Runs migration script safely"
                echo ""
                echo "Options:"
                echo "  --force     Skip confirmation prompts"
                echo "  --help      Show this help message"
                echo ""
                echo "Environment variables (from .env file):"
                echo "  auth_host     Database host"
                echo "  auth_port     Database port"
                echo "  auth_db       Database name"
                echo "  auth_user     Database user"
                echo "  auth_password Database password"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done

    # Get script directory
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

    check_environment
    check_postgres

    echo
    create_database
    
    echo
    check_database_type

    # Confirmation prompt
    if [ "$FORCE_OPTION" != "--force" ]; then
        echo
        if [ "$DATABASE_TYPE" = "fresh" ]; then
            print_warning "This will create complete database structure and add sample data."
        else
            print_warning "This will migrate your existing database to add new tracking features."
        fi
        echo -n "Continue? (y/N): "
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            print_status "Setup cancelled by user"
            exit 0
        fi
    fi

    echo
    if [ "$DATABASE_TYPE" = "fresh" ]; then
        # Fresh database - run full initialization
        run_sql_script "$SCRIPT_DIR/01_database_init_complete.sql" "Creating complete database structure"
        
        echo
        run_sql_script "$SCRIPT_DIR/02_seed_data_complete.sql" "Inserting sample data and test users"
        
        echo
        print_success "Fresh database setup completed successfully!"
        echo
        print_status "Test user accounts created:"
        echo "  admin@company.com (password: admin123) - Admin User"
        echo "  user@company.com (password: user123) - Test User"
        echo "  Organisation: Default Organisation (max 100 users)"
        echo
        print_warning "Remember to change the default passwords in production!"
    else
        # Existing database - run migration
        run_sql_script "$SCRIPT_DIR/03_database_migration.sql" "Migrating existing database structure"
        
        echo
        print_success "Database migration completed successfully!"
        echo
        print_status "Your existing data has been preserved."
        print_status "New tracking and rating features have been added."
    fi
    echo
}

# Run main function
main "$@"
