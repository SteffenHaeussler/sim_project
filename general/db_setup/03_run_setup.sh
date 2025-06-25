#!/bin/bash

# Database setup script for industrial process monitoring application
# This script initializes the PostgreSQL database with tables and seed data

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Database connection parameters
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-organisation}
DB_USER=${DB_USER:-postgres}

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
        print_status "Make sure your PostgreSQL container is running: docker compose up -d db"
        exit 1
    fi
}

# Create database if it doesn't exist
create_database() {
    print_status "Creating database '$DB_NAME' if it doesn't exist..."

    # Check if database exists
    DB_EXISTS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -tAc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" 2>/dev/null)

    if [ "$DB_EXISTS" = "1" ]; then
        print_warning "Database '$DB_NAME' already exists"
    else
        createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME
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

    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$script_file"

    if [ $? -eq 0 ]; then
        print_success "$description completed"
    else
        print_error "$description failed"
        exit 1
    fi
}

# Backup existing database
backup_database() {
    if [ "$1" = "--backup" ]; then
        print_status "Creating backup of existing database..."
        local backup_file="backup_${DB_NAME}_$(date +%Y%m%d_%H%M%S).sql"

        pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME > "$backup_file" 2>/dev/null

        if [ $? -eq 0 ]; then
            print_success "Database backed up to: $backup_file"
        else
            print_warning "Failed to create backup (database might not exist yet)"
        fi
    fi
}

# Check for required environment variables
check_environment() {
    print_status "Checking environment variables..."

    if [ -z "$PGPASSWORD" ]; then
        print_warning "PGPASSWORD environment variable not set"
        echo -n "Enter PostgreSQL password for user '$DB_USER': "
        read -s password
        echo
        export PGPASSWORD=$password
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
    echo "  Database Setup for Industrial Process"
    echo "  Monitoring Authentication System"
    echo "=============================================="
    echo

    # Parse command line arguments
    BACKUP_OPTION=""
    FORCE_OPTION=""

    while [[ $# -gt 0 ]]; do
        case $1 in
            --backup)
                BACKUP_OPTION="--backup"
                shift
                ;;
            --force)
                FORCE_OPTION="--force"
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --backup    Create a backup before running setup"
                echo "  --force     Skip confirmation prompts"
                echo "  --help      Show this help message"
                echo ""
                echo "Environment variables:"
                echo "  DB_HOST     Database host (default: localhost)"
                echo "  DB_PORT     Database port (default: 5432)"
                echo "  DB_NAME     Database name (default: sim_frontend_db)"
                echo "  DB_USER     Database user (default: postgres)"
                echo "  PGPASSWORD  PostgreSQL password"
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

    # Confirmation prompt
    if [ "$FORCE_OPTION" != "--force" ]; then
        echo
        print_warning "This will create/modify the database structure and add seed data."
        echo -n "Continue? (y/N): "
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            print_status "Setup cancelled by user"
            exit 0
        fi
    fi

    echo
    backup_database $BACKUP_OPTION
    create_database

    echo
    run_sql_script "$SCRIPT_DIR/01_init_database.sql" "Creating database tables and functions"

    echo
    run_sql_script "$SCRIPT_DIR/02_seed_data.sql" "Inserting seed data and demo users"

    echo
    print_success "Database setup completed successfully!"
    echo
    print_status "Demo user accounts created:"
    echo "  admin@demo.local     (password: admin123!)     - System Administrator"
    echo "  manager@demo.local   (password: manager123!)   - Manager"
    echo "  engineer@demo.local  (password: engineer123!)  - Process Engineer"
    echo "  operator@demo.local  (password: operator123!)  - Plant Operator"
    echo
    print_warning "Remember to change these default passwords in production!"
    echo
}

# Run main function
main "$@"
