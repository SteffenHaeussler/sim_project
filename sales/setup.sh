#!/bin/bash

# International Sales Database Setup Script
# This script sets up the complete environment for the sales database

set -e  # Exit on any error

echo "=========================================="
echo "International Sales Database Setup"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Check if .env file exists
if [ ! -f .env ]; then
    print_error ".env file not found!"
    echo "Please create a .env file with database configuration:"
    echo "DB_HOST=localhost"
    echo "DB_PORT=5432"
    echo "DB_NAME=sales"
    echo "DB_USER=postgres"
    echo "PGPASSWORD=your_password"
    exit 1
fi

print_status "Found .env configuration file"

# Source the .env file
source .env

# Test PostgreSQL connection
print_status "Checking PostgreSQL connection..."
if ! PGPASSWORD=$PGPASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "SELECT version();" > /dev/null 2>&1; then
    print_error "Cannot connect to PostgreSQL server"
    echo "Please ensure PostgreSQL is running and credentials are correct"
    exit 1
fi

print_success "PostgreSQL connection successful"

# Check uv installation
print_status "Checking uv installation..."

if ! command -v uv &> /dev/null; then
    print_error "uv is not installed"
    echo "Please install uv first:"
    echo "curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo "or visit: https://github.com/astral-sh/uv"
    exit 1
fi

UV_VERSION=$(uv --version 2>&1 | cut -d' ' -f2)
print_success "uv $UV_VERSION found"

# Check Python version
print_status "Checking Python installation..."
PYTHON_VERSION=$(uv python list 2>/dev/null | head -1 | awk '{print $1}' || python3 --version 2>&1 | cut -d' ' -f2)
print_success "Python $PYTHON_VERSION available"

# Install Python dependencies using uv
print_status "Installing Python dependencies with uv..."
uv sync

print_success "Dependencies installed"

# Make initialization script executable
chmod +x init_database.py
chmod +x generate_data.py

# Run database initialization
print_status "Initializing database..."

echo ""
echo "Choose initialization option:"
echo "1) Full initialization (create database + schema + reference data)"
echo "2) Schema only (create database + schema)"
echo "3) Test connection only"
echo "4) Drop and recreate everything"

read -p "Enter your choice (1-4): " choice

case $choice in
    1)
        print_status "Running full initialization..."
        uv run init_database.py --populate-ref --verbose
        ;;
    2)
        print_status "Running schema initialization..."
        uv run init_database.py --verbose
        ;;
    3)
        print_status "Testing connection..."
        uv run init_database.py --test-connection --verbose
        ;;
    4)
        print_warning "This will DROP the existing database!"
        read -p "Are you sure? (y/N): " confirm
        if [[ $confirm == [yY] || $confirm == [yY][eE][sS] ]]; then
            print_status "Dropping and recreating database..."
            uv run init_database.py --drop-existing --populate-ref --verbose
        else
            print_status "Operation cancelled"
            exit 0
        fi
        ;;
    *)
        print_error "Invalid choice"
        exit 1
        ;;
esac

if [ $? -eq 0 ]; then
    print_success "Database initialization completed!"
    echo ""
    echo "=========================================="
    echo -e "${GREEN}Setup Complete!${NC}"
    echo "=========================================="
    echo ""
    echo "Your sales database is ready to use."
    echo ""
    echo "Connection details:"
    echo "  Host: $DB_HOST"
    echo "  Port: $DB_PORT"
    echo "  Database: $DB_NAME"
    echo "  User: $DB_USER"
    echo ""
    echo "Next steps:"
    echo "  1. Generate sample data: uv run generate_data.py --preset medium"
    echo "  2. Connect with psql: PGPASSWORD=$PGPASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
    echo "  3. Test connection: uv run init_database.py --test-connection"
    echo "  4. Run sample queries: PGPASSWORD=$PGPASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f sample_queries.sql"
    echo ""
    echo "Data generation presets:"
    echo "  - Small:      uv run generate_data.py --preset small      (100 employees, 2K orders)"
    echo "  - Medium:     uv run generate_data.py --preset medium     (300 employees, 5K orders)"
    echo "  - Large:      uv run generate_data.py --preset large      (500 employees, 10K orders)"
    echo "  - Enterprise: uv run generate_data.py --preset enterprise (1K employees, 25K orders)"
    echo ""
    echo "Files created:"
    echo "  - schema.sql (database schema)"
    echo "  - init_database.py (initialization script)"
    echo "  - generate_data.py (data generation script)"
    echo "  - sample_queries.sql (example queries)"
    echo "  - pyproject.toml (Python project configuration)"
    echo ""
else
    print_error "Database initialization failed!"
    exit 1
fi