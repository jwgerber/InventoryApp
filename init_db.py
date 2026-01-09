#!/usr/bin/env python3
"""Initialize the database with data from the Excel file."""

import json
import os
import re
from database import init_db, bulk_insert_inventory, bulk_insert_prices

def parse_js_array(filepath, var_name):
    """Parse a JavaScript file and extract an array variable."""
    with open(filepath, 'r') as f:
        content = f.read()

    pattern = rf'const\s+{var_name}\s*=\s*(\[[\s\S]*?\]);'
    match = re.search(pattern, content)
    if not match:
        raise ValueError(f"Could not find {var_name} in {filepath}")

    return json.loads(match.group(1))

def load_js_file(script_dir, filename, var_name, bulk_insert_func):
    """Load and insert data from a JavaScript file."""
    filepath = os.path.join(script_dir, filename)
    if not os.path.exists(filepath):
        print(f"Warning: {filename} not found")
        return

    print(f"Loading items from {filename}...")
    items = parse_js_array(filepath, var_name)
    print(f"  Found {len(items)} items")
    bulk_insert_func(items)
    print("  Inserted into database")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    print("Initializing database schema...")
    init_db()

    load_js_file(script_dir, 'data.js', 'MASTER_ITEMS', bulk_insert_inventory)
    load_js_file(script_dir, 'prices.js', 'PRICE_DATABASE', bulk_insert_prices)

    print("\nDatabase initialization complete!")
    print(f"Database file: {os.path.join(script_dir, 'food_cost.db')}")

if __name__ == '__main__':
    main()
