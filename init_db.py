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

    # Find the array assignment
    pattern = rf'const\s+{var_name}\s*=\s*(\[[\s\S]*?\]);'
    match = re.search(pattern, content)
    if not match:
        raise ValueError(f"Could not find {var_name} in {filepath}")

    # Parse the JSON array
    json_str = match.group(1)
    return json.loads(json_str)

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Initialize database schema
    print("Initializing database schema...")
    init_db()

    # Load and insert inventory items
    data_js = os.path.join(script_dir, 'data.js')
    if os.path.exists(data_js):
        print("Loading inventory items from data.js...")
        inventory_items = parse_js_array(data_js, 'MASTER_ITEMS')
        print(f"  Found {len(inventory_items)} inventory items")
        bulk_insert_inventory(inventory_items)
        print("  Inserted into database")
    else:
        print("Warning: data.js not found")

    # Load and insert price items
    prices_js = os.path.join(script_dir, 'prices.js')
    if os.path.exists(prices_js):
        print("Loading price items from prices.js...")
        price_items = parse_js_array(prices_js, 'PRICE_DATABASE')
        print(f"  Found {len(price_items)} price items")
        bulk_insert_prices(price_items)
        print("  Inserted into database")
    else:
        print("Warning: prices.js not found")

    print("\nDatabase initialization complete!")
    print(f"Database file: {os.path.join(script_dir, 'food_cost.db')}")

if __name__ == '__main__':
    main()
