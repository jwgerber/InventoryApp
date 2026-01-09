#!/usr/bin/env python3
"""Flask server for Food Cost Database."""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import database as db
import os

app = Flask(__name__, static_folder='.')
CORS(app)

# ==================== STATIC FILES ====================

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory('.', filename)

# ==================== INVENTORY API ====================

@app.route('/api/inventory/months', methods=['GET'])
def get_inventory_months():
    """Get list of months with inventory data."""
    months = db.get_inventory_months()
    return jsonify(months)

@app.route('/api/inventory', methods=['GET'])
def get_inventory():
    """Get all inventory items with counts for specified month and store."""
    month = request.args.get('month')
    store = request.args.get('store')
    items = db.get_all_inventory(month, store)
    return jsonify(items)

@app.route('/api/inventory/<item_id>', methods=['GET'])
def get_inventory_item(item_id):
    """Get a single inventory item."""
    month = request.args.get('month')
    store = request.args.get('store')
    item = db.get_inventory_item(item_id, month, store)
    return jsonify(item) if item else (jsonify({'error': 'Item not found'}), 404)

@app.route('/api/inventory/<item_id>', methods=['PUT'])
def update_inventory_item(item_id):
    """Update an inventory item."""
    data = request.get_json()
    month = data.get('month')
    store = data.get('store')  # Store for count tracking (Inman, Central, etc.)
    item = db.update_inventory_item(item_id, data, month, store)
    return jsonify(item) if item else (jsonify({'error': 'Item not found'}), 404)

@app.route('/api/inventory', methods=['POST'])
def add_inventory_item():
    """Add a new custom inventory item."""
    data = request.get_json()
    if not data.get('item'):
        return jsonify({'error': 'Item name is required'}), 400
    item = db.add_inventory_item(data)
    return jsonify(item), 201

@app.route('/api/inventory/<item_id>', methods=['DELETE'])
def delete_inventory_item(item_id):
    """Delete a custom inventory item."""
    if db.delete_inventory_item(item_id):
        return jsonify({'success': True})
    return jsonify({'error': 'Item not found or not deletable'}), 404

@app.route('/api/inventory/clear-counts', methods=['POST'])
def clear_counts():
    """Clear all inventory counts for a specific month and/or store."""
    data = request.get_json() or {}
    month = data.get('month')
    store = data.get('store')
    db.clear_all_counts(month, store)
    return jsonify({'success': True})

# ==================== PRICE API ====================

@app.route('/api/prices', methods=['GET'])
def get_prices():
    """Get all price items with history."""
    items = db.get_all_prices()
    return jsonify(items)

@app.route('/api/prices/<item_id>', methods=['GET'])
def get_price_item(item_id):
    """Get a single price item with history."""
    item = db.get_price_item(item_id)
    return jsonify(item) if item else (jsonify({'error': 'Item not found'}), 404)

@app.route('/api/prices', methods=['POST'])
def add_price_item():
    """Add a new price item."""
    data = request.get_json()
    if not data.get('item'):
        return jsonify({'error': 'Item name is required'}), 400
    item = db.add_price_item(data)
    return jsonify(item), 201

@app.route('/api/prices/<item_id>', methods=['PUT'])
def update_price(item_id):
    """Update price for an item."""
    data = request.get_json()
    month = data.get('month')
    price = data.get('price')

    if not month or price is None:
        return jsonify({'error': 'Month and price are required'}), 400

    item = db.update_price(item_id, month, float(price))
    return jsonify(item) if item else (jsonify({'error': 'Item not found'}), 404)

@app.route('/api/prices/<item_id>/edit', methods=['PUT'])
def edit_price_item(item_id):
    """Edit all fields of a price item."""
    data = request.get_json()
    if not data.get('item'):
        return jsonify({'error': 'Item name is required'}), 400

    item = db.update_price_item(item_id, {
        'item': data.get('item'),
        'locations': data.get('locations', []),
        'supplier': data.get('supplier', ''),
        'purchase_unit': data.get('purchase_unit', ''),
        'units_per_inv': float(data.get('units_per_inv', 1)),
        'current_price': float(data.get('current_price', 0))
    })

    # Also update price history if price and month provided
    if data.get('month') and data.get('current_price') is not None:
        db.update_price(item_id, data['month'], float(data['current_price']))

    return jsonify(item) if item else (jsonify({'error': 'Item not found'}), 404)

@app.route('/api/prices/<item_id>', methods=['DELETE'])
def delete_price_item(item_id):
    """Delete a price item."""
    if db.delete_price_item(item_id):
        return jsonify({'success': True})
    return jsonify({'error': 'Item not found'}), 404

@app.route('/api/prices/sync', methods=['POST'])
def sync_prices():
    """Sync prices to inventory costs."""
    updated = db.sync_prices_to_inventory()
    return jsonify({'updated': updated})

# ==================== SUPPLIER API ====================

@app.route('/api/suppliers', methods=['GET'])
def get_suppliers():
    """Get all suppliers."""
    suppliers = db.get_all_suppliers()
    return jsonify(suppliers)

@app.route('/api/suppliers', methods=['POST'])
def add_supplier():
    """Add a new supplier."""
    data = request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'Supplier name is required'}), 400
    supplier = db.add_supplier(name)
    if supplier:
        return jsonify(supplier), 201
    return jsonify({'error': 'Supplier already exists'}), 409

@app.route('/api/suppliers/<int:supplier_id>', methods=['PUT'])
def update_supplier(supplier_id):
    """Update a supplier."""
    data = request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'Supplier name is required'}), 400
    supplier = db.update_supplier(supplier_id, name)
    if supplier:
        return jsonify(supplier)
    return jsonify({'error': 'Supplier not found or name already exists'}), 404

@app.route('/api/suppliers/<int:supplier_id>', methods=['DELETE'])
def delete_supplier(supplier_id):
    """Delete a supplier."""
    if db.delete_supplier(supplier_id):
        return jsonify({'success': True})
    return jsonify({'error': 'Supplier not found'}), 404

# ==================== LOCATION API ====================

@app.route('/api/locations', methods=['GET'])
def get_locations():
    """Get all locations."""
    locations = db.get_all_locations()
    return jsonify(locations)

@app.route('/api/locations', methods=['POST'])
def add_location():
    """Add a new location."""
    data = request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'Location name is required'}), 400
    location = db.add_location(name)
    if location:
        return jsonify(location), 201
    return jsonify({'error': 'Location already exists'}), 409

@app.route('/api/locations/<int:location_id>', methods=['PUT'])
def update_location(location_id):
    """Update a location."""
    data = request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'Location name is required'}), 400
    location = db.update_location(location_id, name)
    if location:
        return jsonify(location)
    return jsonify({'error': 'Location not found or name already exists'}), 404

@app.route('/api/locations/<int:location_id>', methods=['DELETE'])
def delete_location(location_id):
    """Delete a location."""
    if db.delete_location(location_id):
        return jsonify({'success': True})
    return jsonify({'error': 'Location not found'}), 404

# ==================== STORE API ====================

@app.route('/api/stores', methods=['GET'])
def get_stores():
    """Get all stores (physical locations like Inman, Central)."""
    stores = db.get_all_stores()
    return jsonify(stores)

@app.route('/api/stores', methods=['POST'])
def add_store():
    """Add a new store."""
    data = request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'Store name is required'}), 400
    store = db.add_store(name)
    if store:
        return jsonify(store), 201
    return jsonify({'error': 'Store already exists'}), 409

@app.route('/api/stores/<int:store_id>', methods=['PUT'])
def update_store(store_id):
    """Update a store."""
    data = request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'Store name is required'}), 400
    store = db.update_store(store_id, name)
    if store:
        return jsonify(store)
    return jsonify({'error': 'Store not found or name already exists'}), 404

@app.route('/api/stores/<int:store_id>', methods=['DELETE'])
def delete_store(store_id):
    """Delete a store."""
    if db.delete_store(store_id):
        return jsonify({'success': True})
    return jsonify({'error': 'Store not found'}), 404

# ==================== EXPORT API ====================

@app.route('/api/export/inventory', methods=['GET'])
def export_inventory():
    """Export inventory as CSV for specified month with supplier subtotals."""
    from collections import defaultdict

    month = request.args.get('month')
    items = db.get_all_inventory(month)

    # Filter to only items with counts
    items = [i for i in items if any([i['count1'], i['count2'], i['count3'], i['count4']])]

    if not items:
        return jsonify({'error': 'No items with counts'}), 400

    # Group items by supplier
    by_supplier = defaultdict(list)
    for i in items:
        by_supplier[i['supplier'] or 'No Supplier'].append(i)

    lines = ['Supplier,Item,Unit,Cost,Count1,Count2,Count3,Count4,Total,Extended']
    grand_total = 0

    for supplier in sorted(by_supplier.keys()):
        supplier_items = by_supplier[supplier]
        supplier_total = 0

        for i in supplier_items:
            total = sum([i['count1'] or 0, i['count2'] or 0, i['count3'] or 0, i['count4'] or 0])
            extended = total * (i['cost'] or 0)
            supplier_total += extended

            lines.append(','.join([
                escape_csv(i['supplier']),
                escape_csv(i['item']),
                escape_csv(i['unit']),
                f"{i['cost']:.2f}",
                str(i['count1'] or 0),
                str(i['count2'] or 0),
                str(i['count3'] or 0),
                str(i['count4'] or 0),
                str(total),
                f"{extended:.2f}"
            ]))

        lines.append(f'{escape_csv(supplier)} TOTAL,,,,,,,,,{supplier_total:.2f}')
        lines.append('')
        grand_total += supplier_total

    lines.append(f'GRAND TOTAL,,,,,,,,,{grand_total:.2f}')

    filename = f'inventory-{month}.csv' if month else 'inventory.csv'
    return '\n'.join(lines), 200, {
        'Content-Type': 'text/csv',
        'Content-Disposition': f'attachment; filename={filename}'
    }

@app.route('/api/export/prices', methods=['GET'])
def export_prices():
    """Export prices as CSV for editing. Includes ID for re-import. Locations are semicolon-separated."""
    items = db.get_all_prices()

    lines = ['ID,Locations,Supplier,Item,PurchaseUnit,UnitsPerInv,CurrentPrice']
    for i in items:
        # Join multiple locations with semicolon
        locations_str = '; '.join(i.get('locations', []))
        lines.append(','.join([
            escape_csv(i['id']),
            escape_csv(locations_str),
            escape_csv(i['supplier']),
            escape_csv(i['item']),
            escape_csv(i['purchase_unit']),
            str(i['units_per_inv']),
            f"{i['current_price']:.2f}"
        ]))

    csv_content = '\n'.join(lines)
    return csv_content, 200, {
        'Content-Type': 'text/csv',
        'Content-Disposition': 'attachment; filename=prices-export.csv'
    }

@app.route('/api/import/prices', methods=['POST'])
def import_prices():
    """Import prices from CSV. Updates existing items by ID, creates new ones if ID is empty.
    Locations should be semicolon-separated for multiple locations."""
    import csv
    import io

    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400

    try:
        content = file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))

        updated = 0
        created = 0
        errors = []

        for row_num, row in enumerate(reader, start=2):
            try:
                item_id = row.get('ID', '').strip()
                # Support both 'Locations' (new) and 'Location' (old) column names
                locations_str = row.get('Locations', row.get('Location', '')).strip()
                # Parse semicolon or comma separated locations
                locations = [loc.strip() for loc in locations_str.replace(',', ';').split(';') if loc.strip()]
                supplier = row.get('Supplier', '').strip()
                item_name = row.get('Item', '').strip()
                purchase_unit = row.get('PurchaseUnit', '').strip()
                units_per_inv = float(row.get('UnitsPerInv', 1) or 1)
                current_price = float(row.get('CurrentPrice', 0) or 0)

                if not item_name:
                    errors.append(f"Row {row_num}: Item name is required")
                    continue

                if item_id:
                    # Update existing item
                    result = db.update_price_item(item_id, {
                        'locations': locations,
                        'supplier': supplier,
                        'item': item_name,
                        'purchase_unit': purchase_unit,
                        'units_per_inv': units_per_inv,
                        'current_price': current_price
                    })
                    if result:
                        updated += 1
                    else:
                        errors.append(f"Row {row_num}: Item ID '{item_id}' not found")
                else:
                    # Create new item
                    db.add_price_item({
                        'locations': locations,
                        'supplier': supplier,
                        'item': item_name,
                        'purchase_unit': purchase_unit,
                        'units_per_inv': units_per_inv,
                        'current_price': current_price
                    })
                    created += 1

            except ValueError as e:
                errors.append(f"Row {row_num}: Invalid number format")
            except Exception as e:
                errors.append(f"Row {row_num}: {str(e)}")

        return jsonify({
            'updated': updated,
            'created': created,
            'errors': errors
        })

    except Exception as e:
        return jsonify({'error': f'Failed to parse CSV: {str(e)}'}), 400

def escape_csv(s):
    """Escape a string for CSV."""
    s = str(s) if s else ''
    needs_escape = any(c in s for c in ',"\n')
    return f'"{s.replace(chr(34), chr(34)*2)}"' if needs_escape else s

# ==================== MAIN ====================

if __name__ == '__main__':
    # Initialize database if it doesn't exist
    if not os.path.exists('food_cost.db'):
        print("Database not found. Run 'python3 init_db.py' first.")
        exit(1)

    print("Starting Food Cost Database server...")
    print("Open http://localhost:8000 in your browser")
    app.run(debug=True, port=8000)
