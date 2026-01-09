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
    """Get all inventory items with counts for specified month."""
    month = request.args.get('month')
    items = db.get_all_inventory(month)
    return jsonify(items)

@app.route('/api/inventory/<item_id>', methods=['GET'])
def get_inventory_item(item_id):
    """Get a single inventory item."""
    month = request.args.get('month')
    item = db.get_inventory_item(item_id, month)
    if item:
        return jsonify(item)
    return jsonify({'error': 'Item not found'}), 404

@app.route('/api/inventory/<item_id>', methods=['PUT'])
def update_inventory_item(item_id):
    """Update an inventory item."""
    data = request.get_json()
    month = data.get('month')
    item = db.update_inventory_item(item_id, data, month)
    if item:
        return jsonify(item)
    return jsonify({'error': 'Item not found'}), 404

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
    """Clear all inventory counts for a specific month."""
    data = request.get_json() or {}
    month = data.get('month')
    db.clear_all_counts(month)
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
    if item:
        return jsonify(item)
    return jsonify({'error': 'Item not found'}), 404

@app.route('/api/prices/<item_id>', methods=['PUT'])
def update_price(item_id):
    """Update price for an item."""
    data = request.get_json()
    month = data.get('month')
    price = data.get('price')

    if not month or price is None:
        return jsonify({'error': 'Month and price are required'}), 400

    item = db.update_price(item_id, month, float(price))
    if item:
        return jsonify(item)
    return jsonify({'error': 'Item not found'}), 404

@app.route('/api/prices/sync', methods=['POST'])
def sync_prices():
    """Sync prices to inventory costs."""
    updated = db.sync_prices_to_inventory()
    return jsonify({'updated': updated})

# ==================== EXPORT API ====================

@app.route('/api/export/inventory', methods=['GET'])
def export_inventory():
    """Export inventory as CSV for specified month."""
    month = request.args.get('month')
    items = db.get_all_inventory(month)

    # Filter to only items with counts
    items = [i for i in items if (i['count1'] or i['count2'] or i['count3'] or i['count4'])]

    if not items:
        return jsonify({'error': 'No items with counts'}), 400

    lines = ['Supplier,Item,Unit,Cost,Count1,Count2,Count3,Count4,Total,Extended']
    for i in items:
        total = (i['count1'] or 0) + (i['count2'] or 0) + (i['count3'] or 0) + (i['count4'] or 0)
        extended = total * (i['cost'] or 0)
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

    csv_content = '\n'.join(lines)
    filename = f'inventory-{month}.csv' if month else 'inventory.csv'
    return csv_content, 200, {
        'Content-Type': 'text/csv',
        'Content-Disposition': f'attachment; filename={filename}'
    }

@app.route('/api/export/prices', methods=['GET'])
def export_prices():
    """Export prices as CSV."""
    items = db.get_all_prices()

    lines = ['Location,Supplier,Item,PurchaseUnit,UnitsPerInv,CurrentPrice,PerUnitCost']
    for i in items:
        lines.append(','.join([
            escape_csv(i['location']),
            escape_csv(i['supplier']),
            escape_csv(i['item']),
            escape_csv(i['purchase_unit']),
            str(i['units_per_inv']),
            f"{i['current_price']:.2f}",
            f"{i['per_unit_cost']:.2f}"
        ]))

    csv_content = '\n'.join(lines)
    return csv_content, 200, {
        'Content-Type': 'text/csv',
        'Content-Disposition': 'attachment; filename=prices.csv'
    }

def escape_csv(s):
    """Escape a string for CSV."""
    s = str(s) if s else ''
    if ',' in s or '"' in s or '\n' in s:
        return '"' + s.replace('"', '""') + '"'
    return s

# ==================== MAIN ====================

if __name__ == '__main__':
    # Initialize database if it doesn't exist
    if not os.path.exists('food_cost.db'):
        print("Database not found. Run 'python3 init_db.py' first.")
        exit(1)

    print("Starting Food Cost Database server...")
    print("Open http://localhost:8000 in your browser")
    app.run(debug=True, port=8000)
