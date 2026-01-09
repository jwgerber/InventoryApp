import sqlite3
from datetime import datetime
import os

DATABASE = 'food_cost.db'

def get_db():
    """Get database connection with row factory for dict-like access."""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize database schema."""
    conn = get_db()
    cursor = conn.cursor()

    # Inventory items table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory_items (
            id TEXT PRIMARY KEY,
            supplier TEXT DEFAULT '',
            item TEXT NOT NULL,
            unit TEXT DEFAULT '',
            cost REAL DEFAULT 0,
            count1 REAL DEFAULT 0,
            count2 REAL DEFAULT 0,
            count3 REAL DEFAULT 0,
            count4 REAL DEFAULT 0,
            is_custom INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
    ''')

    # Price items table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_items (
            id TEXT PRIMARY KEY,
            location TEXT DEFAULT '',
            supplier TEXT DEFAULT '',
            item TEXT NOT NULL,
            purchase_unit TEXT DEFAULT '',
            units_per_inv REAL DEFAULT 1,
            current_price REAL DEFAULT 0,
            per_unit_cost REAL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
    ''')

    # Price history table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            price_item_id TEXT,
            month TEXT,
            price REAL,
            FOREIGN KEY (price_item_id) REFERENCES price_items(id)
        )
    ''')

    # Inventory counts by month
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory_counts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inventory_item_id TEXT,
            month TEXT,
            count1 REAL DEFAULT 0,
            count2 REAL DEFAULT 0,
            count3 REAL DEFAULT 0,
            count4 REAL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (inventory_item_id) REFERENCES inventory_items(id),
            UNIQUE(inventory_item_id, month)
        )
    ''')

    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_inventory_item ON inventory_items(item)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_item ON price_items(item)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_history_item ON price_history(price_item_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_inventory_counts_month ON inventory_counts(month)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_inventory_counts_item ON inventory_counts(inventory_item_id)')

    conn.commit()
    conn.close()

# ==================== INVENTORY FUNCTIONS ====================

def get_all_inventory(month=None):
    """Get all inventory items with counts for specified month."""
    conn = get_db()
    cursor = conn.cursor()

    query = '''
        SELECT i.id, i.supplier, i.item, i.unit, i.cost, i.is_custom,
               i.created_at, i.updated_at,
               COALESCE(c.count1, 0) as count1,
               COALESCE(c.count2, 0) as count2,
               COALESCE(c.count3, 0) as count3,
               COALESCE(c.count4, 0) as count4
        FROM inventory_items i
        LEFT JOIN inventory_counts c ON i.id = c.inventory_item_id {}
        ORDER BY i.item
    '''.format('AND c.month = ?' if month else '')

    cursor.execute(query, (month,) if month else ())
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_inventory_item(item_id, month=None):
    """Get a single inventory item by ID with counts for specified month."""
    conn = get_db()
    cursor = conn.cursor()

    if month:
        query = '''
            SELECT i.id, i.supplier, i.item, i.unit, i.cost, i.is_custom,
                   i.created_at, i.updated_at,
                   COALESCE(c.count1, 0) as count1,
                   COALESCE(c.count2, 0) as count2,
                   COALESCE(c.count3, 0) as count3,
                   COALESCE(c.count4, 0) as count4
            FROM inventory_items i
            LEFT JOIN inventory_counts c ON i.id = c.inventory_item_id AND c.month = ?
            WHERE i.id = ?
        '''
        cursor.execute(query, (month, item_id))
    else:
        cursor.execute('SELECT * FROM inventory_items WHERE id = ?', (item_id,))

    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_inventory_months():
    """Get list of all months that have inventory counts."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT month FROM inventory_counts ORDER BY month DESC')
    rows = cursor.fetchall()
    conn.close()
    return [row['month'] for row in rows]

def update_inventory_item(item_id, data, month=None):
    """Update an inventory item and its counts for specific month."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    # Update base item info (supplier, item, unit, cost)
    cursor.execute('''
        UPDATE inventory_items
        SET supplier = ?, item = ?, unit = ?, cost = ?, updated_at = ?
        WHERE id = ?
    ''', (
        data.get('supplier', ''),
        data.get('item', ''),
        data.get('unit', ''),
        data.get('cost', 0),
        now,
        item_id
    ))

    # Update counts for specific month if provided
    if month:
        cursor.execute('''
            INSERT INTO inventory_counts (inventory_item_id, month, count1, count2, count3, count4, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(inventory_item_id, month) DO UPDATE SET
                count1 = excluded.count1,
                count2 = excluded.count2,
                count3 = excluded.count3,
                count4 = excluded.count4,
                updated_at = excluded.updated_at
        ''', (
            item_id,
            month,
            data.get('count1', 0),
            data.get('count2', 0),
            data.get('count3', 0),
            data.get('count4', 0),
            now,
            now
        ))

    conn.commit()
    conn.close()
    return get_inventory_item(item_id, month)

def add_inventory_item(data):
    """Add a new custom inventory item."""
    conn = get_db()
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    item_id = f"custom-{int(datetime.now().timestamp() * 1000)}"

    cursor.execute('''
        INSERT INTO inventory_items
        (id, supplier, item, unit, cost, count1, count2, count3, count4, is_custom, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, 1, ?, ?)
    ''', (
        item_id,
        data.get('supplier', ''),
        data.get('item', ''),
        data.get('unit', ''),
        data.get('cost', 0),
        now,
        now
    ))

    conn.commit()
    conn.close()
    return get_inventory_item(item_id)

def delete_inventory_item(item_id):
    """Delete a custom inventory item."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM inventory_items WHERE id = ? AND is_custom = 1', (item_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def clear_all_counts(month=None):
    """Reset all inventory counts to zero for a specific month."""
    conn = get_db()
    cursor = conn.cursor()
    if month:
        cursor.execute('DELETE FROM inventory_counts WHERE month = ?', (month,))
    conn.commit()
    conn.close()

# ==================== PRICE FUNCTIONS ====================

def get_all_prices():
    """Get all price items with their history."""
    conn = get_db()
    cursor = conn.cursor()

    # Get all price items
    cursor.execute('SELECT * FROM price_items ORDER BY item')
    items = [dict(row) for row in cursor.fetchall()]

    # Get history for each item
    for item in items:
        cursor.execute('''
            SELECT month, price FROM price_history
            WHERE price_item_id = ?
            ORDER BY id DESC
        ''', (item['id'],))
        item['priceHistory'] = {row['month']: row['price'] for row in cursor.fetchall()}

    conn.close()
    return items

def get_price_item(item_id):
    """Get a single price item with history."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM price_items WHERE id = ?', (item_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None

    item = dict(row)
    cursor.execute('''
        SELECT month, price FROM price_history
        WHERE price_item_id = ?
        ORDER BY id DESC
    ''', (item_id,))
    item['priceHistory'] = {row['month']: row['price'] for row in cursor.fetchall()}

    conn.close()
    return item

def update_price(item_id, month, price):
    """Update price for an item and add to history."""
    conn = get_db()
    cursor = conn.cursor()

    now = datetime.now().isoformat()

    # Get current item to calculate per_unit_cost
    cursor.execute('SELECT units_per_inv FROM price_items WHERE id = ?', (item_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None

    per_unit_cost = round(price / (row['units_per_inv'] or 1), 2)

    # Update price item
    cursor.execute('''
        UPDATE price_items
        SET current_price = ?, per_unit_cost = ?, updated_at = ?
        WHERE id = ?
    ''', (price, per_unit_cost, now, item_id))

    # Update or insert price history
    cursor.execute('SELECT id FROM price_history WHERE price_item_id = ? AND month = ?', (item_id, month))
    existing = cursor.fetchone()

    if existing:
        cursor.execute('UPDATE price_history SET price = ? WHERE id = ?', (price, existing['id']))
    else:
        cursor.execute('INSERT INTO price_history (price_item_id, month, price) VALUES (?, ?, ?)', (item_id, month, price))

    conn.commit()
    conn.close()
    return get_price_item(item_id)

def add_price_item(data):
    """Add a new price item."""
    conn = get_db()
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    item_id = f"price-{int(datetime.now().timestamp() * 1000)}"

    units_per_inv = data.get('units_per_inv', 1) or 1
    current_price = data.get('current_price', 0) or 0
    per_unit_cost = round(current_price / units_per_inv, 2)

    cursor.execute('''
        INSERT INTO price_items
        (id, location, supplier, item, purchase_unit, units_per_inv, current_price, per_unit_cost, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        item_id,
        data.get('location', ''),
        data.get('supplier', ''),
        data.get('item', ''),
        data.get('purchase_unit', ''),
        units_per_inv,
        current_price,
        per_unit_cost,
        now,
        now
    ))

    # Add initial price to history if price provided
    if current_price > 0:
        month = data.get('month')
        if month:
            cursor.execute('''
                INSERT INTO price_history (price_item_id, month, price)
                VALUES (?, ?, ?)
            ''', (item_id, month, current_price))

    conn.commit()
    conn.close()
    return get_price_item(item_id)

def sync_prices_to_inventory():
    """Sync per_unit_cost from prices to inventory cost."""
    conn = get_db()
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    updated = 0

    cursor.execute('SELECT item, per_unit_cost FROM price_items')
    price_items = cursor.fetchall()

    for price_item in price_items:
        item_lower = price_item['item'].lower()
        cursor.execute('''
            SELECT id, cost FROM inventory_items
            WHERE LOWER(item) LIKE ? OR ? LIKE '%' || LOWER(item) || '%'
        ''', (f'%{item_lower}%', item_lower))

        for inv_item in cursor.fetchall():
            if abs(inv_item['cost'] - price_item['per_unit_cost']) > 0.001:
                cursor.execute('UPDATE inventory_items SET cost = ?, updated_at = ? WHERE id = ?',
                             (price_item['per_unit_cost'], now, inv_item['id']))
                updated += 1

    conn.commit()
    conn.close()
    return updated

# ==================== BULK INSERT FUNCTIONS ====================

def bulk_insert_inventory(items):
    """Bulk insert inventory items."""
    conn = get_db()
    cursor = conn.cursor()

    now = datetime.now().isoformat()

    for item in items:
        cursor.execute('''
            INSERT OR REPLACE INTO inventory_items
            (id, supplier, item, unit, cost, count1, count2, count3, count4, is_custom, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('id'),
            item.get('supplier', ''),
            item.get('item', ''),
            item.get('unit', ''),
            item.get('cost', 0),
            item.get('count1', 0),
            item.get('count2', 0),
            item.get('count3', 0),
            item.get('count4', 0),
            item.get('is_custom', 0),
            now,
            now
        ))

    conn.commit()
    conn.close()

def bulk_insert_prices(items):
    """Bulk insert price items with history."""
    conn = get_db()
    cursor = conn.cursor()

    now = datetime.now().isoformat()

    for item in items:
        cursor.execute('''
            INSERT OR REPLACE INTO price_items
            (id, location, supplier, item, purchase_unit, units_per_inv, current_price, per_unit_cost, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('id'),
            item.get('location', ''),
            item.get('supplier', ''),
            item.get('item', ''),
            item.get('purchaseUnit', ''),
            item.get('unitsPerInv', 1),
            item.get('currentPrice', 0),
            item.get('perUnitCost', 0),
            now,
            now
        ))

        # Insert price history
        price_history = item.get('priceHistory', {})
        for month, price in price_history.items():
            cursor.execute('''
                INSERT INTO price_history (price_item_id, month, price)
                VALUES (?, ?, ?)
            ''', (item.get('id'), month, price))

    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    print("Database initialized successfully!")
