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
            location TEXT DEFAULT '',
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
            store TEXT DEFAULT '',
            month TEXT,
            count1 REAL DEFAULT 0,
            count2 REAL DEFAULT 0,
            count3 REAL DEFAULT 0,
            count4 REAL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (inventory_item_id) REFERENCES inventory_items(id),
            UNIQUE(inventory_item_id, store, month)
        )
    ''')

    # Suppliers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT
        )
    ''')

    # Stores table (physical store locations like Inman, Central)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT
        )
    ''')

    # Locations table (item storage locations within a store)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT
        )
    ''')

    # Price item locations junction table (many-to-many)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_item_locations (
            price_item_id TEXT,
            location_id INTEGER,
            PRIMARY KEY (price_item_id, location_id),
            FOREIGN KEY (price_item_id) REFERENCES price_items(id) ON DELETE CASCADE,
            FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE CASCADE
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

    # Migrate existing suppliers/locations to new tables
    migrate_suppliers_locations()
    # Add location column to inventory if needed
    migrate_inventory_location()
    # Add store column to inventory_counts for multi-store support
    migrate_inventory_counts_store()
    # Add cost column to inventory_counts for historical price tracking
    migrate_inventory_counts_cost()
    # Add archived column to price_items and inventory_items
    migrate_archived_column()

def migrate_suppliers_locations():
    """Extract existing suppliers/locations from items and add to new tables."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    # Get unique suppliers from inventory_items
    cursor.execute('SELECT DISTINCT supplier FROM inventory_items WHERE supplier != ""')
    for row in cursor.fetchall():
        try:
            cursor.execute('INSERT OR IGNORE INTO suppliers (name, created_at) VALUES (?, ?)',
                         (row['supplier'], now))
        except:
            pass

    # Get unique suppliers from price_items
    cursor.execute('SELECT DISTINCT supplier FROM price_items WHERE supplier != ""')
    for row in cursor.fetchall():
        try:
            cursor.execute('INSERT OR IGNORE INTO suppliers (name, created_at) VALUES (?, ?)',
                         (row['supplier'], now))
        except:
            pass

    # Get unique locations from price_items
    cursor.execute('SELECT DISTINCT location FROM price_items WHERE location != ""')
    for row in cursor.fetchall():
        try:
            cursor.execute('INSERT OR IGNORE INTO locations (name, created_at) VALUES (?, ?)',
                         (row['location'], now))
        except:
            pass

    # Migrate existing single locations to junction table
    cursor.execute('''
        SELECT p.id, p.location, l.id as location_id
        FROM price_items p
        JOIN locations l ON p.location = l.name
        WHERE p.location != ""
    ''')
    for row in cursor.fetchall():
        try:
            cursor.execute('INSERT OR IGNORE INTO price_item_locations (price_item_id, location_id) VALUES (?, ?)',
                         (row['id'], row['location_id']))
        except:
            pass

    conn.commit()
    conn.close()

def migrate_inventory_location():
    """Add location column to inventory_items if it doesn't exist."""
    conn = get_db()
    cursor = conn.cursor()

    # Check if location column exists in inventory_items
    cursor.execute("PRAGMA table_info(inventory_items)")
    columns = [col[1] for col in cursor.fetchall()]

    if 'location' not in columns:
        cursor.execute('ALTER TABLE inventory_items ADD COLUMN location TEXT DEFAULT ""')
        conn.commit()

    conn.close()

def migrate_inventory_counts_store():
    """Add store column to inventory_counts for multi-store support."""
    conn = get_db()
    cursor = conn.cursor()

    # Check if store column exists in inventory_counts
    cursor.execute("PRAGMA table_info(inventory_counts)")
    columns = [col[1] for col in cursor.fetchall()]

    if 'store' not in columns:
        # We need to recreate the table with store column
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory_counts_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inventory_item_id TEXT,
                store TEXT DEFAULT '',
                month TEXT,
                count1 REAL DEFAULT 0,
                count2 REAL DEFAULT 0,
                count3 REAL DEFAULT 0,
                count4 REAL DEFAULT 0,
                created_at TEXT,
                updated_at TEXT,
                FOREIGN KEY (inventory_item_id) REFERENCES inventory_items(id),
                UNIQUE(inventory_item_id, store, month)
            )
        ''')

        # Copy data from old table (use location as store if it exists, otherwise empty)
        if 'location' in columns:
            cursor.execute('''
                INSERT INTO inventory_counts_new
                (inventory_item_id, store, month, count1, count2, count3, count4, created_at, updated_at)
                SELECT inventory_item_id, COALESCE(location, ''), month, count1, count2, count3, count4, created_at, updated_at
                FROM inventory_counts
            ''')
        else:
            cursor.execute('''
                INSERT INTO inventory_counts_new
                (inventory_item_id, store, month, count1, count2, count3, count4, created_at, updated_at)
                SELECT inventory_item_id, '', month, count1, count2, count3, count4, created_at, updated_at
                FROM inventory_counts
            ''')

        # Drop old table and rename new one
        cursor.execute('DROP TABLE inventory_counts')
        cursor.execute('ALTER TABLE inventory_counts_new RENAME TO inventory_counts')

        # Recreate index
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_inventory_counts_item ON inventory_counts(inventory_item_id)')

        conn.commit()

    conn.close()

def migrate_inventory_counts_cost():
    """Add cost column to inventory_counts for historical price tracking."""
    conn = get_db()
    cursor = conn.cursor()

    # Check if cost column exists in inventory_counts
    cursor.execute("PRAGMA table_info(inventory_counts)")
    columns = [col[1] for col in cursor.fetchall()]

    if 'cost' not in columns:
        # Add cost column
        cursor.execute('ALTER TABLE inventory_counts ADD COLUMN cost REAL DEFAULT NULL')

        # Populate existing counts with current item costs
        cursor.execute('''
            UPDATE inventory_counts
            SET cost = (SELECT cost FROM inventory_items WHERE id = inventory_counts.inventory_item_id)
        ''')

        conn.commit()

    conn.close()

def migrate_archived_column():
    """Add archived column to price_items and inventory_items."""
    conn = get_db()
    cursor = conn.cursor()

    # Check and add archived column to price_items
    cursor.execute("PRAGMA table_info(price_items)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'archived' not in columns:
        cursor.execute('ALTER TABLE price_items ADD COLUMN archived INTEGER DEFAULT 0')

    # Check and add archived column to inventory_items
    cursor.execute("PRAGMA table_info(inventory_items)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'archived' not in columns:
        cursor.execute('ALTER TABLE inventory_items ADD COLUMN archived INTEGER DEFAULT 0')

    conn.commit()
    conn.close()

# ==================== SUPPLIER FUNCTIONS ====================

def get_all_suppliers():
    """Get all suppliers."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT id, name FROM suppliers ORDER BY name')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def add_supplier(name):
    """Add a new supplier."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    try:
        cursor.execute('INSERT INTO suppliers (name, created_at) VALUES (?, ?)', (name, now))
        conn.commit()
        supplier_id = cursor.lastrowid
        conn.close()
        return {'id': supplier_id, 'name': name}
    except sqlite3.IntegrityError:
        conn.close()
        return None

def update_supplier(supplier_id, name):
    """Update a supplier name."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE suppliers SET name = ? WHERE id = ?', (name, supplier_id))
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return {'id': supplier_id, 'name': name} if updated else None
    except sqlite3.IntegrityError:
        conn.close()
        return None

def delete_supplier(supplier_id):
    """Delete a supplier."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM suppliers WHERE id = ?', (supplier_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

# ==================== STORE FUNCTIONS ====================

def get_all_stores():
    """Get all stores (physical locations like Inman, Central)."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT id, name FROM stores ORDER BY name')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def add_store(name):
    """Add a new store."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    try:
        cursor.execute('INSERT INTO stores (name, created_at) VALUES (?, ?)', (name, now))
        conn.commit()
        store_id = cursor.lastrowid
        conn.close()
        return {'id': store_id, 'name': name}
    except sqlite3.IntegrityError:
        conn.close()
        return None

def update_store(store_id, name):
    """Update a store name."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE stores SET name = ? WHERE id = ?', (name, store_id))
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return {'id': store_id, 'name': name} if updated else None
    except sqlite3.IntegrityError:
        conn.close()
        return None

def delete_store(store_id):
    """Delete a store."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM stores WHERE id = ?', (store_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

# ==================== LOCATION FUNCTIONS ====================

def get_all_locations():
    """Get all locations."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT id, name FROM locations ORDER BY name')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def add_location(name):
    """Add a new location."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    try:
        cursor.execute('INSERT INTO locations (name, created_at) VALUES (?, ?)', (name, now))
        conn.commit()
        location_id = cursor.lastrowid
        conn.close()
        return {'id': location_id, 'name': name}
    except sqlite3.IntegrityError:
        conn.close()
        return None

def update_location(location_id, name):
    """Update a location name."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE locations SET name = ? WHERE id = ?', (name, location_id))
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return {'id': location_id, 'name': name} if updated else None
    except sqlite3.IntegrityError:
        conn.close()
        return None

def delete_location(location_id):
    """Delete a location."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM locations WHERE id = ?', (location_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

# ==================== PRICE ITEM LOCATIONS FUNCTIONS ====================

def get_price_item_locations(price_item_id):
    """Get all location names for a price item."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT l.name FROM locations l
        JOIN price_item_locations pil ON l.id = pil.location_id
        WHERE pil.price_item_id = ?
        ORDER BY l.name
    ''', (price_item_id,))
    rows = cursor.fetchall()
    conn.close()
    return [row['name'] for row in rows]

def set_price_item_locations(price_item_id, location_names):
    """Set locations for a price item (replaces existing)."""
    conn = get_db()
    cursor = conn.cursor()

    # Clear existing locations
    cursor.execute('DELETE FROM price_item_locations WHERE price_item_id = ?', (price_item_id,))

    # Add new locations
    for name in location_names:
        if not name:
            continue
        # Get or create location
        cursor.execute('SELECT id FROM locations WHERE name = ?', (name,))
        row = cursor.fetchone()
        if row:
            location_id = row['id']
        else:
            now = datetime.now().isoformat()
            cursor.execute('INSERT INTO locations (name, created_at) VALUES (?, ?)', (name, now))
            location_id = cursor.lastrowid

        # Add to junction table
        cursor.execute('INSERT OR IGNORE INTO price_item_locations (price_item_id, location_id) VALUES (?, ?)',
                     (price_item_id, location_id))

    conn.commit()
    conn.close()

# ==================== INVENTORY FUNCTIONS ====================

def get_all_inventory(month=None, store=None, include_archived=False):
    """Get all inventory items with counts for specified month and store."""
    conn = get_db()
    cursor = conn.cursor()

    # Build JOIN condition for counts
    join_conditions = []
    join_params = []
    if month:
        join_conditions.append("c.month = ?")
        join_params.append(month)
    if store:
        join_conditions.append("c.store = ?")
        join_params.append(store)

    join_clause = " AND ".join(join_conditions) if join_conditions else "1=1"
    archived_clause = "" if include_archived else "WHERE COALESCE(i.archived, 0) = 0"

    query = f'''
        SELECT i.id, i.supplier, i.location, i.item, i.unit,
               COALESCE(c.cost, i.cost) as cost,
               i.cost as current_cost,
               i.is_custom,
               COALESCE(i.archived, 0) as archived,
               i.created_at, i.updated_at,
               COALESCE(c.count1, 0) as count1,
               COALESCE(c.count2, 0) as count2,
               COALESCE(c.count3, 0) as count3,
               COALESCE(c.count4, 0) as count4
        FROM inventory_items i
        LEFT JOIN inventory_counts c ON i.id = c.inventory_item_id AND {join_clause}
        {archived_clause}
        ORDER BY i.item
    '''

    cursor.execute(query, join_params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_inventory_item(item_id, month=None, store=None):
    """Get a single inventory item by ID with counts for specified month and store."""
    conn = get_db()
    cursor = conn.cursor()

    # Build JOIN condition for counts
    join_conditions = []
    join_params = []
    if month:
        join_conditions.append("c.month = ?")
        join_params.append(month)
    if store:
        join_conditions.append("c.store = ?")
        join_params.append(store)

    if join_conditions:
        join_clause = " AND ".join(join_conditions)
        query = f'''
            SELECT i.id, i.supplier, i.location, i.item, i.unit,
                   COALESCE(c.cost, i.cost) as cost,
                   i.cost as current_cost,
                   i.is_custom,
                   i.created_at, i.updated_at,
                   COALESCE(c.count1, 0) as count1,
                   COALESCE(c.count2, 0) as count2,
                   COALESCE(c.count3, 0) as count3,
                   COALESCE(c.count4, 0) as count4
            FROM inventory_items i
            LEFT JOIN inventory_counts c ON i.id = c.inventory_item_id AND {join_clause}
            WHERE i.id = ?
        '''
        cursor.execute(query, join_params + [item_id])
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

def update_inventory_item(item_id, data, month=None, store=None):
    """Update an inventory item and its counts for specific month and store."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    # Update base item info (supplier, location, item, unit, cost)
    cursor.execute('''
        UPDATE inventory_items
        SET supplier = ?, location = ?, item = ?, unit = ?, cost = ?, updated_at = ?
        WHERE id = ?
    ''', (
        data.get('supplier', ''),
        data.get('location', ''),
        data.get('item', ''),
        data.get('unit', ''),
        data.get('cost', 0),
        now,
        item_id
    ))

    # Update counts for specific month and store if provided
    if month:
        store_name = store or ''
        # Get the current item cost to store with the count (for historical tracking)
        item_cost = data.get('cost', 0)
        cursor.execute('''
            INSERT INTO inventory_counts (inventory_item_id, store, month, count1, count2, count3, count4, cost, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(inventory_item_id, store, month) DO UPDATE SET
                count1 = excluded.count1,
                count2 = excluded.count2,
                count3 = excluded.count3,
                count4 = excluded.count4,
                cost = COALESCE(inventory_counts.cost, excluded.cost),
                updated_at = excluded.updated_at
        ''', (
            item_id,
            store_name,
            month,
            data.get('count1', 0),
            data.get('count2', 0),
            data.get('count3', 0),
            data.get('count4', 0),
            item_cost,
            now,
            now
        ))

    conn.commit()
    conn.close()
    return get_inventory_item(item_id, month, store)

def add_inventory_item(data):
    """Add a new custom inventory item."""
    conn = get_db()
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    item_id = f"custom-{int(datetime.now().timestamp() * 1000)}"

    cursor.execute('''
        INSERT INTO inventory_items
        (id, supplier, location, item, unit, cost, count1, count2, count3, count4, is_custom, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 1, ?, ?)
    ''', (
        item_id,
        data.get('supplier', ''),
        data.get('location', ''),
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

def clear_all_counts(month=None, store=None):
    """Reset all inventory counts to zero for a specific month and/or store."""
    conn = get_db()
    cursor = conn.cursor()

    conditions = []
    params = []
    if month:
        conditions.append('month = ?')
        params.append(month)
    if store:
        conditions.append('store = ?')
        params.append(store)

    if conditions:
        query = 'DELETE FROM inventory_counts WHERE ' + ' AND '.join(conditions)
        cursor.execute(query, params)

    conn.commit()
    conn.close()

# ==================== PRICE FUNCTIONS ====================

def get_all_prices(include_archived=False):
    """Get all price items with their history and locations."""
    conn = get_db()
    cursor = conn.cursor()

    # Get all price items
    if include_archived:
        cursor.execute('SELECT * FROM price_items ORDER BY item')
    else:
        cursor.execute('SELECT * FROM price_items WHERE COALESCE(archived, 0) = 0 ORDER BY item')
    items = [dict(row) for row in cursor.fetchall()]

    # Get history and locations for each item
    for item in items:
        cursor.execute('''
            SELECT month, price FROM price_history
            WHERE price_item_id = ?
            ORDER BY id DESC
        ''', (item['id'],))
        item['priceHistory'] = {row['month']: row['price'] for row in cursor.fetchall()}

        # Get locations array
        cursor.execute('''
            SELECT l.name FROM locations l
            JOIN price_item_locations pil ON l.id = pil.location_id
            WHERE pil.price_item_id = ?
            ORDER BY l.name
        ''', (item['id'],))
        item['locations'] = [row['name'] for row in cursor.fetchall()]

    conn.close()
    return items

def get_price_item(item_id):
    """Get a single price item with history and locations."""
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

    # Get locations array
    cursor.execute('''
        SELECT l.name FROM locations l
        JOIN price_item_locations pil ON l.id = pil.location_id
        WHERE pil.price_item_id = ?
        ORDER BY l.name
    ''', (item_id,))
    item['locations'] = [row['name'] for row in cursor.fetchall()]

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
    """Add a new price item with multiple locations."""
    conn = get_db()
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    item_id = f"price-{int(datetime.now().timestamp() * 1000)}"

    units_per_inv = data.get('units_per_inv', 1) or 1
    current_price = data.get('current_price', 0) or 0
    per_unit_cost = round(current_price / units_per_inv, 2)

    # Handle both single location (backward compat) and locations array
    locations = data.get('locations', [])
    if not locations and data.get('location'):
        locations = [data.get('location')]

    cursor.execute('''
        INSERT INTO price_items
        (id, location, supplier, item, purchase_unit, units_per_inv, current_price, per_unit_cost, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        item_id,
        ', '.join(locations) if locations else '',  # Keep location field for display/compat
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

    # Set locations in junction table
    if locations:
        set_price_item_locations(item_id, locations)

    return get_price_item(item_id)

def update_price_item(item_id, data):
    """Update all fields of a price item including locations."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    units_per_inv = data.get('units_per_inv', 1) or 1
    current_price = data.get('current_price', 0) or 0
    per_unit_cost = round(current_price / units_per_inv, 2)

    # Handle both single location (backward compat) and locations array
    locations = data.get('locations', [])
    if not locations and data.get('location'):
        locations = [data.get('location')]

    cursor.execute('''
        UPDATE price_items
        SET location = ?, supplier = ?, item = ?, purchase_unit = ?,
            units_per_inv = ?, current_price = ?, per_unit_cost = ?, updated_at = ?
        WHERE id = ?
    ''', (
        ', '.join(locations) if locations else '',  # Keep location field for display/compat
        data.get('supplier', ''),
        data.get('item', ''),
        data.get('purchase_unit', ''),
        units_per_inv,
        current_price,
        per_unit_cost,
        now,
        item_id
    ))

    updated = cursor.rowcount > 0
    conn.commit()
    conn.close()

    # Update locations in junction table
    if updated:
        set_price_item_locations(item_id, locations)

    return get_price_item(item_id) if updated else None

def delete_price_item(item_id):
    """Delete a price item and its location associations."""
    conn = get_db()
    cursor = conn.cursor()

    # Delete location associations first (due to foreign key)
    cursor.execute('DELETE FROM price_item_locations WHERE price_item_id = ?', (item_id,))

    # Delete the price item
    cursor.execute('DELETE FROM price_items WHERE id = ?', (item_id,))
    deleted = cursor.rowcount > 0

    conn.commit()
    conn.close()
    return deleted

def archive_price_item(item_id, archive=True):
    """Archive or unarchive a price item and matching inventory items."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    archived_val = 1 if archive else 0

    # Get the price item name first
    cursor.execute('SELECT item FROM price_items WHERE id = ?', (item_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False

    item_name = row['item']

    # Archive the price item
    cursor.execute('UPDATE price_items SET archived = ?, updated_at = ? WHERE id = ?',
                   (archived_val, now, item_id))

    # Archive matching inventory items (exact name match)
    cursor.execute('UPDATE inventory_items SET archived = ?, updated_at = ? WHERE LOWER(item) = LOWER(?)',
                   (archived_val, now, item_name))
    inventory_updated = cursor.rowcount

    conn.commit()
    conn.close()
    return {'price_item': item_id, 'inventory_items_updated': inventory_updated}

def archive_inventory_item(item_id, archive=True):
    """Archive or unarchive an inventory item."""
    conn = get_db()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    archived_val = 1 if archive else 0

    cursor.execute('UPDATE inventory_items SET archived = ?, updated_at = ? WHERE id = ?',
                   (archived_val, now, item_id))
    updated = cursor.rowcount > 0

    conn.commit()
    conn.close()
    return updated

def sync_prices_to_inventory():
    """Sync per_unit_cost, supplier, and location from prices to inventory."""
    conn = get_db()
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    updated = 0

    # Get price items with their first location (from junction table)
    cursor.execute('''
        SELECT p.id, p.item, p.supplier, p.per_unit_cost,
               (SELECT l.name FROM price_item_locations pil
                JOIN locations l ON pil.location_id = l.id
                WHERE pil.price_item_id = p.id
                ORDER BY l.name LIMIT 1) as location
        FROM price_items p
    ''')
    price_items = cursor.fetchall()

    for price_item in price_items:
        item_lower = price_item['item'].lower()
        cursor.execute('''
            SELECT id, cost, supplier, location FROM inventory_items
            WHERE LOWER(item) LIKE ? OR ? LIKE '%' || LOWER(item) || '%'
        ''', (f'%{item_lower}%', item_lower))

        for inv_item in cursor.fetchall():
            # Check if any field needs updating
            cost_changed = abs((inv_item['cost'] or 0) - (price_item['per_unit_cost'] or 0)) > 0.001
            supplier_changed = (inv_item['supplier'] or '') != (price_item['supplier'] or '')
            location_changed = (inv_item['location'] or '') != (price_item['location'] or '')

            if cost_changed or supplier_changed or location_changed:
                cursor.execute('''
                    UPDATE inventory_items
                    SET cost = ?, supplier = ?, location = ?, updated_at = ?
                    WHERE id = ?
                ''', (
                    price_item['per_unit_cost'],
                    price_item['supplier'] or '',
                    price_item['location'] or '',
                    now,
                    inv_item['id']
                ))
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
