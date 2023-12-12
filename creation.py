import psycopg2
import csv

conn = psycopg2.connect(database="just_in_time", user="postgres",
                        password="postgres", host="localhost", port="5432")
cursor = conn.cursor()

cursor.execute('DROP TABLE IF EXISTS "Customer" CASCADE')
cursor.execute('DROP TABLE IF EXISTS "Order" CASCADE')
cursor.execute('DROP TABLE IF EXISTS "Warehouse" CASCADE')
cursor.execute('DROP TABLE IF EXISTS "Delivery" CASCADE')
cursor.execute('DROP TABLE IF EXISTS "Product" CASCADE')
cursor.execute('DROP TABLE IF EXISTS "DeliveryProduct" CASCADE')
cursor.execute('DROP TABLE IF EXISTS "WarehouseProduct" CASCADE')
cursor.execute('DROP TABLE IF EXISTS "OrderDelivery" CASCADE')


cursor.execute('''
    CREATE TABLE IF NOT EXISTS "Customer" (
        "CustomerID" SERIAL PRIMARY KEY,
        "Market" TEXT,
        "Region" TEXT,
        "Country" TEXT
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS "Product" (
        "Name" TEXT PRIMARY KEY,
        "Department" TEXT,
        "Category" TEXT,
        "FulfillmentDays" REAL
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS "Warehouse" (
        "Country" TEXT PRIMARY KEY
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS "Order" (
        "OrderID" SERIAL PRIMARY KEY,
        "CustomerID" INTEGER,
        FOREIGN KEY ("CustomerID") REFERENCES "Customer"("CustomerID")
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS "Delivery" (
        "DeliveryID" SERIAL PRIMARY KEY,
        "ShipmentDate" DATE,
        "ShipmentMode" TEXT,
        "Scheduled" INTEGER,
        "WarehouseCountry" TEXT,
        FOREIGN KEY ("WarehouseCountry") REFERENCES "Warehouse"("Country")
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS "OrderDelivery" (
        "OrderID" INTEGER,
        "DeliveryID" INTEGER,
        "OrderDate" DATE,
        PRIMARY KEY ("OrderID", "DeliveryID"),
        FOREIGN KEY ("OrderID") REFERENCES "Order"("OrderID"),
        FOREIGN KEY ("DeliveryID") REFERENCES "Delivery"("DeliveryID")
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS "DeliveryProduct" (
        "DeliveryID" INTEGER,
        "ProductName" TEXT,
        "Quantity" INTEGER,
        "Discount" REAL,
        "Profit" REAL,
        "GrossSales" REAL,
        PRIMARY KEY ("DeliveryID", "ProductName"),
        FOREIGN KEY ("DeliveryID") REFERENCES "Delivery"("DeliveryID"),
        FOREIGN KEY ("ProductName") REFERENCES "Product"("Name")
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS "WarehouseProduct" (
        "WarehouseCountry" TEXT,
        "ProductName" TEXT,
        "StockQuantity" INTEGER,
        "CostPerUnit" REAL,
        PRIMARY KEY ("WarehouseCountry", "ProductName"),
        FOREIGN KEY ("WarehouseCountry") REFERENCES "Warehouse"("Country"),
        FOREIGN KEY ("ProductName") REFERENCES "Product"("Name")
    );
''')

conn.commit()


def read_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return list(reader)


orders_and_shipments_data = read_csv('orders_and_shipments.csv')
fulfillment_data = read_csv('fulfillment.csv')
inventory_data = read_csv('inventory.csv')

existing_order = set()
existing_customers = set()
existing_products = set()
existing_warehouses = set()
delivery_id = 0
for row in orders_and_shipments_data:
    customer_id = row.get('Customer ID', '')
    customer_market = row.get('Customer Market', '')
    customer_region = row.get('Customer Region', '')
    customer_country = row.get('Customer Country', '')

    product_name = row.get('Product Name', '')
    product_department = row.get('Product Department', '')
    product_category = row.get('Product Category', '')

    warehouse_country = row.get('Warehouse Country', '')

    order_id = int(row.get('Order ID', ''))
    order_year = str(int(row.get('Order Year', '')) + 6)
    order_month = row.get('Order Month', '')
    order_day = row.get('Order Day', '')
    if int(order_month) == 2 and int(order_day) > 28:
        order_day = 28
    order_date = f"{order_year}/{order_month}/{order_day}"

    product_quantity = int(row.get('Order Quantity', 0))
    try:
        discount = float(row.get('Discount', ''))
    except ValueError:
        discount = None
    profit = float(row.get('Profit', 0))
    gross_sales = float(row.get('Gross Sales', 0))

    shipment_year = str(int(row.get('Shipment Year', '')) + 6)
    shipment_month = row.get('Shipment Month', '')
    shipment_day = row.get('Shipment Day', '')
    if int(shipment_month) == 2 and int(shipment_day) > 28:
        shipment_day = 28
    shipment_date = f"{shipment_year}/{shipment_month}/{shipment_day}"
    shipment_mode = row.get('Shipment Mode', '')
    shipment_scheduled = row.get('Shipment Days - Scheduled', '')

    if customer_id not in existing_customers:
        cursor.execute('INSERT INTO "Customer" ("CustomerID", "Market", "Region", "Country") VALUES (%s, %s, %s, %s)',
                       (customer_id, customer_market, customer_region, customer_country))
        existing_customers.add(customer_id)

    if product_name not in existing_products:
        cursor.execute('INSERT INTO "Product" ("Name", "Department", "Category") VALUES (%s, %s, %s)',
                       (product_name, product_department, product_category))
        existing_products.add(product_name)

    if warehouse_country not in existing_warehouses:
        cursor.execute('INSERT INTO "Warehouse" ("Country") VALUES (%s)', (warehouse_country,))
        existing_warehouses.add(warehouse_country)

    if order_id not in existing_order:
        cursor.execute(
            'INSERT INTO "Order" ("OrderID", "CustomerID") VALUES (%s, %s)',
            (order_id, customer_id))
        existing_order.add(order_id)

    cursor.execute('INSERT INTO "Delivery" ("ShipmentDate", "ShipmentMode", "Scheduled",'
                   ' "WarehouseCountry") VALUES (%s, %s, %s, %s)',
                   (shipment_date, shipment_mode, shipment_scheduled, warehouse_country))

    # delivery_id = cursor.lastrowid
    delivery_id += 1
    cursor.execute(
        'INSERT INTO "DeliveryProduct" '
        '("DeliveryID", "ProductName", "Quantity", "Discount", "Profit", "GrossSales") VALUES (%s, %s, %s, %s, %s, %s)',
        (delivery_id, product_name, product_quantity, discount, profit, gross_sales))

    cursor.execute('INSERT INTO "OrderDelivery" ("OrderID", "DeliveryID", "OrderDate") VALUES (%s, %s, %s)',
                   (order_id, delivery_id, order_date))

for fulfillment in fulfillment_data:
    product_name = fulfillment.get('Product Name', '')
    fulfillment_days = float(fulfillment.get('Warehouse Order Fulfillment (days)', ''))

    cursor.execute('SELECT * FROM "Product" WHERE "Name" = %s', (product_name,))
    existing_product = cursor.fetchone()

    if existing_product:
        cursor.execute('UPDATE "Product" SET "FulfillmentDays" = %s WHERE "Name" = %s',
                       (fulfillment_days, product_name))
    else:
        cursor.execute('INSERT INTO "Product" ("Name", "FulfillmentDays") VALUES (%s, %s)',
                       (product_name, fulfillment_days))

inventory_data = read_csv('inventory.csv')
for inventory in inventory_data:
    product_name = inventory.get('Product Name', '')
    warehouse_country = inventory.get('Warehouse Country', '')
    stock_quantity = int(inventory.get('Stock Quantity', ''))
    cost_per_unit = float(inventory.get('Cost per Unit', '').replace(',', '.'))
    cursor.execute(
        'INSERT INTO "WarehouseProduct" ("WarehouseCountry", "ProductName", "StockQuantity", "CostPerUnit")'
        ' VALUES (%s, %s, %s, %s)', (warehouse_country, product_name, stock_quantity, cost_per_unit))
conn.commit()

conn.close()

