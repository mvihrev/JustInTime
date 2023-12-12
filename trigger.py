import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

conn = psycopg2.connect(
    database="just_in_time",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

drop_trigger_command = """
DO $$ 
BEGIN
    BEGIN
        EXECUTE 'DROP TRIGGER IF EXISTS update_stock_trigger ON "WarehouseProduct"';
    EXCEPTION
        WHEN undefined_table THEN
    END;
END $$;
"""

drop_function_command = """
DO $$ 
BEGIN
    BEGIN
        EXECUTE 'DROP FUNCTION IF EXISTS update_stock_trigger_function()';
    EXCEPTION
        WHEN undefined_function THEN
    END;
END $$;
"""

trigger_function = """
CREATE OR REPLACE FUNCTION update_stock_trigger_function()
RETURNS TRIGGER AS $$
DECLARE
    ROP INTEGER;
    EOQ INTEGER;
    TotalStock INTEGER;
BEGIN
    ROP := (SELECT ROUND(SUM(dp."Quantity") / CAST(28 * 2 as NUMERIC) * AVG(p."FulfillmentDays")) + 1
        FROM "DeliveryProduct" dp LEFT JOIN "WarehouseProduct" wp ON dp."ProductName" = wp."ProductName"
        LEFT JOIN "Delivery" d ON dp."DeliveryID" = d."DeliveryID" 
        LEFT JOIN "OrderDelivery" od ON od."DeliveryID" = dp."DeliveryID"
        LEFT JOIN "Product" p ON p."Name" = dp."ProductName"
        WHERE od."OrderDate" > '2023-11-12' AND od."OrderDate" < '2023-12-11'
        AND dp."ProductName" = NEW."ProductName");
        
    EOQ := (SELECT ROUND(SQRT(2 * SUM(dp."Quantity") * 5 / AVG(wp."CostPerUnit")))
        FROM "DeliveryProduct" dp LEFT JOIN "WarehouseProduct" wp ON dp."ProductName" = wp."ProductName"
        LEFT JOIN "Delivery" d ON dp."DeliveryID" = d."DeliveryID" 
        LEFT JOIN "OrderDelivery" od ON od."DeliveryID" = dp."DeliveryID"
        LEFT JOIN "Product" p ON p."Name" = dp."ProductName"
        WHERE od."OrderDate" > '2023-11-12' AND od."OrderDate" < '2023-12-11'
        AND dp."ProductName" = NEW."ProductName");
    
    TotalStock := (SELECT SUM(wp."StockQuantity") + NEW."StockQuantity"
        FROM "WarehouseProduct" wp WHERE wp."ProductName" = NEW."ProductName"
         AND wp."WarehouseCountry" <> NEW."WarehouseCountry");
    
    IF TotalStock < ROP THEN
        PERFORM pg_notify('email_channel',
         format('{"product_name": "%s", "stock_quantity": %s, "rop": %s, "eoq": %s}',
         NEW."ProductName", TotalStock, ROP, EOQ));
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

trigger_command = """
CREATE TRIGGER update_stock_trigger
    BEFORE UPDATE OF "StockQuantity" ON "WarehouseProduct"
    FOR EACH ROW
    WHEN (NEW."StockQuantity" <> OLD."StockQuantity")
    EXECUTE FUNCTION update_stock_trigger_function();
"""

enable_trigger_command = """
ALTER TABLE "WarehouseProduct" ENABLE TRIGGER "update_stock_trigger";
"""

with conn.cursor() as cursor:
    cursor.execute(drop_trigger_command)
    cursor.execute(drop_function_command)
    cursor.execute(trigger_function)
    cursor.execute(trigger_command)
    cursor.execute(enable_trigger_command)

conn.close()
