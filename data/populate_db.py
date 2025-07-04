# immersive id="data_munging_script" type="code" title="Python Script: Database Schema and Population (Based ONLY on Provided CSV Headers)"
import sqlite3
import pandas as pd
import os

# --- Configuration ---
DATABASE_NAME = 'simplified_walmart_shipments.db' # Changed DB name to reflect simplification
# Paths to your CSV files
# Headers provided by user:
# SPREADSHEET0_PATH ('shipping_data_0.csv'): origin_warehouse,destination_store,product,on_time,product_quantity,driver_identifier
# SPREADSHEET1_PATH ('shipping_data_1.csv'): shipment_identifier,product,on_time
# SPREADSHEET2_PATH ('shipping_data_2.csv'): shipment_identifier,origin_warehouse,destination_store,driver_identifier

SPREADSHEET0_PATH = 'shipping_data_0.csv'
SPREADSHEET1_PATH = 'shipping_data_1.csv'
SPREADSHEET2_PATH = 'shipping_data_2.csv'

# --- 1. NEW Simplified Database Setup (Derived ONLY from Provided CSV Headers) ---
def setup_database(db_name):
    """
    Creates a simplified SQLite database schema based ONLY on the column names
    present in the provided CSV headers for shipping_data_0, 1, and 2.
    This schema does NOT include tables for Manufacturer, PetFood, PetToy, PetApparel,
    Animal, Customer, or Transaction, nor detailed columns like ZipCode or ShipmentDate.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute("PRAGMA foreign_keys = ON;")

    # 1. Product Table (from 'product' in csv0/csv1)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Products (
        ProductID INTEGER PRIMARY KEY AUTOINCREMENT,
        ProductName TEXT NOT NULL UNIQUE
    );
    """)

    # 2. Location Table (from 'origin_warehouse' and 'destination_store' in csv0/csv2)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Locations (
        LocationID INTEGER PRIMARY KEY AUTOINCREMENT,
        LocationName TEXT NOT NULL UNIQUE
    );
    """)

    # 3. Driver Table (from 'driver_identifier' in csv0/csv2)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Drivers (
        DriverID INTEGER PRIMARY KEY AUTOINCREMENT,
        DriverIdentifier TEXT NOT NULL UNIQUE
    );
    """)

    # 4. Shipments Table (from 'shipment_identifier' in csv1/csv2, and linking locations/drivers)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Shipments (
        ShipmentID INTEGER PRIMARY KEY, -- Using CSV's shipment_identifier as PK directly
        OriginLocationID INTEGER NOT NULL,
        DestinationLocationID INTEGER NOT NULL,
        DriverID INTEGER NOT NULL,
        -- No ShipmentDate as it's not in provided headers
        FOREIGN KEY (OriginLocationID) REFERENCES Locations(LocationID),
        FOREIGN KEY (DestinationLocationID) REFERENCES Locations(LocationID),
        FOREIGN KEY (DriverID) REFERENCES Drivers(DriverID)
    );
    """)

    # 5. ShipmentLineItems Table (combines product, quantity, on_time from csv0/csv1)
    # We'll use csv0 for quantity where available, otherwise use default
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ShipmentLineItems (
        LineItemID INTEGER PRIMARY KEY AUTOINCREMENT,
        ShipmentID INTEGER NOT NULL,
        ProductID INTEGER NOT NULL,
        Quantity INTEGER NOT NULL,
        OnTimeStatus TEXT, -- 'Yes' or 'No'
        FOREIGN KEY (ShipmentID) REFERENCES Shipments(ShipmentID),
        FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
    );
    """)

    conn.commit()
    conn.close()
    print(f"Database '{db_name}' and simplified tables set up successfully based on CSV headers.")
    print("NOTE: This schema is simplified and does not include detailed product, manufacturer, or extended shipment/location attributes.")

# --- Helper Functions for Data Insertion (Adjusted for simplified schema) ---

def get_or_insert_product(cursor, product_name):
    """Gets ProductID if exists, otherwise inserts and returns new ID."""
    cursor.execute("SELECT ProductID FROM Products WHERE ProductName = ?", (product_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO Products (ProductName) VALUES (?)", (product_name,))
        return cursor.lastrowid

def get_or_insert_location(cursor, location_name):
    """Gets LocationID if exists, otherwise inserts and returns new ID."""
    cursor.execute("SELECT LocationID FROM Locations WHERE LocationName = ?", (location_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO Locations (LocationName) VALUES (?)", (location_name,))
        return cursor.lastrowid

def get_or_insert_driver(cursor, driver_identifier):
    """Gets DriverID if exists, otherwise inserts and returns new ID."""
    cursor.execute("SELECT DriverID FROM Drivers WHERE DriverIdentifier = ?", (driver_identifier,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO Drivers (DriverIdentifier) VALUES (?)", (driver_identifier,))
        return cursor.lastrowid

# --- 3. Populate Database Function (Adjusted for NEW simplified schema and provided CSV headers) ---
def populate_database(db_name, s0_path, s1_path, s2_path):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # --- Step 1: Populate Products, Locations, Drivers from all CSVs ---
    print("\nPopulating Products, Locations, and Drivers tables from all CSVs...")
    
    df0 = pd.read_csv(s0_path)
    df1 = pd.read_csv(s1_path)
    df2 = pd.read_csv(s2_path)

    # Products: from df0 and df1
    all_products = pd.concat([df0['product'], df1['product']]).unique()
    for product_name in all_products:
        get_or_insert_product(cursor, product_name)
    conn.commit()

    # Locations: from df0 and df2
    all_locations = pd.concat([
        df0['origin_warehouse'], df0['destination_store'],
        df2['origin_warehouse'], df2['destination_store']
    ]).unique()
    for location_name in all_locations:
        get_or_insert_location(cursor, location_name)
    conn.commit()

    # Drivers: from df0 and df2
    all_drivers = pd.concat([df0['driver_identifier'], df2['driver_identifier']]).unique()
    for driver_identifier in all_drivers:
        get_or_insert_driver(cursor, driver_identifier)
    conn.commit()
    print("Finished initial population of Products, Locations, and Drivers.")

    # --- Step 2: Populate Shipments and ShipmentLineItems ---
    print("\nPopulating Shipments and ShipmentLineItems tables...")


    unique_shipments_df2 = df2.drop_duplicates(subset=['shipment_identifier'])

    for index, row_s2 in unique_shipments_df2.iterrows():
        try:
            shipment_id = row_s2['shipment_identifier']
            origin_loc_id = get_or_insert_location(cursor, row_s2['origin_warehouse'])
            dest_loc_id = get_or_insert_location(cursor, row_s2['destination_store'])
            driver_id = get_or_insert_driver(cursor, row_s2['driver_identifier'])

            cursor.execute("""
                INSERT OR IGNORE INTO Shipments (ShipmentID, OriginLocationID, DestinationLocationID, DriverID)
                VALUES (?, ?, ?, ?)
            """, (shipment_id, origin_loc_id, dest_loc_id, driver_id))
            conn.commit()
        except sqlite3.IntegrityError as e:
            print(f"Integrity Error inserting shipment {row_s2.get('shipment_identifier', 'N/A')} from {s2_path}: {e}")
            conn.rollback()
        except Exception as e:
            print(f"An unexpected error occurred processing shipment from {s2_path}: {e} for shipment {row_s2.get('shipment_identifier', 'N/A')}")
            conn.rollback()

    # Process ShipmentLineItems from df1, as it has shipment_identifier
    for index, row_s1 in df1.iterrows():
        try:
            shipment_id = row_s1['shipment_identifier']
            product_id = get_or_insert_product(cursor, row_s1['product'])
            on_time_status = row_s1['on_time']
            
            # Quantity is not in df1.product_quantity. Default to 1.
            quantity = 1
            print(f"Warning: Quantity not found in {s1_path} for shipment {shipment_id} product {row_s1['product']}. Defaulting to {quantity}.")

            cursor.execute("""
                INSERT INTO ShipmentLineItems (ShipmentID, ProductID, Quantity, OnTimeStatus)
                VALUES (?, ?, ?, ?)
            """, (shipment_id, product_id, quantity, on_time_status))
            conn.commit()
        except sqlite3.IntegrityError as e:
            print(f"Integrity Error inserting line item for shipment {row_s1.get('shipment_identifier', 'N/A')} from {s1_path}: {e}")
            conn.rollback()
        except Exception as e:
            print(f"An unexpected error occurred processing line item from {s1_path}: {e} for shipment {row_s1.get('shipment_identifier', 'N/A')}")
            conn.rollback()
            
    print("Finished population of Shipments and ShipmentLineItems.")
    
    conn.close()
    print("Database population complete with simplified schema.")

# --- Main Execution ---
if __name__ == "__main__":
    if os.path.exists(DATABASE_NAME):
        os.remove(DATABASE_NAME)
        print(f"Removed existing database file: {DATABASE_NAME}")
 
    setup_database(DATABASE_NAME)

    populate_database(DATABASE_NAME, SPREADSHEET0_PATH, SPREADSHEET1_PATH, SPREADSHEET2_PATH)

    print("\n--- Verification (Optional, for your own testing) ---")
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    print("\nProducts:")
    cursor.execute("SELECT * FROM Products")
    print(cursor.fetchall())

    print("\nLocations:")
    cursor.execute("SELECT * FROM Locations")
    print(cursor.fetchall())
    
    print("\nDrivers:")
    cursor.execute("SELECT * FROM Drivers")
    print(cursor.fetchall())

    print("\nShipments:")
    cursor.execute("""
        SELECT s.ShipmentID, ol.LocationName as Origin, dl.LocationName as Destination, d.DriverIdentifier
        FROM Shipments s
        JOIN Locations ol ON s.OriginLocationID = ol.LocationID
        JOIN Locations dl ON s.DestinationLocationID = dl.LocationID
        JOIN Drivers d ON s.DriverID = d.DriverID
    """)
    print(cursor.fetchall())

    print("\nShipmentLineItems:")
    cursor.execute("""
        SELECT sli.ShipmentID, p.ProductName, sli.Quantity, sli.OnTimeStatus
        FROM ShipmentLineItems sli
        JOIN Products p ON sli.ProductID = p.ProductID LIMIT 10
    """)
    print(cursor.fetchall())

    conn.close()