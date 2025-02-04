import sqlite3
import pyodbc
import pandas as pd
from datetime import datetime

# Establish connection to SQL Server
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=192.168.10.1;"
                      "Database=GMS_LIVE;"
                      "uid=SAPReadonly;"
                      "pwd=AdmTsg@25;"
                      "Trusted_Connection=no;")

def fetch_and_store_data(cnxn, append=True):
    # Execute query to fetch data from SQL Server
    cursor = cnxn.cursor()
    query = """
        SELECT E.DocDate, E.FYears, E.CardName, E.GroupName as 'Group', E.Principal,
               E.Item, E.Quantity, E.Price as Sale, E.Cost, E.GPSC as GP,
               E.GPSC / (E.Price + 0.000001) * 100 as 'GP Ratio',
               E.ProductSegment as Segment, E.[Product Family] as Product
        FROM GetSalesGMSLive E
    """
    cursor.execute(query)
    
    # Fetch all data and get column names
    columns = [column[0] for column in cursor.description]
    data_rows = cursor.fetchall()

    # Convert to DataFrame
    df = pd.DataFrame.from_records(data_rows, columns=columns)
    
    print(df)
    # Rename columns in df
    df = df.rename(columns={
        'Group': 'GroupName',
        'Segment': 'ProductSegment',
        'Sale': 'Sales_Value',
        'GP Ratio': 'Gp_Ratio'
    })

    # Format 'DocDate' to match the expected format in the SQLite database
    df['DocDate'] = pd.to_datetime(df['DocDate'], errors='coerce').dt.strftime('%Y-%m-%d')

    # Convert numeric columns to float (applying numeric conversion directly)
    df['Sales_Value'] = pd.to_numeric(df['Sales_Value'], errors='coerce').fillna(0).astype(float)
    df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce').fillna(0).astype(float)
    df['GP'] = pd.to_numeric(df['GP'], errors='coerce').fillna(0).astype(float)
    df['Gp_Ratio'] = pd.to_numeric(df['Gp_Ratio'], errors='coerce').fillna(0).astype(float)
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
    # Reorganize the DataFrame to match your required columns for SQLite
    df = df[[
        "DocDate", "FYears", "CardName", "GroupName", "Principal",
        "Item", "Quantity", "Sales_Value", "Cost", "GP", "Gp_Ratio", "ProductSegment", "Product"
    ]]

    # Connect to SQLite database
    conn1 = sqlite3.connect('GMSLIVE.db')  # Replace 'GMSLIVE.db' with your SQLite database name
    cursor1 = conn1.cursor()

    # Create table (if it doesn't exist)
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS GMS (
        DocDate date,
        FYears TEXT,
        CardName TEXT,
        GroupName TEXT,
        Principal TEXT,
        Item TEXT,
        Quantity Decimal,
        Sales_Value Double,
        Cost Decimal,
        GP Decimal,
        Gp_Ratio Decimal,
        ProductSegment TEXT,
        Product TEXT,
        PRIMARY KEY (DocDate, CardName, Item)  -- Ensures no duplicates based on these fields
    )
    '''
    cursor1.execute(create_table_query)

    # Step 3: Insert data into SQLite database if not already present
    for row in df.itertuples(index=False):
        cursor1.execute('''
            INSERT OR IGNORE INTO GMS (DocDate, FYears, CardName, GroupName, Principal, 
                                       Item, Quantity, Sales_Value, Cost, GP, Gp_Ratio, ProductSegment, Product)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', row)

    # Step 4: Commit changes and close the connection
    conn1.commit()

    # Step 5: Print all records to confirm insertion
    data = cursor1.execute('''SELECT * FROM GMS''')
    print("The inserted records are:")
    for row in data:
        print(row)

    # Close the SQLite connection
    conn1.close()

    print("CSV data has been successfully inserted into the SQLite database!")

# Example usage (assuming 'cnxn' is your connection object)
fetch_and_store_data(cnxn, append=True)
