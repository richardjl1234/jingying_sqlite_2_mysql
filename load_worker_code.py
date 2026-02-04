"""
Load worker codes from Excel to MySQL database.
This program reads worker data from Excel sheet '工人', validates against SQLite,
and uploads the results to the workers table in MySQL.
"""

import os
import pandas as pd
from datetime import datetime
from sql_util import sqlite_sql


def main():
    """
    Main function to load worker data from Excel to MySQL.
    """
    # Step 1: Read worker data from Excel file
    excel_file = '定额型号类别编码_updated.xlsx'
    print(f"Reading worker data from Excel file: {excel_file}")
    print("Sheet: 工人")
    
    try:
        # Read 编码 as string to preserve any leading zeros
        df_excel = pd.read_excel(excel_file, sheet_name='工人', dtype={'编码': str})
        print(f"Loaded {len(df_excel)} worker records from Excel")
        print(f"Excel columns: {df_excel.columns.tolist()}")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return
    
    # Step 2: Include ALL workers (active and inactive)
    print(f"Total workers in Excel: {len(df_excel)}")
    
    # Step 3: Query distinct employee names from SQLite
    print("\nQuerying distinct employee names from SQLite database...")
    query = "select distinct 职员全名 from payroll_details order by 职员全名"
    df_sqlite = sqlite_sql(query)
    
    sqlite_names = set(df_sqlite['职员全名'].tolist())
    excel_names = set(df_excel['工人姓名'].tolist())
    
    print(f"Retrieved {len(sqlite_names)} distinct employee names from SQLite")
    print(f"Retrieved {len(excel_names)} workers from Excel")
    
    # Step 4: Check for discrepancies
    print("\nChecking for discrepancies between Excel and SQLite...")
    
    # Names in SQLite but not in Excel
    missing_in_excel = sqlite_names - excel_names
    
    # Names in Excel but not in SQLite
    extra_in_excel = excel_names - sqlite_names
    
    if missing_in_excel:
        print(f"\n❌ ERROR: {len(missing_in_excel)} employees found in SQLite but NOT in Excel:")
        for name in sorted(missing_in_excel):
            print(f"   - {name}")
    
    if extra_in_excel:
        print(f"\n⚠️  WARNING: {len(extra_in_excel)} workers found in Excel but NOT in SQLite:")
        for name in sorted(extra_in_excel):
            print(f"   - {name}")
    
    # If there are discrepancies, stop the program
    if missing_in_excel:
        print("\n🚫 Program terminated due to discrepancies. Please update the Excel file.")
        return
    
    print("\n✅ No discrepancies found! All SQLite employees are in Excel.")
    
    # Step 5: Prepare worker codes from Excel
    print("\nPreparing worker codes from Excel...")
    
    # Sort by 编码 (preserve the original order/codes from Excel)
    df_excel = df_excel.sort_values('编码')
    
    # Rename columns for the final output
    df_excel = df_excel.rename(columns={'工人姓名': 'name'})
    
    # Format the code directly from Excel (no W prefix)
    # Ensure the code is formatted as 3-digit zero-padded string
    df_excel['worker_code'] = df_excel['编码'].apply(lambda x: str(x).zfill(3))
    
    print("Generated worker codes:")
    print(df_excel[['worker_code', 'name']].head(10))
    
    # Step 6: Add timestamp columns
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%M')
    df_excel['created_at'] = current_time
    df_excel['updated_at'] = current_time
    
    print(f"\nCurrent timestamp: {current_time}")
    
    # Step 7: Upload to MySQL database
    table_name = "workers"
    
    print(f"\nUploading DataFrame to MySQL table: {table_name}")
    
    # Get MySQL database URL from environment variable
    MYSQL_DB_URL = os.environ.get("MYSQL_DB_URL")
    
    if not MYSQL_DB_URL:
        print("Error: MYSQL_DB_URL environment variable not set.")
        return
    
    # Select only the columns we need for the database
    df_final = df_excel[['worker_code', 'name', 'created_at', 'updated_at']].copy()
    
    # Use bulk insertion with pandas to_sql
    try:
        from sqlalchemy import create_engine
        engine = create_engine(MYSQL_DB_URL)
        
        # Bulk insert with chunksize
        df_final.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=100)
        
        print(f"Successfully bulk uploaded {len(df_final)} rows to MySQL table: {table_name}")
        
    except Exception as e:
        print(f"Bulk insert failed: {e}")
        print("Program terminated due to database error.")
        return
    
    print(f"\nAll {len(df_final)} rows successfully uploaded to MySQL table: {table_name}")
    
    # Display final data summary
    print("\nFinal data summary:")
    print(df_final.head(10))
    
    return df_final


if __name__ == "__main__":
    main()
