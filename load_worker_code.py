"""
Load worker codes from SQLite to MySQL database.
This program reads distinct employee names from SQLite, generates worker codes,
and uploads the results to the workers table in MySQL.
"""

import os
import pandas as pd
from datetime import datetime
from sql_util import sqlite_sql


def main():
    """
    Main function to load worker data from SQLite to MySQL.
    """
    # Step 1: Query distinct employee names from SQLite
    print("Querying distinct employee names from SQLite database...")
    query = "select distinct 职员全名 from payroll_details order by 职员全名"
    df = sqlite_sql(query)
    
    print(f"Retrieved {len(df)} distinct employee names")
    print(f"Columns: {df.columns.tolist()}")
    print("\nFirst 10 records:")
    print(df.head(10))
    
    # Step 2: Generate worker codes in format Wxxx (001, 002, etc.)
    print("\nGenerating worker codes...")
    
    # Create worker_code column with format Wxxx where xxx is sequence number
    df['worker_code'] = [f'W{str(i+1).zfill(3)}' for i in range(len(df))]
    
    # Rename 职员全名 to name
    df = df.rename(columns={'职员全名': 'name'})
    
    print("Generated worker codes:")
    print(df[['worker_code', 'name']].head(10))
    
    # Step 3: Add timestamp columns
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['created_at'] = current_time
    df['updated_at'] = current_time
    
    print(f"\nCurrent timestamp: {current_time}")
    
    # Step 4: Upload to MySQL database
    table_name = "workers"
    
    print(f"\nUploading DataFrame to MySQL table: {table_name}")
    
    # Get MySQL database URL from environment variable
    MYSQL_DB_URL = os.environ.get("MYSQL_DB_URL")
    
    if not MYSQL_DB_URL:
        print("Error: MYSQL_DB_URL environment variable not set.")
        return
    
    # Use bulk insertion with pandas to_sql
    try:
        from sqlalchemy import create_engine
        engine = create_engine(MYSQL_DB_URL)
        
        # Bulk insert with chunksize
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=100)
        
        print(f"Successfully bulk uploaded {len(df)} rows to MySQL table: {table_name}")
        
    except Exception as e:
        print(f"Bulk insert failed: {e}")
        print("Program terminated due to database error.")
        return
    
    print(f"\nAll {len(df)} rows successfully uploaded to MySQL table: {table_name}")
    
    # Display final data summary
    print("\nFinal data summary:")
    print(df[['worker_code', 'name', 'created_at', 'updated_at']].head(10))
    
    return df


if __name__ == "__main__":
    main()
