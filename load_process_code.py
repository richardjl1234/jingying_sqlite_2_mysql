"""
Process code upload program for Excel file processing.
This program reads an Excel file and uploads the data to MySQL database.
"""

import os
import pandas as pd
from datetime import datetime


def main():
    """
    Main function to read the Excel file and upload data to MySQL database.
    """
    # File configuration
    excel_file = "定额型号类别编码_updated.xlsx"
    sheet_name = "加工工序"
    table_name = "processes"
    
    try:
        # Step 1: Read the Excel file and upload to MySQL
        print(f"Reading Excel file: {excel_file}, Sheet: {sheet_name}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Display sample data
        print("\nDataFrame sample:")
        print(df.head(10))
        
        # Get MySQL database URL from environment variable
        MYSQL_DB_URL = os.environ.get("MYSQL_DB_URL")
        
        if not MYSQL_DB_URL:
            print("Error: MYSQL_DB_URL environment variable not set.")
            return
        
        # Bulk upload the DataFrame to MySQL database
        print(f"\nUploading DataFrame to MySQL table: {table_name}")
        
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
        
        return df
        
    except Exception as e:
        print(f"Error processing file: {e}")
        raise


if __name__ == "__main__":
    main()
