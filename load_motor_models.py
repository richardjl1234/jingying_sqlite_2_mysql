"""
Load motor models from Excel file to MySQL database.
This program reads model_code.xlsx and uploads the data to the motor_models table.
"""

import os
import pandas as pd
from datetime import datetime


def main():
    """
    Main function to load motor model data from Excel to MySQL.
    """
    # File configuration
    excel_file = "model_code.xlsx"
    sheet_name = 0  # First sheet by default
    
    try:
        # Step 1: Open the Excel file and read the data
        print(f"Reading Excel file: {excel_file}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Step 2: Rename columns to match database schema
        # 型号编码 -> model_code
        # 型号 -> name
        df = df.rename(columns={
            '型号编码': 'model_code',
            '型号': 'name'
        })
        print(f"Renamed columns to: model_code, name")
        
        # Step 3: Add additional required columns
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['description'] = ''
        df['created_at'] = current_time
        df['updated_at'] = current_time
        
        # Display the results
        print("\nDataFrame to be uploaded:")
        print(df.head(10))
        
        # Step 4: Upload to MySQL database
        table_name = "motor_models"
        
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
        
        return df
        
    except Exception as e:
        print(f"Error processing file: {e}")
        raise


if __name__ == "__main__":
    main()
