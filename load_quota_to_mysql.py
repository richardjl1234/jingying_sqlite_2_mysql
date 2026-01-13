"""
Load quota data from SQLite database, calculate obsolete_date for each record,
and export the results to MySQL database.

This program:
1. Reads quota data from SQLite database (quota table)
2. Calculates obsolete_date for each record based on effective dates
3. Maps data to MySQL quotas table using code dictionaries
4. Loads the processed data to MySQL database
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from sql_util import sqlite_sql, mysql_sql


def calculate_obsolete_date(group):
    """
    Calculate obsolete_date for records within a group.
    
    For each group (类别1, 类别2, 型号, 加工工序):
    - First record: obsolete_date = next_record.effected_from - 1 day
    - Last record: obsolete_date = 99991231
    - Single record group: obsolete_date = 99991231
    
    Args:
        group: DataFrame subset for a specific group
        
    Returns:
        DataFrame with obsolete_date column added
    """
    # Sort by effected_from within the group
    group = group.sort_values('effected_from').reset_index(drop=True)
    
    # Initialize obsolete_date column
    group['obsolete_date'] = None
    
    if len(group) == 1:
        # Single record in group: obsolete_date = 99991231
        group.loc[0, 'obsolete_date'] = '99991231'
    else:
        # Multiple records in group
        for i in range(len(group) - 1):
            # Get next record's effected_from
            next_effected_from = group.loc[i + 1, 'effected_from']
            
            # Convert to datetime, subtract 1 day
            if isinstance(next_effected_from, str):
                next_date = datetime.strptime(str(next_effected_from), '%Y%m%d')
            else:
                next_date = pd.to_datetime(next_effected_from)
            
            # Subtract 1 day and format back to YYYYMMDD
            obsolete_date = next_date - timedelta(days=1)
            group.loc[i, 'obsolete_date'] = obsolete_date.strftime('%Y%m%d')
        
        # Last record: obsolete_date = 99991231
        group.loc[len(group) - 1, 'obsolete_date'] = '99991231'
    
    return group


def get_quota_with_obsolete_date():
    """
    Read quota data from SQLite database and calculate obsolete_date for each record.
    
    This function:
    1. Reads all columns from the quota table including '代码'
    2. Groups data by (类别1, 类别2, 型号, 加工工序)
    3. Calculates obsolete_date for each record within each group
    4. Returns the processed DataFrame sorted by 代码 to maintain original sequence
    
    Returns:
        pandas.DataFrame: Processed quota data with obsolete_date column
                          Sorted by 代码 to maintain original SQLite sequence
    """
    # Build SQL query to select all columns from quota table including 代码
    query = """
    SELECT 代码, 类别1, 类别2, 型号, 加工工序, 定额, effected_from 
    FROM quota 
    ORDER BY 代码
    """
    
    print("Reading quota data from SQLite database...")
    df = sqlite_sql(query)
    
    print(f"Retrieved {len(df)} records from quota table")
    print(f"Columns: {df.columns.tolist()}")
    
    # Check if we have any data
    if df.empty:
        print("No data found in quota table")
        return df
    
    # Print sample data
    print("\nSample data (first 5 records):")
    print(df.head())
    
    # Group by (类别1, 类别2, 型号, 加工工序) and calculate obsolete_date
    print("\nCalculating obsolete_date for each record...")
    
    group_columns = ['类别1', '类别2', '型号', '加工工序']
    
    # Apply calculate_obsolete_date to each group
    # Note: include_groups=True (default) keeps grouping columns in the result
    df_with_obsolete = df.groupby(group_columns, group_keys=False).apply(calculate_obsolete_date)
    
    # Reset index if it was added by groupby
    if 'level_0' in df_with_obsolete.columns:
        df_with_obsolete = df_with_obsolete.drop(columns=['level_0'])
    if 'index' in df_with_obsolete.columns:
        df_with_obsolete = df_with_obsolete.drop(columns=['index'])
    
    # Reset the index (groupby creates a multi-index)
    df_with_obsolete = df_with_obsolete.reset_index(drop=True)
    
    # Sort by the 代码 column to maintain original SQLite sequence
    df_with_obsolete = df_with_obsolete.sort_values(by='代码').reset_index(drop=True)
    
    print(f"Processed {len(df_with_obsolete)} records with obsolete_date")
    
    # Print sample of processed data
    print("\nSample processed data (first 10 records):")
    print(df_with_obsolete.head(10))
    
    return df_with_obsolete


def get_cat1_dict():
    """
    Get category 1 code dictionary from MySQL database.
    
    Returns:
        dict: Mapping of category 1 names to codes {name: cat1_code}
        
    Raises:
        ValueError: If duplicate names found in the table
    """
    print("Fetching category 1 codes from MySQL...")
    query = "SELECT cat1_code, name FROM process_cat1"
    df = mysql_sql(query)
    
    if df.empty:
        raise ValueError("No data found in process_cat1 table")
    
    # Check for duplicate names
    duplicate_names = df[df.duplicated(subset=['name'], keep=False)]['name'].unique()
    if len(duplicate_names) > 0:
        raise ValueError(f"Duplicate names found in process_cat1 table: {duplicate_names.tolist()}")
    
    # Create dictionary {name: cat1_code}
    cat1_dict = dict(zip(df['name'], df['cat1_code']))
    
    print(f"Loaded {len(cat1_dict)} category 1 codes")
    return cat1_dict


def get_cat2_dict():
    """
    Get category 2 code dictionary from MySQL database.
    
    Returns:
        dict: Mapping of category 2 names to codes {name: cat2_code}
        
    Raises:
        ValueError: If duplicate names found in the table
    """
    print("Fetching category 2 codes from MySQL...")
    query = "SELECT cat2_code, name FROM process_cat2"
    df = mysql_sql(query)
    
    if df.empty:
        raise ValueError("No data found in process_cat2 table")
    
    # Check for duplicate names
    duplicate_names = df[df.duplicated(subset=['name'], keep=False)]['name'].unique()
    if len(duplicate_names) > 0:
        raise ValueError(f"Duplicate names found in process_cat2 table: {duplicate_names.tolist()}")
    
    # Create dictionary {name: cat2_code}
    cat2_dict = dict(zip(df['name'], df['cat2_code']))
    
    print(f"Loaded {len(cat2_dict)} category 2 codes")
    return cat2_dict


def get_model_dict():
    """
    Get motor model code dictionary from MySQL database.
    
    Returns:
        dict: Mapping of model names to codes {name: model_code}
        
    Raises:
        ValueError: If duplicate names found in the table
    """
    print("Fetching motor model codes from MySQL...")
    query = "SELECT model_code, name FROM motor_models"
    df = mysql_sql(query)
    
    if df.empty:
        raise ValueError("No data found in motor_models table")
    
    # Check for duplicate names
    duplicate_names = df[df.duplicated(subset=['name'], keep=False)]['name'].unique()
    if len(duplicate_names) > 0:
        raise ValueError(f"Duplicate names found in motor_models table: {duplicate_names.tolist()}")
    
    # Create dictionary {name: model_code}
    model_dict = dict(zip(df['name'], df['model_code']))
    
    print(f"Loaded {len(model_dict)} motor model codes")
    return model_dict


def get_process_dict():
    """
    Get process code dictionary from MySQL database.
    
    Returns:
        dict: Mapping of process names to codes {name: process_code}
        
    Raises:
        ValueError: If duplicate names found in the table
    """
    print("Fetching process codes from MySQL...")
    query = "SELECT process_code, name FROM processes"
    df = mysql_sql(query)
    
    if df.empty:
        raise ValueError("No data found in processes table")
    
    # Check for duplicate names
    duplicate_names = df[df.duplicated(subset=['name'], keep=False)]['name'].unique()
    if len(duplicate_names) > 0:
        raise ValueError(f"Duplicate names found in processes table: {duplicate_names.tolist()}")
    
    # Create dictionary {name: process_code}
    process_dict = dict(zip(df['name'], df['process_code']))
    
    print(f"Loaded {len(process_dict)} process codes")
    return process_dict


def map_dataframe_to_quotas(df, cat1_dict, cat2_dict, model_dict, process_dict):
    """
    Map quota dataframe to MySQL quotas table format.
    
    Note: The '代码' column is used for sorting to maintain original SQLite sequence
    but is NOT loaded to MySQL (discarded after sorting).
    
    Column mapping:
    - 类别1 -> cat1_code (using cat1_dict)
    - 类别2 -> cat2_code (using cat2_dict)
    - 型号 -> model_code (using model_dict)
    - 加工工序 -> process_code (using process_dict)
    - 定额 -> unit_price (direct)
    - effected_from -> effective_date (direct)
    - obsolete_date -> obsolete_date (direct)
    - 1 -> created_by (constant)
    - current_timestamp -> created_at (generated)
    
    Args:
        df: Source DataFrame with columns [代码, 类别1, 类别2, 型号, 加工工序, 定额, effected_from, obsolete_date]
        cat1_dict: Dictionary mapping category 1 names to codes
        cat2_dict: Dictionary mapping category 2 names to codes
        model_dict: Dictionary mapping model names to codes
        process_dict: Dictionary mapping process names to codes
        
    Returns:
        pandas.DataFrame: Mapped data ready for MySQL insertion (without 代码 column)
        
    Raises:
        ValueError: If any mapping key is not found in the dictionary
    """
    print("\nMapping dataframe to MySQL quotas schema...")
    
    # Track missing keys for error reporting
    missing_cat1 = set()
    missing_cat2 = set()
    missing_model = set()
    missing_process = set()
    
    # Create new dataframe with MySQL column names
    quotas_df = pd.DataFrame()
    
    # Map 类别1 to cat1_code
    quotas_df['cat1_code'] = df['类别1'].apply(
        lambda x: cat1_dict.get(x) if x not in missing_cat1 else None
    )
    for val in df['类别1']:
        if val not in cat1_dict:
            missing_cat1.add(val)
    
    # Map 类别2 to cat2_code
    quotas_df['cat2_code'] = df['类别2'].apply(
        lambda x: cat2_dict.get(x) if x not in missing_cat2 else None
    )
    for val in df['类别2']:
        if val not in cat2_dict:
            missing_cat2.add(val)
    
    # Map 型号 to model_code
    quotas_df['model_code'] = df['型号'].apply(
        lambda x: model_dict.get(x) if x not in missing_model else None
    )
    for val in df['型号']:
        if val not in model_dict:
            missing_model.add(val)
    
    # Map 加工工序 to process_code
    quotas_df['process_code'] = df['加工工序'].apply(
        lambda x: process_dict.get(x) if x not in missing_process else None
    )
    for val in df['加工工序']:
        if val not in process_dict:
            missing_process.add(val)
    
    # Map other columns directly
    quotas_df['unit_price'] = df['定额']
    quotas_df['effective_date'] = df['effected_from'].astype(str)
    quotas_df['obsolete_date'] = df['obsolete_date'].astype(str)
    quotas_df['created_by'] = 1
    
    # Generate created_at timestamp
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    quotas_df['created_at'] = current_time
    
    # Check for missing keys and raise error if any
    errors = []
    if missing_cat1:
        errors.append(f"Missing cat1 codes for values: {sorted(missing_cat1)}")
    if missing_cat2:
        errors.append(f"Missing cat2 codes for values: {sorted(missing_cat2)}")
    if missing_model:
        errors.append(f"Missing model codes for values: {sorted(missing_model)}")
    if missing_process:
        errors.append(f"Missing process codes for values: {sorted(missing_process)}")
    
    if errors:
        raise ValueError("Mapping errors:\n" + "\n".join(errors))
    
    print(f"Mapped {len(quotas_df)} records to quotas schema")
    print(f"Output columns: {quotas_df.columns.tolist()}")
    
    # Print sample of mapped data
    print("\nSample mapped data (first 5 records):")
    print(quotas_df.head())
    
    return quotas_df


def load_to_mysql(quotas_df):
    """
    Load quotas DataFrame to MySQL database.
    
    Args:
        quotas_df: DataFrame with MySQL quotas table schema
        
    Returns:
        int: Number of records successfully loaded
    """
    table_name = "quotas"
    
    print(f"\nLoading data to MySQL table: {table_name}")
    
    # Get MySQL database URL from environment variable
    MYSQL_DB_URL = os.environ.get("MYSQL_DB_URL")
    
    if not MYSQL_DB_URL:
        raise ValueError("MYSQL_DB_URL environment variable not set.")
    
    try:
        from sqlalchemy import create_engine
        engine = create_engine(MYSQL_DB_URL)
        
        # Bulk insert with chunksize
        quotas_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=100
        )
        
        print(f"Successfully loaded {len(quotas_df)} records to MySQL table: {table_name}")
        return len(quotas_df)
        
    except Exception as e:
        raise ValueError(f"MySQL insert failed: {e}")


def main():
    """
    Main function to load quota data, calculate obsolete_date, map to MySQL schema, and load to database.
    """
    print("=" * 60)
    print("Load Quota to MySQL - Quota Processing and Loading Program")
    print("=" * 60)
    
    # Step 1: Get quota data with obsolete_date from SQLite
    print("\n[Step 1] Reading and processing quota data from SQLite...")
    df = get_quota_with_obsolete_date()
    
    if df.empty:
        print("No data to process. Exiting.")
        return
    
    # Step 2: Get code dictionaries from MySQL
    print("\n[Step 2] Fetching code dictionaries from MySQL...")
    
    try:
        cat1_dict = get_cat1_dict()
        cat2_dict = get_cat2_dict()
        model_dict = get_model_dict()
        process_dict = get_process_dict()
    except ValueError as e:
        print(f"Error fetching dictionaries: {e}")
        raise
    
    # Step 3: Map dataframe to MySQL quotas schema
    print("\n[Step 3] Mapping dataframe to MySQL quotas schema...")
    
    try:
        quotas_df = map_dataframe_to_quotas(df, cat1_dict, cat2_dict, model_dict, process_dict)
    except ValueError as e:
        print(f"Error mapping data: {e}")
        raise
    
    # Step 4: Load to MySQL database
    print("\n[Step 4] Loading data to MySQL database...")
    
    try:
        loaded_count = load_to_mysql(quotas_df)
    except ValueError as e:
        print(f"Error loading to MySQL: {e}")
        raise
    
    # Display summary statistics
    print(f"\nSummary:")
    print(f"- Total records processed: {len(df)}")
    print(f"- Records loaded to MySQL: {loaded_count}")
    print(f"- Unique groups: {df.groupby(['类别1', '类别2', '型号', '加工工序']).ngroups}")
    print(f"- Records with '99991231' obsolete_date: {len(df[df['obsolete_date'] == '99991231'])}")
    
    print(f"\nProcessing complete!")
    
    return quotas_df


if __name__ == "__main__":
    main()
