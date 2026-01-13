"""
Load quota data from SQLite database, calculate obsolete_date for each record,
and export the results to an HTML file.

This program:
1. Reads quota data from SQLite database (quota table)
2. Calculates obsolete_date for each record based on effective dates
3. Exports the processed data to HTML format
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from sql_util import sqlite_sql


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
    1. Reads all columns except '代码' from the quota table
    2. Groups data by (类别1, 类别2, 型号, 加工工序)
    3. Calculates obsolete_date for each record within each group
    4. Returns the processed DataFrame sorted by the grouping columns
    
    Returns:
        pandas.DataFrame: Processed quota data with obsolete_date column
                          Sorted by 类别1, 类别2, 型号, 加工工序
    """
    # Build SQL query to select all columns except 代码 from quota table
    query = """
    SELECT 类别1, 类别2, 型号, 加工工序, 定额, effected_from 
    FROM quota 
    ORDER BY 类别1, 类别2, 型号, 加工工序, effected_from
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
    
    # Sort by the grouping columns to ensure consistent ordering
    df_with_obsolete = df_with_obsolete.sort_values(by=group_columns).reset_index(drop=True)
    
    print(f"Processed {len(df_with_obsolete)} records with obsolete_date")
    
    # Print sample of processed data
    print("\nSample processed data (first 10 records):")
    print(df_with_obsolete.head(10))
    
    return df_with_obsolete


def main():
    """
    Main function to load quota data, calculate obsolete_date, and save to HTML.
    """
    print("=" * 60)
    print("Load Quota to MySQL - Quota Processing Program")
    print("=" * 60)
    
    # Step 1: Get quota data with obsolete_date
    print("\n[Step 1] Reading and processing quota data...")
    df = get_quota_with_obsolete_date()
    
    if df.empty:
        print("No data to save. Exiting.")
        return
    
    # Step 2: Save to HTML file
    print("\n[Step 2] Saving processed data to HTML file...")
    
    output_file = "quota_with_obsolete_date.html"
    
    try:
        # Save to HTML with proper formatting
        df.to_html(output_file, index=False, encoding='utf-8-sig')
        
        print(f"Successfully saved {len(df)} records to HTML file: {output_file}")
        
        # Display column information
        print(f"\nOutput columns: {df.columns.tolist()}")
        
        # Display summary statistics
        print(f"\nSummary:")
        print(f"- Total records: {len(df)}")
        print(f"- Unique groups: {df.groupby(['类别1', '类别2', '型号', '加工工序']).ngroups}")
        print(f"- Records with '99991231' obsolete_date: {len(df[df['obsolete_date'] == '99991231'])}")
        
        print(f"\nProcessing complete! Output saved to: {output_file}")
        
    except Exception as e:
        print(f"Error saving HTML file: {e}")
        raise
    
    return df


if __name__ == "__main__":
    main()
