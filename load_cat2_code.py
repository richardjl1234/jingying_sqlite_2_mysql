"""
Category2 code generation program for Excel file processing.
This program reads an Excel file, generates category2 codes based on Chinese pinyin,
and uploads the results to MySQL database.
"""

import os
import pandas as pd
from pypinyin import pinyin, Style
from datetime import datetime


def is_chinese_punctuation(char):
    """Check if character is Chinese punctuation."""
    chinese_punctuation = set('，。！？；：""''【】（）《》〈〉、…—·')
    return char in chinese_punctuation


def process_encode(input_string: str) -> str:
    """
    Generate a code for the input category2 string.
    
    Processing logic:
    1. Ignore all punctuation including ':.()' (Chinese and English punctuation)
    2. For English characters, output as-is
    3. For digit characters (0-9), output as-is
    4. For Chinese characters, get the first letter of pinyin
    
    Args:
        input_string: The input category2 name string
        
    Returns:
        The generated character sequence code
        
    Example:
        "2人校正" -> "2RXZ"
        "Y2后装" -> "Y2HZ"
    """
    if not input_string or not isinstance(input_string, str):
        return ""
    
    result_parts = []
    
    for char in input_string:
        # Skip punctuation including ':.()' (Chinese punctuation and half-width punctuation)
        if char in '，。！？；：""''【】（）《》〈〉、…—·:.' or char in '()' or char.isspace() or is_chinese_punctuation(char):
            continue
        # Check if character is Chinese (Unicode range for CJK characters)
        elif '\u4e00' <= char <= '\u9fff':
            # Get pinyin first letter for Chinese characters
            pinyin_list = pinyin(char, style=Style.FIRST_LETTER)
            if pinyin_list:
                result_parts.append(pinyin_list[0][0].upper())
        elif char.isalpha():  # English letters - keep as-is
            result_parts.append(char)
        elif char.isdigit():  # Digits - keep as-is
            result_parts.append(char)
        # Other characters are skipped
    
    return ''.join(result_parts)


def main():
    """
    Main function to process the Excel file, generate category2 codes,
    and upload to MySQL database.
    """
    # File configuration
    excel_file = "../quota/quota_distinct_values.xlsx"
    sheet_name = "类别2"
    
    try:
        # Step 1: Open the Excel file and read the specific sheet
        print(f"Reading Excel file: {excel_file}, Sheet: {sheet_name}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Step 2: Rename the column 类别2 -> name
        df = df.rename(columns={'类别2': 'name'})
        print(f"Renamed column to: name")
        
        # Step 3: Apply the process_encode function to generate cat2_code
        df['cat2_code'] = df['name'].apply(process_encode)
        
        # Display the results
        print("\nDataFrame with generated codes:")
        print(df[['name', 'cat2_code']].head(10))
        
        # Step 4: Add additional columns
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['description'] = ''
        df['created_at'] = current_time
        df['updated_at'] = current_time
        
        # Step 5: Check for duplicate cat2_code values
        duplicates = df[df['cat2_code'].duplicated(keep=False)]
        if not duplicates.empty:
            print("\nDuplicate cat2_code found:")
            print(duplicates[['name', 'cat2_code']].to_string())
            # Process duplicates: for each group of duplicate cat2_code values,
            # keep the first occurrence unchanged, and for subsequent occurrences
            # add suffix _1, _2, etc. starting from 1.
            # Identify duplicate groups using pandas groupby on the original codes
            duplicate_groups = {}
            # Get unique duplicate codes
            duplicate_codes = df['cat2_code'][df['cat2_code'].duplicated()].unique()
            
            for code in duplicate_codes:
                # Get all indices for this duplicate code
                indices = df[df['cat2_code'] == code].index.tolist()
                duplicate_groups[code] = indices
            
            # Process each duplicate group
            for code, indices in duplicate_groups.items():
                if len(indices) > 1:
                    # Sort indices to maintain original order
                    indices.sort()
                    # Keep first occurrence unchanged
                    # For subsequent occurrences, add alphabetical suffix _a, _b, _c...
                    for i, idx in enumerate(indices[1:], start=0):
                        suffix = chr(ord('a') + i)  # a, b, c...
                        df.at[idx, 'cat2_code'] = f"{code}{suffix}"
            
            print("\nAfter processing duplicate cat2_code:")
            # Show all rows that were originally duplicates
            print(df.loc[duplicates.index, ['name', 'cat2_code']])
            # df.to_html("processed_quota_distinct_values.html", index=False)

        
        # print("\nNo duplicate category2 codes found. Proceeding with database upload...")
        
        # Step 6: Save the result to Excel (optional)
        output_file = "processed_category2_values.xlsx"
        df.to_excel(output_file, index=False)
        print(f"\nProcessed DataFrame saved to: {output_file}")
        
        # Step 7: Bulk upload the DataFrame to MySQL database (existing table: process_cat2)
        table_name = "process_cat2"
        
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
