# Load column_seq to MySQL - Implementation Plan

## Overview
Modify `load_quota_to_mysql.py` to also load the `column_seq` table from SQLite to MySQL, mapping category/process names to their corresponding codes.

## Column Mapping

| SQLite Column | MySQL Column | Mapping Dictionary |
|--------------|--------------|-------------------|
| 类别1 | cat1_code | cat1_dict |
| 类别2 | cat2_code | cat2_dict |
| 加工工序 | process_code | process_dict |
| seq | seq | direct |

## Implementation Steps

### Step 1: Add function to check if column_seq table exists in MySQL
```python
def check_column_seq_table_exists():
    """
    Check if column_seq table exists in MySQL.
    
    Returns:
        bool: True if table exists, False otherwise
    """
```

### Step 2: Add function to create column_seq table in MySQL
```python
def create_column_seq_table():
    """
    Create column_seq table in MySQL if it doesn't exist.
    
    Table schema:
    - id INT AUTO_INCREMENT PRIMARY KEY
    - cat1_code VARCHAR(50)
    - cat2_code VARCHAR(50)
    - process_code VARCHAR(50)
    - seq INT
    """
```

### Step 3: Add function to read column_seq data from SQLite
```python
def get_column_seq_from_sqlite():
    """
    Read all data from SQLite column_seq table.
    
    Returns:
        pandas.DataFrame: Data with columns [类别1, 类别2, 加工工序, seq]
    """
```

### Step 4: Add function to map column_seq data to MySQL schema
```python
def map_column_seq_to_mysql(df, cat1_dict, cat2_dict, process_dict):
    """
    Map column_seq DataFrame to MySQL column_seq schema.
    
    Column mapping:
    - 类别1 -> cat1_code (using cat1_dict)
    - 类别2 -> cat2_code (using cat2_dict)
    - 加工工序 -> process_code (using process_dict)
    - seq -> seq (direct)
    
    Returns:
        pandas.DataFrame: Mapped data ready for MySQL insertion
    """
```

### Step 5: Add function to load column_seq data to MySQL
```python
def load_column_seq_to_mysql(column_seq_df):
    """
    Load column_seq DataFrame to MySQL database.
    
    Args:
        column_seq_df: DataFrame with MySQL column_seq table schema
    """
```

### Step 6: Integrate into main() function
Add a new step in the main() function after Step 4 (before Excel export):

```python
# Step 6: Load column_seq to MySQL
print("\n[Step 6] Loading column_seq table to MySQL...")

try:
    # Check if table exists
    if not check_column_seq_table_exists():
        print("  Creating column_seq table in MySQL...")
        create_column_seq_table()
    
    # Read from SQLite
    column_seq_df = get_column_seq_from_sqlite()
    
    if not column_seq_df.empty:
        # Map to MySQL schema
        column_seq_mysql_df = map_column_seq_to_mysql(
            column_seq_df, cat1_dict, cat2_dict, process_dict
        )
        # Load to MySQL
        load_column_seq_to_mysql(column_seq_mysql_df)
        print(f"  Loaded {len(column_seq_mysql_df)} records to column_seq table")
    else:
        print("  No data in SQLite column_seq table to load")
except Exception as e:
    print(f"  Warning: Error loading column_seq: {e}")
    # Continue with other operations even if this fails
```

## Error Handling Strategy
- If column_seq table doesn't exist in MySQL, create it
- If mapping fails (missing keys in dictionaries), log warnings but continue
- If MySQL load fails, log warning and continue (non-critical operation)

## Files to Modify
- `load_quota_to_mysql.py`
