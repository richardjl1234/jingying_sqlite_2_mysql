"""Database utilities for executing SQL on SQLite and MySQL databases."""

import os
import sqlite3
import pymysql
import pandas as pd
from sqlalchemy import create_engine, text
from functools import wraps
import time


# Retry configuration
RETRIES = 5


def retry(max_retries: int = RETRIES, delay: float = 1.0):
    """
    Retry decorator for functions that may fail transiently.
    
    Args:
        max_retries: Maximum number of retry attempts (default: RETRIES)
        delay: Delay in seconds between retries (default: 1.0)
        
    Returns:
        Decorated function that will retry on failure.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):  # +1 to include the initial attempt
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        print(f"All {max_retries + 1} attempts failed. Last error: {e}")
                        raise
            raise last_exception
        return wrapper
    return decorator


# Read database URLs from environment variables
SQLITE_DB_URL = os.environ.get("SQLITE_DB_URL", "sqlite:////home/richard/shared/jianglei/payroll/payroll_database.db")
MYSQL_DB_URL = os.environ.get("MYSQL_DB_URL", "mysql+pymysql://REMOVED_CREDENTIALS@sh-cynosdbmysql-grp-icsnw792.sql.tencentcdb.com:21706/payroll_test")


def sqlite_sql(sq: str):
    """
    Execute an SQL query on the SQLite database.
    
    Args:
        sq: The SQL query string to execute.
        
    Returns:
        pandas.DataFrame: If the query is a SELECT, returns results as DataFrame.
        None: If the query is an UPDATE/INSERT/DELETE.
    """
    engine = create_engine(SQLITE_DB_URL)
    with engine.connect() as connection:
        if sq.strip().upper().startswith("SELECT"):
            result = connection.execute(text(sq))
            columns = result.keys()
            data = result.fetchall()
            return pd.DataFrame(data, columns=columns)
        else:
            connection.execute(text(sq))
            connection.commit()
            return None


@retry(max_retries=RETRIES)
def mysql_sql(sq: str):
    """
    Execute an SQL query on the MySQL database.
    
    Args:
        sq: The SQL query string to execute.
        
    Returns:
        pandas.DataFrame: If the query is a SELECT, returns results as DataFrame.
        None: If the query is an UPDATE/INSERT/DELETE.
    """
    engine = create_engine(MYSQL_DB_URL)
    with engine.connect() as connection:
        if sq.strip().upper().startswith("SELECT"):
            result = connection.execute(text(sq))
            columns = result.keys()
            data = result.fetchall()
            return pd.DataFrame(data, columns=columns)
        else:
            connection.execute(text(sq))
            connection.commit()
            return None


if __name__ == "__main__":
    # Example usage
    # sqlite_sql("UPDATE employees SET salary = 5000 WHERE id = 1")
    # mysql_sql("UPDATE employees SET salary = 6000 WHERE id = 1")
    # df = sqlite_sql("SELECT * FROM payroll_details")
    # print(df)   
    df1 = mysql_sql("SELECT * FROM users")
    print(df1)
