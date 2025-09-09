import sqlite3
import pandas as pd
from pathlib import Path

def query_burnup_db(db_path="combined_yearly_data.db"):
    """
    Simple utility to query the burnup database
    
    Args:
        db_path (str): Path to SQLite database file
    """
    if not Path(db_path).exists():
        print(f"Database file '{db_path}' not found!")
        return None
    
    conn = sqlite3.connect(db_path)
    
    try:
        print(f"Connected to database: {db_path}")
        print("\nDatabase schema:")
        
        # Get table info
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(burnup_data)")
        columns = cursor.fetchall()
        
        print("Columns in burnup_data table:")
        for i, (cid, name, dtype, notnull, default, pk) in enumerate(columns, 1):
            print(f"  {i:2d}. {name} ({dtype})")
        
        # Get basic statistics
        cursor.execute("SELECT COUNT(*) FROM burnup_data")
        total_rows = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT sheet_name) FROM burnup_data")
        sheet_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(start_year), MAX(end_year) FROM burnup_data")
        year_range = cursor.fetchone()
        
        print(f"\nDatabase statistics:")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Number of sheets: {sheet_count}")
        print(f"  Year range: {year_range[0]}-{year_range[1]}")
        
        # Show available sheets
        cursor.execute("SELECT sheet_name, COUNT(*) as row_count FROM burnup_data GROUP BY sheet_name ORDER BY start_year")
        sheets = cursor.fetchall()
        
        print(f"\nSheets in database:")
        for sheet, count in sheets:
            print(f"  {sheet}: {count:,} rows")
        
        return conn
        
    except Exception as e:
        print(f"Error querying database: {e}")
        conn.close()
        return None

def run_custom_query(query, db_path="combined_yearly_data.db"):
    """
    Run a custom SQL query on the burnup database
    
    Args:
        query (str): SQL query to execute
        db_path (str): Path to SQLite database file
        
    Returns:
        pd.DataFrame: Query results
    """
    try:
        conn = sqlite3.connect(db_path)
        result = pd.read_sql_query(query, conn)
        conn.close()
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def example_queries(db_path="combined_yearly_data.db"):
    """
    Run some example queries on the burnup database
    """
    print("Running example queries...\n")
    
    queries = [
        ("Recent years (2020+)", 
         "SELECT sheet_name, COUNT(*) as rows FROM burnup_data WHERE start_year >= 2020 GROUP BY sheet_name ORDER BY start_year"),
        
        ("Power data summary", 
         "SELECT sheet_name, AVG(CAST([Power (kw)] AS REAL)) as avg_power FROM burnup_data WHERE [Power (kw)] IS NOT NULL AND [Power (kw)] != '' GROUP BY sheet_name ORDER BY start_year LIMIT 5"),
        
        ("Data by year", 
         "SELECT start_year, COUNT(*) as total_records FROM burnup_data GROUP BY start_year ORDER BY start_year"),
        
        ("Sample data from 2023", 
         "SELECT Date, [Power (kw)], [Total Energy\n(average)] FROM burnup_data WHERE sheet_name = '2023-2024' AND Date IS NOT NULL LIMIT 5")
    ]
    
    for title, query in queries:
        print(f"{title}:")
        try:
            result = run_custom_query(query, db_path)
            if result is not None:
                print(result.to_string(index=False))
            else:
                print("  Query failed")
        except Exception as e:
            print(f"  Error: {e}")
        print()

if __name__ == "__main__":
    # Query the database
    conn = query_burnup_db()
    
    if conn:
        conn.close()
        print("\n" + "="*50)
        example_queries()
    else:
        print("Could not connect to database")