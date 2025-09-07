#!/usr/bin/env python3
"""
Complete Burnup Data Processing Pipeline - FIXED VERSION

This script consolidates all functionality to process nuclear reactor burnup data from Excel files
into a comprehensive SQLite database with calculated columns and proper chronological ordering.

FIXES:
- Proper column alignment across different sheet types
- Standardized column mapping
- Removes numeric artifact columns
- Better error handling

Usage:
    python process_burnup_complete_fixed.py

Requirements:
    - Burnup.xlsx (main Excel file)
    - BURN UP FROM 2006-1992.xlsx (secondary Excel file)
    - pandas, openpyxl, sqlite3
"""

import pandas as pd
import sqlite3
import re
from pathlib import Path
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BurnupProcessor:
    def __init__(self, main_excel="Burnup.xlsx", secondary_excel="BURNUPROM2006-1992.xlsx", 
                 output_db="combined_yearly_data.db"):
        self.main_excel = main_excel
        self.secondary_excel = secondary_excel 
        self.output_db = output_db
        self.all_data = []
        
        # Define standard column schema to ensure consistency
        self.standard_columns = [
            'start_year', 'end_year', 'sheet_name', 'original_row', 'source_file',
            'Date', 'Checkout\nStart', 'Purpose', 'Power (kw)', 'Time @', 'Time @_1',
            'Time D', 'Time D_1', 'Shutdown\nTime', 'Delta Time\n(minutes)', 
            'Power\nDuration', 'Power\nCycles', 'Checkout\nStart_1', 'Console\nDuration',
            'Delta Energy\n(average)', 'Delta Energy\n(exponential)', 'Stable\nEnergy',
            'Total Energy\n(average)', 'Total Energy\n(exponential)', 'startup_count'
        ]
        
    def print_header(self, title):
        """Print formatted section header"""
        print("\n" + "=" * 80)
        print(f" {title}")
        print("=" * 80)
        
    def print_step(self, step_num, title):
        """Print formatted step header"""
        print(f"\n--- Step {step_num}: {title} ---")
        
    def normalize_dataframe_columns(self, df, sheet_name, source):
        """Normalize DataFrame to standard column structure"""
        # Start with standard metadata columns
        normalized_df = pd.DataFrame()
        normalized_df['start_year'] = df.get('start_year', None)
        normalized_df['end_year'] = df.get('end_year', None)
        normalized_df['sheet_name'] = df.get('sheet_name', sheet_name)
        normalized_df['original_row'] = df.get('original_row', range(len(df)))
        normalized_df['source_file'] = source
        
        # Map data columns with flexible matching for both file formats
        column_mapping = {
            'Date': ['Date'],
            'Checkout\nStart': ['Checkout\nStart', 'Checkout Start', 'Start'],
            'Purpose': ['Purpose'],
            'Power (kw)': ['Power (kw)', 'Power(kw)', 'Power', 'Pwr'],
            'Time @': ['Time @', 'At'],
            'Time @_1': ['Time @_1', 'Time @ _1', '@'],
            'Time D': ['Time D', 'Down'],
            'Time D_1': ['Time D_1', 'D'],
            'Shutdown\nTime': ['Shutdown\nTime', 'Shutdown Time', 'Shutdown'],
            'Delta Time\n(minutes)': ['Delta Time\n(minutes)', 'Delta Time (minutes)', 'Delta Time(minutes)', 'Delta Time'],
            'Power\nDuration': ['Power\nDuration', 'Power Duration', 'Duration'],
            'Power\nCycles': ['Power\nCycles', 'Power Cycles', 'Cycles'],
            'Checkout\nStart_1': ['Checkout\nStart_1', 'Checkout Start_1'],
            'Console\nDuration': ['Console\nDuration', 'Console Duration'],
            'Delta Energy\n(average)': ['Delta Energy\n(average)', 'Delta Energy (average)', 'Delta Energy(average)', 'Delta Energy (Average)'],
            'Delta Energy\n(exponential)': ['Delta Energy\n(exponential)', 'Delta Energy (exponential)', 'Delta Energy(exponential)', 'Delta Energy (Exponential)'],
            'Stable\nEnergy': ['Stable\nEnergy', 'Stable Energy'],
            'Total Energy\n(average)': ['Total Energy\n(average)', 'Total Energy (average)', 'Total Energy(average)', 'Total Energy (Average)'],
            'Total Energy\n(exponential)': ['Total Energy\n(exponential)', 'Total Energy (exponential)', 'Total Energy(exponential)', 'Total Energy (Exponential)'],
            'startup_count': ['startup_count', 'Start-up count']
        }
        
        # Map each standard column
        for std_col, possible_names in column_mapping.items():
            mapped = False
            for possible_name in possible_names:
                if possible_name in df.columns:
                    normalized_df[std_col] = df[possible_name]
                    mapped = True
                    break
            
            if not mapped:
                normalized_df[std_col] = None
        
        # Filter out rows that are completely empty (all NaN except metadata)
        data_cols = [col for col in normalized_df.columns if col not in ['start_year', 'end_year', 'sheet_name', 'original_row', 'source_file']]
        normalized_df = normalized_df.dropna(how='all', subset=data_cols)
        
        print(f"    Normalized to {len(normalized_df)} rows with standard {len(normalized_df.columns)} columns")
        return normalized_df
        
    def process_main_excel(self):
        """Process main Burnup.xlsx file (1992-1993, 2006-2026)"""
        self.print_step(1, "Processing Main Excel File (Burnup.xlsx)")
        
        if not Path(self.main_excel).exists():
            raise FileNotFoundError(f"Main Excel file '{self.main_excel}' not found!")
            
        excel_file = pd.ExcelFile(self.main_excel)
        year_pattern = re.compile(r'^\d{4}-\d{4}$')
        yearly_sheets = [sheet for sheet in excel_file.sheet_names if year_pattern.match(sheet)]
        
        print(f"Found {len(yearly_sheets)} yearly sheets in main file")
        
        for i, sheet_name in enumerate(sorted(yearly_sheets)):
            print(f"  Processing {sheet_name}...")
            start_year, end_year = sheet_name.split('-')
            
            try:
                # Check for Start-up count column (2014+ sheets)
                header_df = pd.read_excel(self.main_excel, sheet_name=sheet_name, skiprows=13, nrows=1, header=None)
                raw_headers = [str(col) if pd.notna(col) else f'Unnamed_{idx}' for idx, col in enumerate(header_df.iloc[0])]
                has_startup_count = 'Start-up count' in raw_headers[0] if raw_headers else False
                
                # Read the data
                df = pd.read_excel(self.main_excel, sheet_name=sheet_name, skiprows=14, header=None)
                
                if has_startup_count and len(df.columns) > 0:
                    # Handle 2014+ format with Start-up count
                    startup_count_col = df.iloc[:, 0] if len(df.columns) > 0 else None
                    df = df.iloc[:, 1:]  # Skip first column
                    
                    # Create column names based on expected structure
                    expected_cols = [
                        'Date', 'Checkout\nStart', 'Purpose', 'Power (kw)', 'Time @', 'Time @_1',
                        'Time D', 'Time D_1', 'Shutdown\nTime', 'Delta Time\n(minutes)', 
                        'Power\nDuration', 'Power\nCycles', 'Checkout\nStart_1', 'Console\nDuration',
                        'Delta Energy\n(average)', 'Delta Energy\n(exponential)', 'Stable\nEnergy',
                        'Total Energy\n(average)', 'Total Energy\n(exponential)'
                    ]
                    
                    # Assign column names up to available columns
                    actual_cols = expected_cols[:min(len(expected_cols), len(df.columns))]
                    df = df.iloc[:, :len(actual_cols)]  # Keep only columns we can name
                    df.columns = actual_cols
                    
                    if startup_count_col is not None:
                        df['startup_count'] = startup_count_col
                        
                else:
                    # Handle pre-2014 format
                    expected_cols = [
                        'Date', 'Checkout\nStart', 'Purpose', 'Power (kw)', 'Time @', 'Time @_1',
                        'Time D', 'Time D_1', 'Shutdown\nTime', 'Delta Time\n(minutes)', 
                        'Power\nDuration', 'Power\nCycles', 'Checkout\nStart_1', 'Console\nDuration',
                        'Delta Energy\n(average)', 'Delta Energy\n(exponential)', 'Stable\nEnergy',
                        'Total Energy\n(average)', 'Total Energy\n(exponential)'
                    ]
                    
                    actual_cols = expected_cols[:min(len(expected_cols), len(df.columns))]
                    df = df.iloc[:, :len(actual_cols)]
                    df.columns = actual_cols
                
                # Add metadata
                df['start_year'] = int(start_year)
                df['end_year'] = int(end_year)
                df['sheet_name'] = sheet_name
                df['original_row'] = df.index + 15
                
                # Normalize the DataFrame
                normalized_df = self.normalize_dataframe_columns(df, sheet_name, 'main')
                self.all_data.append(normalized_df)
                
            except Exception as e:
                print(f"    Error: {e}")
                continue
                
        print(f"✓ Processed {len([d for d in self.all_data if d.iloc[0]['source_file'] == 'main'])} sheets from main file")
    
    def process_secondary_excel(self):
        """Process secondary BURN UP FROM 2006-1992.xlsx file"""
        self.print_step(2, "Processing Secondary Excel File (Missing Years)")
        
        if not Path(self.secondary_excel).exists():
            print(f"⚠ Secondary Excel file '{self.secondary_excel}' not found, skipping...")
            return
            
        excel_file = pd.ExcelFile(self.secondary_excel)
        
        def detect_sheet_type(sheet_name):
            if re.match(r'^\d{4}-\d{4}$', sheet_name):
                return 'standard'
            if '1992' in sheet_name and ('FIRST' in sheet_name or 'CORRECTED' in sheet_name):
                return 'special_1992'
            return 'date_range'
        
        processed_sheets = 0
        for sheet_name in excel_file.sheet_names:
            print(f"  Processing {sheet_name}...")
            sheet_type = detect_sheet_type(sheet_name)
            
            try:
                if sheet_type == 'standard':
                    start_year, end_year = map(int, sheet_name.split('-'))
                    df = pd.read_excel(self.secondary_excel, sheet_name=sheet_name)  # Headers at row 0
                    
                elif sheet_type == 'special_1992':
                    start_year = end_year = 1992
                    if 'FIRST' in sheet_name:
                        df = pd.read_excel(self.secondary_excel, sheet_name=sheet_name, skiprows=1)  # Minimal skip
                    else:
                        df = pd.read_excel(self.secondary_excel, sheet_name=sheet_name)  # Headers at row 0
                        
                else:  # date_range
                    # Extract years from sheet name - comprehensive mapping
                    start_year = end_year = 2000  # Default
                    
                    # Direct pattern matching for known sheet names
                    year_mappings = {
                        '03-29-01 to 10-02-00': (2000, 2001),
                        '03-31-1997 to 10-01-96': (1996, 1997), 
                        '03-31-98 to 10-07-97': (1997, 1998),
                        '04-01-1997 to 09-30-1997': (1997, 1997),
                        '04-01-98 to 09-25-98': (1998, 1998),
                        '04-02-01 to 07-18-01': (2001, 2001),
                        '10-01-02 to 03-31-03': (2002, 2003),
                        '10-04-99 to 3-26-00': (1999, 2000),
                        '10-05-01 to 12-07-01': (2001, 2001),
                        '10-1-1998 to 3-31-99': (1998, 1999),
                        '10-1-92 to 4-31-93': (1992, 1993),
                        '9-26-96 to 4-03-96': (1996, 1996),
                        '3-29-1996 to 10-03-1995': (1995, 1996),
                        '9-29-95 to 4-5-95': (1995, 1995),
                        '3-31-95 to 10-04-94': (1994, 1995),
                        '4-12-94 to 9-29-94': (1994, 1994),
                        '3-31-94 to 10-01-93': (1993, 1994),
                        '9-30-93 to 4-5-93': (1993, 1993),
                        '4-02-2002 to 9-27-2002': (2002, 2002),
                        '9-30-99 to 4-3-00': (1999, 2000),
                        '10|01|02-03|31|03': (2002, 2003)
                    }
                    
                    if sheet_name in year_mappings:
                        start_year, end_year = year_mappings[sheet_name]
                    else:
                        # Fallback parsing for unknown patterns
                        if 'to' in sheet_name:
                            parts = sheet_name.split(' to ')
                            if len(parts) == 2:
                                years = []
                                for part in parts:
                                    # Find 4-digit years first
                                    four_digit = re.findall(r'\b(19|20)\d{2}\b', part)
                                    if four_digit:
                                        years.extend([int(y) for y in four_digit])
                                    else:
                                        # Find 2-digit years at end of date strings  
                                        two_digit = re.findall(r'-(\d{2})(?:\D|$)', part)
                                        if two_digit:
                                            for y in two_digit:
                                                year = int(y)
                                                if year >= 92:  # 1992+
                                                    years.append(1900 + year)
                                                else:  # 2000+
                                                    years.append(2000 + year)
                                
                                if years:
                                    start_year = min(years)
                                    end_year = max(years)
                        
                    df = pd.read_excel(self.secondary_excel, sheet_name=sheet_name)  # Headers at row 0
                
                # Add metadata
                df['start_year'] = start_year
                df['end_year'] = end_year
                df['sheet_name'] = sheet_name
                df['original_row'] = df.index + 15
                
                # Normalize the DataFrame
                normalized_df = self.normalize_dataframe_columns(df, sheet_name, 'secondary')
                self.all_data.append(normalized_df)
                processed_sheets += 1
                
            except Exception as e:
                print(f"    Error: {e}")
                continue
                
        print(f"✓ Processed {processed_sheets} sheets from secondary file")
    
    def create_database(self):
        """Combine all data and create SQLite database"""
        self.print_step(3, "Creating Database")
        
        if not self.all_data:
            raise ValueError("No data to process! Check Excel files.")
            
        # Combine all DataFrames - they should all have the same columns now
        print("Combining data from all normalized sheets...")
        final_df = pd.concat(self.all_data, ignore_index=True)
        
        print(f"Combined dataset: {len(final_df):,} rows, {len(final_df.columns)} columns")
        
        # Create SQLite database
        conn = sqlite3.connect(self.output_db)
        
        try:
            # Fix data types for SQLite
            df_for_db = final_df.copy()
            precision_fixes = 0
            
            print("Fixing data types and precision...")
            for col in df_for_db.columns:
                col_str = str(col)
                
                if df_for_db[col].dtype == 'object':
                    # Check if datetime
                    sample = df_for_db[col].dropna().head(10)
                    if len(sample) > 0 and any(isinstance(x, (pd.Timestamp, datetime)) for x in sample):
                        df_for_db[col] = df_for_db[col].astype(str)
                        
                elif df_for_db[col].dtype in ['float64', 'float32']:
                    if 'Energy' in col_str or 'Power' in col_str:
                        df_for_db[col] = df_for_db[col].round(6)
                        precision_fixes += 1
                    elif 'Time' in col_str or 'Duration' in col_str or 'minutes' in col_str:
                        df_for_db[col] = df_for_db[col].round(6)
                        precision_fixes += 1
                    else:
                        df_for_db[col] = df_for_db[col].round(8)
                        precision_fixes += 1
            
            print(f"  Fixed precision for {precision_fixes} columns")
            
            # Save to database
            df_for_db.to_sql('burnup_data', conn, if_exists='replace', index=False)
            
            # Create indexes
            cursor = conn.cursor()
            indexes = [
                'CREATE INDEX idx_start_year ON burnup_data (start_year)',
                'CREATE INDEX idx_end_year ON burnup_data (end_year)',
                'CREATE INDEX idx_sheet_name ON burnup_data (sheet_name)',
                'CREATE INDEX idx_year_sheet ON burnup_data (start_year, sheet_name)'
            ]
            
            for idx_sql in indexes:
                cursor.execute(idx_sql)
            
            conn.commit()
            print(f"✓ Database created: {self.output_db}")
            print(f"  Rows: {len(df_for_db):,}")
            print(f"  Columns: {len(df_for_db.columns)}")
            print(f"  Year range: {df_for_db['start_year'].min()}-{df_for_db['end_year'].max()}")
            
        except Exception as e:
            print(f"Error creating database: {e}")
            raise
        finally:
            conn.close()
    
    def add_datetime_column(self):
        """Add datetime_combined column with forward-fill logic"""
        self.print_step(4, "Adding DateTime Column with Forward-Fill")
        
        conn = sqlite3.connect(self.output_db)
        cursor = conn.cursor()
        
        try:
            # Add column
            try:
                cursor.execute('ALTER TABLE burnup_data ADD COLUMN datetime_combined TEXT')
            except Exception:
                pass  # Column may already exist
            
            print("Processing datetime combinations with forward-fill...")
            
            # Get all data ordered by rowid
            query = 'SELECT rowid, Date, `Time @_1` FROM burnup_data ORDER BY rowid'
            df = pd.read_sql_query(query, conn)
            
            last_valid_date = None
            updates = []
            
            for _, row in df.iterrows():
                rowid = row['rowid']
                date = row['Date']
                time = row['Time @_1']
                
                # Update last valid date
                if pd.notna(date) and str(date) not in ['NaT', 'nan', 'None']:
                    last_valid_date = str(date).split(' ')[0]
                
                # Create datetime_combined
                datetime_combined = None
                
                if pd.notna(date) and str(date) not in ['NaT', 'nan', 'None']:
                    date_part = str(date).split(' ')[0]
                elif last_valid_date:
                    date_part = last_valid_date
                else:
                    continue
                
                if pd.notna(time) and str(time).strip():
                    time_str = str(time)
                    if ':' in time_str:
                        time_part = time_str.split('.')[0][:8]
                    else:
                        try:
                            time_val = float(time_str)
                            if 0 <= time_val < 2400:
                                hours = int(time_val / 100)
                                minutes = int(time_val % 100)
                                time_part = f"{hours:02d}:{minutes:02d}:00"
                            else:
                                time_part = "00:00:00"
                        except:
                            time_part = "00:00:00"
                else:
                    time_part = "00:00:00"
                
                datetime_combined = f"{date_part} {time_part}"
                updates.append((datetime_combined, rowid))
            
            # Batch update
            cursor.executemany('UPDATE burnup_data SET datetime_combined = ? WHERE rowid = ?', updates)
            conn.commit()
            
            # Check results
            cursor.execute("SELECT COUNT(*), COUNT(datetime_combined) FROM burnup_data")
            total, with_datetime = cursor.fetchone()
            
            print(f"✓ datetime_combined created for {with_datetime:,}/{total:,} rows ({with_datetime/total*100:.1f}%)")
            
        except Exception as e:
            print(f"Error adding datetime column: {e}")
            raise
        finally:
            conn.close()
    
    def reorder_chronologically(self):
        """Reorder database rows chronologically"""
        self.print_step(5, "Reordering Database Chronologically")
        
        conn = sqlite3.connect(self.output_db)
        cursor = conn.cursor()
        
        try:
            print("Creating chronologically ordered table...")
            
            # Create backup
            cursor.execute('DROP TABLE IF EXISTS burnup_data_backup')
            cursor.execute('CREATE TABLE burnup_data_backup AS SELECT * FROM burnup_data')
            
            # Get table structure
            cursor.execute("PRAGMA table_info(burnup_data)")
            columns_info = cursor.fetchall()
            
            column_defs = []
            for col in columns_info:
                col_name, col_type, not_null, default, pk = col[1], col[2], col[3], col[4], col[5]
                col_def = f'"{col_name}" {col_type}'
                if not_null: col_def += " NOT NULL"
                if default: col_def += f" DEFAULT {default}"
                if pk: col_def += " PRIMARY KEY"
                column_defs.append(col_def)
            
            # Create ordered table
            cursor.execute('DROP TABLE IF EXISTS burnup_data_ordered')
            cursor.execute(f'CREATE TABLE burnup_data_ordered ({", ".join(column_defs)})')
            
            # Insert data in chronological order
            cursor.execute('''
                INSERT INTO burnup_data_ordered 
                SELECT * FROM burnup_data_backup 
                ORDER BY 
                    CASE WHEN datetime_combined IS NOT NULL 
                         THEN datetime(datetime_combined) 
                         ELSE NULL END ASC,
                    start_year ASC, 
                    end_year ASC, 
                    sheet_name ASC, 
                    original_row ASC
            ''')
            
            # Replace original table
            cursor.execute('DROP TABLE burnup_data')
            cursor.execute('ALTER TABLE burnup_data_ordered RENAME TO burnup_data')
            
            # Recreate indexes
            indexes = [
                'CREATE INDEX idx_start_year ON burnup_data (start_year)',
                'CREATE INDEX idx_end_year ON burnup_data (end_year)',
                'CREATE INDEX idx_sheet_name ON burnup_data (sheet_name)',
                'CREATE INDEX idx_datetime_combined ON burnup_data (datetime_combined)',
                'CREATE INDEX idx_year_sheet ON burnup_data (start_year, sheet_name)'
            ]
            
            for idx_sql in indexes:
                cursor.execute(idx_sql)
                
            conn.commit()
            print("✓ Database reordered chronologically")
            
        except Exception as e:
            print(f"Error reordering database: {e}")
            raise
        finally:
            conn.close()
    
    def calculate_power_columns(self):
        """Calculate power per minute columns"""
        self.print_step(6, "Calculating Power Per Minute Columns")
        
        conn = sqlite3.connect(self.output_db)
        cursor = conn.cursor()
        
        try:
            # Add columns if they don't exist
            for col_name in ['power_per_minute_avg', 'power_per_minute_exp']:
                try:
                    cursor.execute(f'ALTER TABLE burnup_data ADD COLUMN {col_name} REAL')
                except Exception:
                    pass
            
            print("Calculating power_per_minute_avg...")
            cursor.execute('''
                UPDATE burnup_data 
                SET power_per_minute_avg = 
                    CASE 
                        WHEN `Power\nDuration` IS NOT NULL 
                             AND `Total Energy\n(average)` IS NOT NULL
                             AND CAST(`Power\nDuration` AS REAL) > 0
                        THEN ROUND(
                            CAST(`Total Energy\n(average)` AS REAL) / 
                            (COALESCE(NULLIF(CAST(`Delta Time\n(minutes)` AS REAL), 0), 5.0) * 
                             CAST(`Power\nDuration` AS REAL)), 6)
                        ELSE NULL
                    END
            ''')
            
            print("Calculating power_per_minute_exp...")
            cursor.execute('''
                UPDATE burnup_data 
                SET power_per_minute_exp = 
                    CASE 
                        WHEN `Power\nDuration` IS NOT NULL 
                             AND `Total Energy\n(exponential)` IS NOT NULL
                             AND CAST(`Power\nDuration` AS REAL) > 0
                        THEN ROUND(
                            CAST(`Total Energy\n(exponential)` AS REAL) / 
                            (COALESCE(NULLIF(CAST(`Delta Time\n(minutes)` AS REAL), 0), 5.0) * 
                             CAST(`Power\nDuration` AS REAL)), 6)
                        ELSE NULL
                    END
            ''')
            
            conn.commit()
            
            # Get statistics
            cursor.execute('''
                SELECT COUNT(power_per_minute_avg), COUNT(power_per_minute_exp),
                       AVG(power_per_minute_avg), AVG(power_per_minute_exp)
                FROM burnup_data
            ''')
            avg_count, exp_count, avg_mean, exp_mean = cursor.fetchone()
            
            print(f"✓ power_per_minute_avg: {avg_count:,} values (avg: {avg_mean:.6f})")
            print(f"✓ power_per_minute_exp: {exp_count:,} values (avg: {exp_mean:.6f})")
            
        except Exception as e:
            print(f"Error calculating power columns: {e}")
            raise
        finally:
            conn.close()
    
    def calculate_time_since_shutdown(self):
        """Calculate minutes since previous shutdown"""
        self.print_step(7, "Calculating Time Since Previous Shutdown")
        
        conn = sqlite3.connect(self.output_db)
        cursor = conn.cursor()
        
        try:
            # Add column if it doesn't exist
            try:
                cursor.execute('ALTER TABLE burnup_data ADD COLUMN minutes_since_prev_shutdown REAL')
            except Exception:
                pass
            
            print("Calculating minutes_since_prev_shutdown...")
            cursor.execute('''
                UPDATE burnup_data 
                SET minutes_since_prev_shutdown = 
                    CASE 
                        WHEN Date IS NOT NULL AND `Time @_1` IS NOT NULL AND datetime_combined IS NOT NULL
                        THEN
                            (julianday(datetime_combined) - julianday((
                                SELECT datetime_combined FROM burnup_data b2 
                                WHERE b2.datetime_combined < burnup_data.datetime_combined
                                AND b2.`Shutdown\nTime` IS NOT NULL 
                                AND b2.datetime_combined IS NOT NULL
                                ORDER BY b2.datetime_combined DESC 
                                LIMIT 1
                            ))) * 1440.0
                        ELSE NULL
                    END
            ''')
            
            conn.commit()
            
            # Get statistics  
            cursor.execute('''
                SELECT COUNT(minutes_since_prev_shutdown), 
                       AVG(minutes_since_prev_shutdown),
                       MIN(minutes_since_prev_shutdown),
                       MAX(minutes_since_prev_shutdown)
                FROM burnup_data 
                WHERE minutes_since_prev_shutdown IS NOT NULL
            ''')
            count, avg_time, min_time, max_time = cursor.fetchone()
            
            print(f"✓ minutes_since_prev_shutdown: {count:,} values")
            if avg_time is not None:
                print(f"  Average: {avg_time:.1f} minutes ({avg_time/60:.1f} hours)")
                print(f"  Range: {min_time:.0f} to {max_time:.0f} minutes")
            
        except Exception as e:
            print(f"Error calculating time since shutdown: {e}")
            raise
        finally:
            conn.close()
    
    def generate_final_report(self):
        """Generate final processing report"""
        self.print_header("FINAL PROCESSING REPORT")
        
        conn = sqlite3.connect(self.output_db)
        cursor = conn.cursor()
        
        try:
            # Overall statistics
            cursor.execute("SELECT COUNT(*) FROM burnup_data")
            total_rows = cursor.fetchone()[0]
            
            cursor.execute("SELECT MIN(start_year), MAX(end_year) FROM burnup_data")
            min_year, max_year = cursor.fetchone()
            
            cursor.execute("SELECT COUNT(DISTINCT sheet_name) FROM burnup_data")
            total_sheets = cursor.fetchone()[0]
            
            cursor.execute("PRAGMA table_info(burnup_data)")
            total_columns = len(cursor.fetchall())
            
            print(f"Database: {self.output_db}")
            print(f"Total rows: {total_rows:,}")
            print(f"Total columns: {total_columns}")
            print(f"Total sheets: {total_sheets}")
            print(f"Year coverage: {min_year}-{max_year}")
            
            # Column coverage
            columns_to_check = [
                'datetime_combined',
                'power_per_minute_avg', 
                'power_per_minute_exp',
                'minutes_since_prev_shutdown'
            ]
            
            print("\nColumn Coverage:")
            for col in columns_to_check:
                cursor.execute(f"SELECT COUNT({col}) FROM burnup_data WHERE {col} IS NOT NULL")
                count = cursor.fetchone()[0]
                pct = count / total_rows * 100
                print(f"  {col}: {count:,} ({pct:.1f}%)")
            
            # Year distribution
            cursor.execute('''
                SELECT start_year, COUNT(*) as count
                FROM burnup_data 
                GROUP BY start_year 
                ORDER BY start_year
            ''')
            
            print("\nYear Distribution:")
            for year, count in cursor.fetchall():
                print(f"  {year}: {count:,} rows")
            
            print("\n" + "=" * 80)
            print("✅ PROCESSING COMPLETE!")
            print(f"✅ Clean burnup database with proper column structure created: {self.output_db}")
            print("✅ Ready for analysis and querying")
            print("=" * 80)
            
        except Exception as e:
            print(f"Error generating report: {e}")
        finally:
            conn.close()
    
    def run_complete_pipeline(self):
        """Run the complete processing pipeline"""
        self.print_header("BURNUP DATA COMPLETE PROCESSING PIPELINE - FIXED VERSION")
        print("This script will process all Excel files and create a comprehensive database.")
        print("FIXES: Proper column alignment, standardized mapping, clean structure")
        
        start_time = datetime.now()
        
        try:
            self.process_main_excel()
            self.process_secondary_excel()
            self.create_database()
            self.add_datetime_column()
            self.reorder_chronologically() 
            self.calculate_power_columns()
            self.calculate_time_since_shutdown()
            self.generate_final_report()
            
            end_time = datetime.now()
            processing_time = end_time - start_time
            
            print(f"\n⏱️  Total processing time: {processing_time}")
            print(f"🎉 All done! Your FIXED burnup database is ready at: {self.output_db}")
            
        except Exception as e:
            print(f"\n❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        return True

def main():
    """Main entry point"""
    processor = BurnupProcessor()
    success = processor.run_complete_pipeline()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())