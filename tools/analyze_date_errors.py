import sqlite3
import pandas as pd
from pathlib import Path

def analyze_date_errors(db_path="combined_yearly_data.db"):
    """
    Comprehensive analysis of date/time errors in the burnup database
    
    Identifies:
    1. Chronological inconsistencies within sheets
    2. Year boundary violations
    3. Extreme downtime calculations
    4. Missing or invalid dates
    
    Args:
        db_path (str): Path to SQLite database file
    """
    if not Path(db_path).exists():
        print(f"Database file '{db_path}' not found!")
        return None
    
    conn = sqlite3.connect(db_path)
    
    try:
        print("="*80)
        print("COMPREHENSIVE DATE/TIME ERROR ANALYSIS")
        print("="*80)
        
        # 1. EXTREME DOWNTIME VALUES (likely date errors)
        print("\n1. EXTREME DOWNTIME VALUES:")
        print("-" * 40)
        
        query1 = '''
        SELECT 
            sheet_name,
            original_row,
            Date,
            [Time @_1] as current_time,
            minutes_since_prev_shutdown,
            ROUND(minutes_since_prev_shutdown / 1440.0, 1) as days,
            CASE 
                WHEN minutes_since_prev_shutdown < -100000 THEN 'MAJOR BACKWARDS JUMP'
                WHEN minutes_since_prev_shutdown < -1000 THEN 'Backwards Jump'
                WHEN minutes_since_prev_shutdown > 100000 THEN 'MAJOR OUTAGE' 
                WHEN minutes_since_prev_shutdown > 50000 THEN 'Minor OUTAGE'
                ELSE 'Other'
            END as error_type,
            -- Previous shutdown date (chronologically across all sheets)
            (SELECT b2.datetime_combined FROM burnup_data b2 
             WHERE b2.datetime_combined < burnup_data.datetime_combined
               AND b2.[Shutdown
Time] IS NOT NULL 
               AND b2.datetime_combined IS NOT NULL
             ORDER BY b2.datetime_combined DESC LIMIT 1) as prev_date
        FROM burnup_data
        WHERE minutes_since_prev_shutdown IS NOT NULL
          AND (minutes_since_prev_shutdown < -1000 OR minutes_since_prev_shutdown > 50000)
        ORDER BY minutes_since_prev_shutdown
        '''
        
        df1 = pd.read_sql_query(query1, conn)
        if len(df1) > 0:
            print(f"Found {len(df1)} extreme downtime values:")
            print(df1.to_string(index=False))
        else:
            print("No extreme downtime values found.")
        
        # 2. YEAR MIXING WITHIN SHEETS
        print("\n\n2. YEAR MIXING WITHIN SHEETS:")
        print("-" * 40)
        
        query2 = '''
        SELECT 
            sheet_name,
            SUBSTR(Date, 1, 4) as year_in_data,
            COUNT(*) as occurrences,
            MIN(original_row) as first_row,
            MAX(original_row) as last_row,
            -- Use the properly parsed year columns from database
            start_year as expected_start_year,
            end_year as expected_end_year
        FROM burnup_data
        WHERE Date IS NOT NULL 
          AND Date != 'NaT' 
          AND Date != 'nan'
          AND Date != ''
        GROUP BY sheet_name, SUBSTR(Date, 1, 4), start_year, end_year
        HAVING COUNT(*) > 0
        ORDER BY sheet_name, year_in_data
        '''
        
        df2 = pd.read_sql_query(query2, conn)
        
        # Identify problematic sheets
        problematic_sheets = []
        for sheet in df2['sheet_name'].unique():
            sheet_data = df2[df2['sheet_name'] == sheet]
            expected_years = [sheet[:4], sheet[5:9]]  # Extract years from sheet name
            actual_years = sheet_data['year_in_data'].tolist()
            
            # Check if actual years match expected
            unexpected_years = [y for y in actual_years if y not in expected_years]
            if unexpected_years:
                problematic_sheets.append(sheet)
        
        print(f"Year distribution by sheet:")
        print(df2.to_string(index=False))
        
        if problematic_sheets:
            print(f"\nProblematic sheets with unexpected years: {problematic_sheets}")
        
        # 3. CHRONOLOGICAL VIOLATIONS
        print("\n\n3. CHRONOLOGICAL VIOLATIONS:")
        print("-" * 40)
        
        query3 = '''
        WITH ordered_dates AS (
            SELECT 
                sheet_name,
                original_row,
                Date,
                [Time @_1] as current_time,
                LAG(Date) OVER (PARTITION BY sheet_name ORDER BY original_row) as prev_date,
                LAG(original_row) OVER (PARTITION BY sheet_name ORDER BY original_row) as prev_row
            FROM burnup_data
            WHERE Date IS NOT NULL 
              AND Date != 'NaT' 
              AND Date != 'nan'
              AND Date != ''
        )
        SELECT 
            sheet_name,
            COUNT(*) as total_date_entries,
            COUNT(CASE WHEN Date < prev_date THEN 1 END) as backwards_dates,
            ROUND(COUNT(CASE WHEN Date < prev_date THEN 1 END) * 100.0 / COUNT(*), 1) as pct_backwards
        FROM ordered_dates
        WHERE prev_date IS NOT NULL
        GROUP BY sheet_name
        HAVING COUNT(CASE WHEN Date < prev_date THEN 1 END) > 0
        ORDER BY pct_backwards DESC
        '''
        
        df3 = pd.read_sql_query(query3, conn)
        if len(df3) > 0:
            print("Sheets with chronological violations:")
            print(df3.to_string(index=False))
        else:
            print("No chronological violations found.")
        
        # 4. SPECIFIC DATE VIOLATIONS (detailed examples)
        print("\n\n4. DETAILED DATE VIOLATION EXAMPLES:")
        print("-" * 40)
        
        query4 = '''
        WITH ordered_dates AS (
            SELECT 
                sheet_name,
                original_row,
                Date,
                [Time @_1] as current_time,
                LAG(Date) OVER (PARTITION BY sheet_name ORDER BY original_row) as prev_date,
                LAG(original_row) OVER (PARTITION BY sheet_name ORDER BY original_row) as prev_row
            FROM burnup_data
            WHERE Date IS NOT NULL 
              AND Date != 'NaT' 
              AND Date != 'nan'
              AND Date != ''
        )
        SELECT 
            sheet_name,
            original_row,
            Date as current_date,
            prev_date,
            prev_row,
            SUBSTR(Date, 1, 4) as current_year,
            SUBSTR(prev_date, 1, 4) as prev_year,
            julianday(prev_date) - julianday(Date) as days_backwards
        FROM ordered_dates
        WHERE Date < prev_date
        ORDER BY days_backwards DESC
        LIMIT 15
        '''
        
        df4 = pd.read_sql_query(query4, conn)
        if len(df4) > 0:
            print("Specific examples of date violations:")
            print(df4.to_string(index=False))
        
        # 5. MISSING DATE PATTERNS
        print("\n\n5. MISSING DATE PATTERNS:")
        print("-" * 40)
        
        query5 = '''
        SELECT 
            sheet_name,
            COUNT(*) as total_rows,
            COUNT(Date) as rows_with_date,
            COUNT(*) - COUNT(Date) as missing_dates,
            COUNT(CASE WHEN Date = 'NaT' THEN 1 END) as nat_dates,
            COUNT(CASE WHEN [Time @_1] IS NOT NULL THEN 1 END) as has_time_at,
            COUNT(CASE WHEN [Shutdown
Time] IS NOT NULL THEN 1 END) as has_shutdown_time,
            ROUND((COUNT(*) - COUNT(Date)) * 100.0 / COUNT(*), 1) as pct_missing_dates
        FROM burnup_data
        GROUP BY sheet_name
        ORDER BY pct_missing_dates DESC
        '''
        
        df5 = pd.read_sql_query(query5, conn)
        print("Missing date statistics by sheet:")
        print(df5.to_string(index=False))
        
        # 6. SUMMARY STATISTICS
        print("\n\n6. SUMMARY STATISTICS:")
        print("-" * 40)
        
        query6 = '''
        SELECT 
            COUNT(*) as total_rows,
            COUNT(minutes_since_prev_shutdown) as calculated_downtimes,
            COUNT(CASE WHEN minutes_since_prev_shutdown BETWEEN 1 AND 86400 THEN 1 END) as reasonable_downtimes,
            COUNT(CASE WHEN minutes_since_prev_shutdown < -1000 THEN 1 END) as major_backwards,
            COUNT(CASE WHEN minutes_since_prev_shutdown > 86400 THEN 1 END) as over_60_days,
            ROUND(COUNT(CASE WHEN minutes_since_prev_shutdown BETWEEN 1 AND 86400 THEN 1 END) * 100.0 
                  / COUNT(minutes_since_prev_shutdown), 1) as pct_reasonable
        FROM burnup_data
        '''
        
        df6 = pd.read_sql_query(query6, conn)
        print("Overall data quality summary:")
        print(df6.to_string(index=False))
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)
        
        return {
            'extreme_values': df1,
            'year_mixing': df2,
            'chronological_violations': df3,
            'specific_violations': df4,
            'missing_dates': df5,
            'summary': df6
        }
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        return None
    finally:
        conn.close()

def export_error_report(results, output_file="date_error_report.txt"):
    """
    Export the error analysis to a text file
    """
    if results is None:
        print("No results to export")
        return
    
    with open(output_file, 'w') as f:
        f.write("DATE/TIME ERROR ANALYSIS REPORT\n")
        f.write("=" * 50 + "\n\n")
        
        f.write("1. EXTREME DOWNTIME VALUES:\n")
        f.write("-" * 30 + "\n")
        f.write(results['extreme_values'].to_string(index=False))
        f.write("\n\n")
        
        f.write("2. YEAR MIXING WITHIN SHEETS:\n")
        f.write("-" * 30 + "\n")
        f.write(results['year_mixing'].to_string(index=False))
        f.write("\n\n")
        
        f.write("3. CHRONOLOGICAL VIOLATIONS:\n")
        f.write("-" * 30 + "\n")
        f.write(results['chronological_violations'].to_string(index=False))
        f.write("\n\n")
        
        f.write("4. SPECIFIC DATE VIOLATIONS:\n")
        f.write("-" * 30 + "\n")
        f.write(results['specific_violations'].to_string(index=False))
        f.write("\n\n")
        
        f.write("5. MISSING DATE PATTERNS:\n")
        f.write("-" * 30 + "\n")
        f.write(results['missing_dates'].to_string(index=False))
        f.write("\n\n")
        
        f.write("6. SUMMARY STATISTICS:\n")
        f.write("-" * 30 + "\n")
        f.write(results['summary'].to_string(index=False))
        f.write("\n\n")
    
    print(f"\nError report exported to: {output_file}")

if __name__ == "__main__":
    # Run the comprehensive analysis
    results = analyze_date_errors()
    
    if results:
        # Export to file
        export_error_report(results)
        
        print(f"\nKEY FINDINGS:")
        print(f"- Extreme values: {len(results['extreme_values'])} cases")
        print(f"- Sheets with chronological issues: {len(results['chronological_violations'])}")
        print(f"- Reasonable downtime calculations: {results['summary'].iloc[0]['reasonable_downtimes']} out of {results['summary'].iloc[0]['calculated_downtimes']}")
        print(f"- Data quality: {results['summary'].iloc[0]['pct_reasonable']}% of calculations are reasonable")