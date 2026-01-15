#!/usr/bin/env python3
"""
ORIGEN Power/Time Card Generator

This script processes nuclear reactor burnup data from the SQLite database
and generates ORIGEN-compatible power and time cards for reactor depletion analysis.

The script processes each database row sequentially and:
1. Adds shutdown periods (0 MW) when minutes_since_prev_shutdown is present
2. Uses power_per_minute_avg values (already in MW)
3. Calculates total time duration (Delta Time + Power Duration)
4. Outputs two text blocks suitable for ORIGEN input

Usage:
    python generate_origen_cards.py
    python generate_origen_cards.py --year 2023
    python generate_origen_cards.py --start-date 2020-01-01 --end-date 2023-12-31
    python generate_origen_cards.py --output my_origen_cards.txt
"""

import sqlite3
import argparse
import sys
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrigenCardGenerator:
    def __init__(self, db_path="combined_yearly_data.db"):
        self.db_path = db_path
        self.power_data = []  # List of (power_MW, duration_minutes) tuples
        self.total_rows = 0
        self.processed_rows = 0
        self.error_rows = 0
        self.shutdown_periods = 0
        self.first_date = None
        self.last_date = None
        
    def validate_database(self):
        """Check if database exists and has required table/columns"""
        if not Path(self.db_path).exists():
            raise FileNotFoundError(f"Database file '{self.db_path}' not found!")
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='burnup_data'")
            if not cursor.fetchone():
                raise ValueError("Table 'burnup_data' not found in database")
            
            # Check for required columns
            cursor.execute("PRAGMA table_info(burnup_data)")
            columns = [col[1] for col in cursor.fetchall()]
            required_cols = ['power_per_minute_avg', 'Delta Time\n(minutes)', 'Power\nDuration', 'minutes_since_prev_shutdown']
            
            missing_cols = [col for col in required_cols if col not in columns]
            if missing_cols:
                logger.warning(f"Missing columns in database: {missing_cols}")
                
        finally:
            conn.close()
    
    def build_query(self, start_date=None, end_date=None, year=None):
        """Build SQL query with optional date filtering"""
        base_query = '''
        SELECT 
            Date,
            power_per_minute_avg,
            "Delta Time
(minutes)",
            "Power
Duration",
            minutes_since_prev_shutdown,
            original_row,
            sheet_name
        FROM burnup_data 
        WHERE 1=1
        '''
        
        conditions = []
        params = []
        
        if year:
            conditions.append("strftime('%Y', datetime_combined) = ?")
            params.append(str(year))
        
        if start_date:
            conditions.append("Date >= ?")
            params.append(start_date)
            
        if end_date:
            conditions.append("Date <= ?")
            params.append(end_date)
        
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        base_query += " ORDER BY datetime_combined, Date, original_row"
        
        return base_query, params
    
    def safe_float(self, value, default=0.0):
        """Safely convert value to float, return default if None or invalid"""
        if value is None or value == '':
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def process_database(self, start_date=None, end_date=None, year=None):
        """Process database rows and generate power/time data"""
        logger.info(f"Processing database: {self.db_path}")
        
        self.validate_database()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            query, params = self.build_query(start_date, end_date, year)
            
            # Get total count for progress
            count_query = """
            SELECT COUNT(*)
            FROM burnup_data 
            WHERE 1=1
            """
            if params:
                # Add the same conditions as the main query
                if year:
                    count_query += " AND strftime('%Y', datetime_combined) = ?"
                if start_date:
                    count_query += " AND Date >= ?"
                if end_date:
                    count_query += " AND Date <= ?"
            cursor.execute(count_query, params)
            self.total_rows = int(cursor.fetchone()[0])
            
            if self.total_rows == 0:
                logger.warning("No rows found matching the criteria")
                return
            
            logger.info(f"Found {self.total_rows} rows to process")
            
            # Process rows
            cursor.execute(query, params)
            
            for row_num, row in enumerate(cursor, 1):
                try:
                    self.process_row(row, row_num)
                    if row_num % 1000 == 0:
                        logger.info(f"Processed {row_num}/{self.total_rows} rows ({row_num/self.total_rows*100:.1f}%)")
                except Exception as e:
                    logger.error(f"Error processing row {row_num}: {e}")
                    self.error_rows += 1
                    continue
                    
        finally:
            conn.close()
            
        logger.info(f"Processing complete: {self.processed_rows} rows processed, {self.error_rows} errors, {self.shutdown_periods} shutdown periods")
    
    def process_row(self, row, row_num):
        """Process a single database row"""
        date, power_per_min, delta_time, power_duration, shutdown_time, orig_row, sheet = row
        
        # Track date range
        if date and date != '':
            if self.first_date is None or (date and date < self.first_date):
                self.first_date = date
            if self.last_date is None or (date and date > self.last_date):
                self.last_date = date
        
        # Handle shutdown period first
        if shutdown_time is not None and shutdown_time > 0:
            self.power_data.append((0.0, shutdown_time))  # 0 MW for shutdown duration
            self.shutdown_periods += 1
            logger.debug(f"Row {row_num}: Added shutdown period of {shutdown_time:.1f} minutes")
        
        # Process power period
        if power_per_min is None or power_per_min == '':
            logger.warning(f"Row {row_num} (sheet: {sheet}, orig_row: {orig_row}): Null power value, skipping")
            return
        
        try:
            # power_per_min is in kW (from Total Energy [kWh] / Total Duration [min])
            # Convert kW to MW for ORIGEN input
            power_mw = self.safe_float(power_per_min) / 1000
            
            # Calculate total duration (Delta Time + Power Duration)
            delta_min = self.safe_float(delta_time, 0.0)
            power_min = self.safe_float(power_duration, 0.0)
            total_duration = delta_min + power_min
            
            if total_duration <= 0:
                logger.warning(f"Row {row_num}: Total duration is {total_duration}, skipping")
                return
            
            self.power_data.append((power_mw, total_duration))
            self.processed_rows += 1
            
            logger.debug(f"Row {row_num}: {power_mw:.6f} MW for {total_duration:.1f} minutes")
            
        except Exception as e:
            logger.error(f"Row {row_num}: Error processing power data: {e}")
            raise
    
    def _generate_filename_with_dates(self):
        """Generate output filename with date range information"""
        base_name = "origen_cards"
        
        if self.first_date and self.last_date:
            # Both dates available
            if self.first_date == self.last_date:
                # Single date
                return f"{base_name}_{self.first_date}.txt"
            else:
                # Date range
                first_year = self.first_date[:4] if len(self.first_date) >= 4 else self.first_date
                last_year = self.last_date[:4] if len(self.last_date) >= 4 else self.last_date
                
                if first_year == last_year:
                    # Same year
                    return f"{base_name}_{first_year}.txt"
                else:
                    # Multiple years
                    return f"{base_name}_{first_year}_to_{last_year}.txt"
        elif self.first_date:
            # Only start date
            year = self.first_date[:4] if len(self.first_date) >= 4 else self.first_date
            return f"{base_name}_from_{year}.txt"
        elif self.last_date:
            # Only end date
            year = self.last_date[:4] if len(self.last_date) >= 4 else self.last_date
            return f"{base_name}_to_{year}.txt"
        else:
            # No date information
            return f"{base_name}.txt"
    
    def get_date_info(self):
        """Get date range information for metadata"""
        return {
            'first_date': self.first_date,
            'last_date': self.last_date,
            'date_range_str': self._get_date_range_string()
        }
    
    def _get_date_range_string(self):
        """Get date range string"""
        if self.first_date and self.last_date:
            if self.first_date == self.last_date:
                return self.first_date
            else:
                first_year = self.first_date[:4] if len(self.first_date) >= 4 else self.first_date
                last_year = self.last_date[:4] if len(self.last_date) >= 4 else self.last_date
                
                if first_year == last_year:
                    return first_year
                else:
                    return f"{first_year}-{last_year}"
        elif self.first_date:
            return f"from {self.first_date[:4]}" if len(self.first_date) >= 4 else f"from {self.first_date}"
        elif self.last_date:
            return f"to {self.last_date[:4]}" if len(self.last_date) >= 4 else f"to {self.last_date}"
        else:
            return "unknown"
    
    def generate_origen_cards(self, output_file=None):
        """Generate ORIGEN power and time cards and write to file"""
        if not self.power_data:
            logger.error("No data to write - process database first")
            return False
        
        # Auto-generate filename with date range if not provided
        if output_file is None:
            output_file = self._generate_filename_with_dates()
        
        logger.info(f"Generating ORIGEN cards with {len(self.power_data)} entries")
        
        try:
            with open(output_file, 'w') as f:
                # Write header
                f.write("# ORIGEN Power and Time Cards\n")
                f.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Total entries: {len(self.power_data)}\n")
                f.write(f"# Shutdown periods: {self.shutdown_periods}\n")
                f.write(f"# Power periods: {len(self.power_data) - self.shutdown_periods}\n")
                
                # Add date range information
                if self.first_date and self.last_date:
                    f.write(f"# Date range: {self.first_date} to {self.last_date}\n")
                elif self.first_date:
                    f.write(f"# Start date: {self.first_date}\n")
                elif self.last_date:
                    f.write(f"# End date: {self.last_date}\n")
                
                f.write("\n")
                
                # Calculate summary statistics
                total_time = sum(duration for _, duration in self.power_data)
                avg_power = sum(power * duration for power, duration in self.power_data) / total_time if total_time > 0 else 0
                max_power = max(power for power, _ in self.power_data)
                
                f.write(f"# Summary Statistics:\n")
                f.write(f"# Total time: {total_time:.1f} minutes ({total_time/60:.1f} hours, {total_time/1440:.1f} days)\n")
                f.write(f"# Average power: {avg_power:.6f} MW\n")
                f.write(f"# Maximum power: {max_power:.6f} MW\n")
                f.write("\n")
                
                # Write Power Block
                f.write("# POWER BLOCK (MW)\n")
                for i, (power, _) in enumerate(self.power_data):
                    f.write(f"{power:.8f}")
                    if (i + 1) % 10 == 0:  # New line every 10 values for readability
                        f.write("\n")
                    else:
                        f.write("  ")
                
                if len(self.power_data) % 10 != 0:  # Add final newline if needed
                    f.write("\n")
                
                f.write("\n")
                
                # Write Time Block (cumulative time)
                f.write("# TIME BLOCK (minutes)\n")
                cumulative_time = 0.0
                for i, (_, duration) in enumerate(self.power_data):
                    cumulative_time += duration
                    # Round to whole minutes
                    rounded_time = round(cumulative_time)
                    cumulative_str = str(int(rounded_time))
                    f.write(cumulative_str)
                    if (i + 1) % 10 == 0:  # New line every 10 values for readability
                        f.write("\n")
                    else:
                        f.write("  ")
                
                if len(self.power_data) % 10 != 0:  # Add final newline if needed
                    f.write("\n")
                
            logger.info(f"ORIGEN cards written to: {output_file}")
            logger.info(f"Total entries: {len(self.power_data)}")
            logger.info(f"Total time covered: {total_time:.1f} minutes ({total_time/1440:.1f} days)")
            logger.info(f"Average power: {avg_power:.6f} MW")
            if self.first_date and self.last_date:
                logger.info(f"Date range: {self.first_date} to {self.last_date}")
            
            return {'success': True, 'filename': output_file}
            
        except Exception as e:
            logger.error(f"Error writing output file: {e}")
            return {'success': False, 'filename': output_file if 'output_file' in locals() else None}

def main():
    parser = argparse.ArgumentParser(
        description="Generate ORIGEN power and time cards from burnup database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_origen_cards.py
  python generate_origen_cards.py --year 2023
  python generate_origen_cards.py --start-date 2020-01-01 --end-date 2023-12-31
  python generate_origen_cards.py --output my_cards.txt --year 2022
        """
    )
    
    parser.add_argument("--db", default="combined_yearly_data.db",
                        help="Path to SQLite database file (default: combined_yearly_data.db)")
    
    parser.add_argument("--output", "-o", default=None,
                        help="Output file name (default: auto-generated with date range)")
    
    parser.add_argument("--year", type=int,
                        help="Process data for specific year only")
    
    parser.add_argument("--start-date",
                        help="Start date (YYYY-MM-DD format)")
    
    parser.add_argument("--end-date",
                        help="End date (YYYY-MM-DD format)")
    
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate date arguments
    if args.start_date:
        try:
            datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid start-date format. Use YYYY-MM-DD")
            sys.exit(1)
    
    if args.end_date:
        try:
            datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid end-date format. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Create generator and process data
    try:
        generator = OrigenCardGenerator(args.db)
        generator.process_database(
            start_date=args.start_date,
            end_date=args.end_date,
            year=args.year
        )
        
        if generator.processed_rows == 0:
            logger.error("No valid data processed - check your date filters and database content")
            sys.exit(1)
        
        result = generator.generate_origen_cards(args.output)
        if not result['success']:
            sys.exit(1)
        
        logger.info(f"ORIGEN cards successfully generated: {result['filename']}")
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()