#!/usr/bin/env python3
"""
ORIGEN Cards Verification Script

This script cross-checks generated ORIGEN power/time cards against the original database
to verify that the values match within acceptable tolerances. It recreates the same 
processing logic used to generate the cards and compares each entry.

Features:
- Parses ORIGEN card files to extract power and time values
- Recreates data processing from database using same logic as generator
- Compares values with configurable tolerances
- Provides detailed verification report with statistics
- Supports same filtering options as the generator

Usage:
    python verify_origen_cards.py --file origen_cards.txt
    python verify_origen_cards.py --file test_no_zeros.txt --year 1992
    python verify_origen_cards.py --file cards.txt --verbose --tolerance-power 1e-8
"""

import sqlite3
import argparse
import sys
import re
from pathlib import Path
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrigenCardsVerifier:
    def __init__(self, db_path="combined_yearly_data.db", tolerance_power=1e-9, tolerance_time=0.001):
        self.db_path = db_path
        self.tolerance_power = tolerance_power
        self.tolerance_time = tolerance_time
        
        # File data
        self.file_powers = []
        self.file_times = []
        self.file_metadata = {}
        
        # Database data
        self.db_powers = []
        self.db_times = []
        
        # Verification results
        self.exact_matches = 0
        self.tolerance_matches = 0
        self.mismatches = 0
        self.power_deviations = []
        self.time_deviations = []
        self.mismatch_details = []
    
    def parse_origen_file(self, file_path):
        """Parse ORIGEN cards file to extract power and time values"""
        logger.info(f"Parsing ORIGEN file: {file_path}")
        
        if not Path(file_path).exists():
            raise FileNotFoundError(f"ORIGEN file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Extract metadata from header
        metadata_patterns = {
            'total_entries': r'# Total entries: (\d+)',
            'shutdown_periods': r'# Shutdown periods: (\d+)',
            'power_periods': r'# Power periods: (\d+)',
            'date_range': r'# Date range: (.+)',
            'total_time': r'# Total time: ([\d.]+) minutes',
            'average_power': r'# Average power: ([\d.e-]+) MW',
            'max_power': r'# Maximum power: ([\d.e-]+) MW'
        }
        
        for key, pattern in metadata_patterns.items():
            match = re.search(pattern, content)
            if match:
                if key in ['total_entries', 'shutdown_periods', 'power_periods']:
                    self.file_metadata[key] = int(match.group(1))
                elif key in ['total_time', 'average_power', 'max_power']:
                    self.file_metadata[key] = float(match.group(1))
                else:
                    self.file_metadata[key] = match.group(1).strip()
        
        # Extract power values
        power_section = re.search(r'# POWER BLOCK \(MW\)\n(.*?)\n# TIME BLOCK', content, re.DOTALL)
        if not power_section:
            raise ValueError("Could not find POWER BLOCK in file")
        
        power_text = power_section.group(1)
        power_values = []
        for line in power_text.strip().split('\n'):
            line_values = line.split()
            for val in line_values:
                if val and not val.startswith('#'):
                    power_values.append(float(val))
        
        self.file_powers = power_values
        
        # Extract time values
        time_section = re.search(r'# TIME BLOCK \(minutes\)\n(.*)$', content, re.DOTALL)
        if not time_section:
            raise ValueError("Could not find TIME BLOCK in file")
        
        time_text = time_section.group(1)
        time_values = []
        for line in time_text.strip().split('\n'):
            line_values = line.split()
            for val in line_values:
                if val and not val.startswith('#'):
                    time_values.append(float(val))
        
        self.file_times = time_values
        
        logger.info(f"Parsed {len(self.file_powers)} power values and {len(self.file_times)} time values")
        logger.info(f"File metadata: {self.file_metadata}")
        
        if len(self.file_powers) != len(self.file_times):
            raise ValueError(f"Mismatch: {len(self.file_powers)} power values vs {len(self.file_times)} time values")
    
    def safe_float(self, value, default=0.0):
        """Safely convert value to float, return default if None or invalid"""
        if value is None or value == '':
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def build_query(self, start_date=None, end_date=None, year=None):
        """Build SQL query with optional date filtering (same as generator)"""
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
            conditions.append("start_year = ? OR end_year = ?")
            params.extend([year, year])
        
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
    
    def recreate_from_database(self, start_date=None, end_date=None, year=None):
        """Recreate power/time data from database using same logic as generator"""
        logger.info("Recreating data from database...")
        
        if not Path(self.db_path).exists():
            raise FileNotFoundError(f"Database file not found: {self.db_path}")
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            query, params = self.build_query(start_date, end_date, year)
            cursor.execute(query, params)
            
            power_data = []
            processed_rows = 0
            shutdown_periods = 0
            
            for row_num, row in enumerate(cursor, 1):
                date, power_per_min, delta_time, power_duration, shutdown_time, orig_row, sheet = row
                
                # Handle shutdown period first (same logic as generator)
                if shutdown_time is not None and shutdown_time > 0:
                    power_data.append((0.0, shutdown_time))  # 0 MW for shutdown duration
                    shutdown_periods += 1
                
                # Process power period
                if power_per_min is None or power_per_min == '':
                    logger.debug(f"Row {row_num}: Skipping null power value")
                    continue
                
                try:
                    # Convert power (using same logic as generator - divide by 1000)
                    power_mw = self.safe_float(power_per_min) / 1000
                    
                    # Calculate total duration (Delta Time + Power Duration)
                    delta_min = self.safe_float(delta_time, 0.0)
                    power_min = self.safe_float(power_duration, 0.0)
                    total_duration = delta_min + power_min
                    
                    if total_duration <= 0:
                        logger.debug(f"Row {row_num}: Skipping zero duration")
                        continue
                    
                    power_data.append((power_mw, total_duration))
                    processed_rows += 1
                    
                except Exception as e:
                    logger.warning(f"Row {row_num}: Error processing: {e}")
                    continue
                    
        finally:
            conn.close()
        
        # Extract separate lists
        self.db_powers = [power for power, _ in power_data]
        self.db_times = [duration for _, duration in power_data]
        
        logger.info(f"Recreated {len(self.db_powers)} power values and {len(self.db_times)} time values")
        logger.info(f"Database: {processed_rows} power periods, {shutdown_periods} shutdown periods")
    
    def compare_values(self, verbose=False):
        """Compare file values against database values"""
        logger.info("Comparing file values against database values...")
        
        if len(self.file_powers) != len(self.db_powers):
            logger.error(f"Length mismatch: File has {len(self.file_powers)}, DB has {len(self.db_powers)}")
            return False
        
        total_entries = len(self.file_powers)
        self.exact_matches = 0
        self.tolerance_matches = 0
        self.mismatches = 0
        self.power_deviations = []
        self.time_deviations = []
        self.mismatch_details = []
        
        for i in range(total_entries):
            file_power = self.file_powers[i]
            file_time = self.file_times[i]
            db_power = self.db_powers[i]
            db_time = self.db_times[i]
            
            power_diff = abs(file_power - db_power)
            time_diff = abs(file_time - db_time)
            
            self.power_deviations.append(power_diff)
            self.time_deviations.append(time_diff)
            
            # Check for exact match
            if power_diff == 0 and time_diff == 0:
                self.exact_matches += 1
                if verbose:
                    logger.debug(f"Entry {i+1}: EXACT MATCH - Power: {file_power}, Time: {file_time}")
            
            # Check for tolerance match
            elif power_diff <= self.tolerance_power and time_diff <= self.tolerance_time:
                self.tolerance_matches += 1
                if verbose:
                    logger.debug(f"Entry {i+1}: TOLERANCE MATCH - Power diff: {power_diff:.2e}, Time diff: {time_diff:.6f}")
            
            # Mismatch
            else:
                self.mismatches += 1
                mismatch = {
                    'index': i + 1,
                    'file_power': file_power,
                    'db_power': db_power,
                    'power_diff': power_diff,
                    'file_time': file_time,
                    'db_time': db_time,
                    'time_diff': time_diff
                }
                self.mismatch_details.append(mismatch)
                
                if verbose or len(self.mismatch_details) <= 10:  # Show first 10 mismatches
                    logger.warning(f"Entry {i+1}: MISMATCH")
                    logger.warning(f"  Power: File={file_power:.8e}, DB={db_power:.8e}, Diff={power_diff:.2e}")
                    logger.warning(f"  Time:  File={file_time:.6f}, DB={db_time:.6f}, Diff={time_diff:.6f}")
            
            # Progress indicator for large datasets
            if (i + 1) % 1000 == 0:
                logger.info(f"Compared {i+1}/{total_entries} entries...")
        
        return True
    
    def generate_report(self):
        """Generate comprehensive verification report"""
        total_entries = len(self.file_powers)
        
        if total_entries == 0:
            logger.error("No data to report")
            return
        
        print("\n" + "="*60)
        print("ORIGEN CARDS VERIFICATION REPORT")
        print("="*60)
        
        print(f"Database: {self.db_path}")
        print(f"Total entries compared: {total_entries:,}")
        
        if self.file_metadata.get('date_range'):
            print(f"Date range: {self.file_metadata['date_range']}")
        
        print("\nComparison Results:")
        print("-" * 30)
        exact_pct = (self.exact_matches / total_entries) * 100
        tolerance_pct = (self.tolerance_matches / total_entries) * 100
        mismatch_pct = (self.mismatches / total_entries) * 100
        
        print(f"Exact matches:      {self.exact_matches:,} ({exact_pct:.1f}%)")
        print(f"Within tolerance:   {self.tolerance_matches:,} ({tolerance_pct:.1f}%)")
        print(f"Mismatches:         {self.mismatches:,} ({mismatch_pct:.1f}%)")
        
        print(f"\nTolerances used:")
        print(f"Power tolerance:    {self.tolerance_power:.2e} MW")
        print(f"Time tolerance:     {self.tolerance_time:.6f} minutes")
        
        if self.power_deviations:
            print(f"\nPower Accuracy:")
            print(f"Max deviation:      {max(self.power_deviations):.2e} MW")
            print(f"Mean deviation:     {sum(self.power_deviations)/len(self.power_deviations):.2e} MW")
        
        if self.time_deviations:
            print(f"\nTime Accuracy:")
            print(f"Max deviation:      {max(self.time_deviations):.6f} minutes")
            print(f"Mean deviation:     {sum(self.time_deviations)/len(self.time_deviations):.6f} minutes")
        
        if self.mismatch_details:
            print(f"\nFirst {min(5, len(self.mismatch_details))} Mismatches:")
            print("-" * 40)
            for i, mismatch in enumerate(self.mismatch_details[:5]):
                print(f"Entry {mismatch['index']}:")
                print(f"  Power: File={mismatch['file_power']:.8e}, DB={mismatch['db_power']:.8e}")
                print(f"  Time:  File={mismatch['file_time']:.6f}, DB={mismatch['db_time']:.6f}")
        
        print("\nFinal Result:")
        print("-" * 15)
        if self.mismatches == 0:
            print("✓ VERIFICATION PASSED - All values within tolerance")
        else:
            print(f"✗ VERIFICATION FAILED - {self.mismatches} mismatches found")
        
        print("="*60)

def main():
    parser = argparse.ArgumentParser(
        description="Verify ORIGEN power/time cards against database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python verify_origen_cards.py --file origen_cards.txt
  python verify_origen_cards.py --file test_no_zeros.txt --year 1992
  python verify_origen_cards.py --file cards.txt --verbose --tolerance-power 1e-8
        """
    )
    
    parser.add_argument("--file", "-f", required=True,
                        help="ORIGEN cards file to verify")
    
    parser.add_argument("--db", default="combined_yearly_data.db",
                        help="Path to SQLite database file (default: combined_yearly_data.db)")
    
    parser.add_argument("--year", type=int,
                        help="Filter for specific year (use if file was generated with --year)")
    
    parser.add_argument("--start-date",
                        help="Start date filter (YYYY-MM-DD format)")
    
    parser.add_argument("--end-date", 
                        help="End date filter (YYYY-MM-DD format)")
    
    parser.add_argument("--tolerance-power", type=float, default=1e-8,
                        help="Power comparison tolerance in MW (default: 1e-8)")
    
    parser.add_argument("--tolerance-time", type=float, default=0.001,
                        help="Time comparison tolerance in minutes (default: 0.001)")
    
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show detailed comparison for each entry")
    
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
    
    # Create verifier and run verification
    try:
        verifier = OrigenCardsVerifier(
            db_path=args.db,
            tolerance_power=args.tolerance_power,
            tolerance_time=args.tolerance_time
        )
        
        # Parse the ORIGEN file
        verifier.parse_origen_file(args.file)
        
        # Recreate data from database
        verifier.recreate_from_database(
            start_date=args.start_date,
            end_date=args.end_date,
            year=args.year
        )
        
        # Compare values
        success = verifier.compare_values(verbose=args.verbose)
        if not success:
            sys.exit(1)
        
        # Generate report
        verifier.generate_report()
        
        # Exit with appropriate code
        if verifier.mismatches > 0:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()