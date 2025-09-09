#!/usr/bin/env python3
"""
Simple iteration tracking utility for manual burnup cycles.
Provides cycle status, material database inspection, and workflow assistance.
"""

import sqlite3
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json
from datetime import datetime

class IterationTracker:
    """Simple tracking utility for manual burnup iterations."""
    
    def __init__(self, db_path: str = "materials.db"):
        """
        Initialize tracker with database path.
        
        Args:
            db_path: Path to the materials database
        """
        self.db_path = Path(db_path)
        
    def get_cycle_status(self) -> Dict[str, any]:
        """
        Get current cycle status from database.
        
        Returns:
            Dictionary with cycle information
        """
        if not self.db_path.exists():
            return {
                'database_exists': False,
                'latest_cycle': None,
                'total_materials': 0,
                'elements': []
            }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get latest cycle number
                cursor.execute("SELECT MAX(cycle_number) FROM materials")
                latest_cycle = cursor.fetchone()[0]
                
                # Get total materials count
                cursor.execute("SELECT COUNT(*) FROM materials")
                total_materials = cursor.fetchone()[0]
                
                # Get elements in latest cycle
                if latest_cycle is not None:
                    cursor.execute("""
                        SELECT element_name, case_name, total_mass_g 
                        FROM materials 
                        WHERE cycle_number = ?
                        ORDER BY element_name
                    """, (latest_cycle,))
                    elements = cursor.fetchall()
                else:
                    elements = []
                
                return {
                    'database_exists': True,
                    'latest_cycle': latest_cycle,
                    'total_materials': total_materials,
                    'elements': elements
                }
        except Exception as e:
            return {
                'database_exists': True,
                'error': str(e),
                'latest_cycle': None,
                'total_materials': 0,
                'elements': []
            }
    
    def list_all_cycles(self) -> List[Tuple[int, int, str]]:
        """
        List all cycles with material counts.
        
        Returns:
            List of (cycle_number, material_count, date) tuples
        """
        if not self.db_path.exists():
            return []
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT cycle_number, COUNT(*) as material_count, 
                           MIN(time_point) as earliest_time
                    FROM materials 
                    GROUP BY cycle_number 
                    ORDER BY cycle_number
                """)
                return cursor.fetchall()
        except Exception:
            return []
    
    def get_element_history(self, element_name: str) -> List[Dict[str, any]]:
        """
        Get history of an element across all cycles.
        
        Args:
            element_name: Name of element to track
            
        Returns:
            List of dictionaries with cycle information
        """
        if not self.db_path.exists():
            return []
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT cycle_number, total_mass_g, density_g_cm3, 
                           helium_mass_g, time_point, case_name
                    FROM materials 
                    WHERE element_name = ?
                    ORDER BY cycle_number
                """, (element_name,))
                
                results = []
                for row in cursor.fetchall():
                    results.append({
                        'cycle': row[0],
                        'total_mass_g': row[1],
                        'density_g_cm3': row[2],
                        'helium_mass_g': row[3],
                        'time_point': row[4],
                        'case_name': row[5]
                    })
                return results
        except Exception:
            return []
    
    def suggest_next_step(self) -> str:
        """
        Suggest next step in manual iteration workflow.
        
        Returns:
            Suggested next action as string
        """
        status = self.get_cycle_status()
        
        if not status['database_exists']:
            return """
SUGGESTED NEXT STEP:
1. Run parseOutput.py to create initial material database from SCALE output
   Command: python tools/parseOutput.py
   
This will parse the SCALE output and create the materials database.
"""
        
        if status.get('error'):
            return f"""
DATABASE ERROR: {status['error']}
Check database integrity and fix any issues before proceeding.
"""
        
        if status['latest_cycle'] is None:
            return """
SUGGESTED NEXT STEP:
1. Database exists but contains no materials
2. Run parseOutput.py to populate with initial data
   Command: python tools/parseOutput.py
"""
        
        latest_cycle = status['latest_cycle']
        element_count = len(status['elements'])
        
        return f"""
CURRENT STATUS: Cycle {latest_cycle} complete with {element_count} elements

SUGGESTED NEXT STEP:
1. Generate SCALE input using cycle {latest_cycle} materials:
   Command: python generate_scale_input.py --flux-json sample_flux_data.json --power-time sample_origen_cards.txt --materials-db materials.db --cycle {latest_cycle}

2. Run SCALE simulation with generated input file

3. After SCALE completes, parse output for cycle {latest_cycle + 1}:
   Command: python tools/parseOutput.py --cycle {latest_cycle + 1}

4. Update flux data for next iteration (manual MCNP step)
"""
    
    def print_status(self, verbose: bool = False):
        """
        Print current iteration status.
        
        Args:
            verbose: Include detailed information
        """
        status = self.get_cycle_status()
        
        print("=" * 60)
        print("BURNUP ITERATION TRACKER")
        print("=" * 60)
        
        if not status['database_exists']:
            print("❌ Materials database not found")
            print(f"   Expected location: {self.db_path.absolute()}")
            print("\n" + self.suggest_next_step())
            return
        
        if status.get('error'):
            print(f"❌ Database error: {status['error']}")
            return
        
        print(f"✅ Database found: {self.db_path}")
        print(f"📊 Total materials: {status['total_materials']}")
        
        if status['latest_cycle'] is not None:
            print(f"🔄 Latest cycle: {status['latest_cycle']}")
            print(f"🧪 Elements in latest cycle: {len(status['elements'])}")
            
            if verbose and status['elements']:
                print("\nELEMENT DETAILS:")
                for element_name, case_name, total_mass in status['elements']:
                    print(f"  {element_name:<15} ({case_name:<20}) {total_mass:8.3f}g")
        else:
            print("🔄 No cycles found in database")
        
        if verbose:
            cycles = self.list_all_cycles()
            if cycles:
                print(f"\nCYCLE HISTORY ({len(cycles)} cycles):")
                for cycle, count, time_point in cycles:
                    print(f"  Cycle {cycle:2d}: {count:2d} materials (time: {time_point})")
        
        print("\n" + self.suggest_next_step())

def main():
    """Main command line interface."""
    parser = argparse.ArgumentParser(
        description="Track burnup iteration progress",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python iteration_tracker.py                    # Show current status
  python iteration_tracker.py -v                 # Show detailed status
  python iteration_tracker.py --db custom.db     # Use custom database
  python iteration_tracker.py --element elem_001 # Track specific element
  python iteration_tracker.py --cycles           # List all cycles
        """
    )
    
    parser.add_argument(
        '--database', '--db',
        default='materials.db',
        help='Path to materials database (default: materials.db)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show detailed information'
    )
    
    parser.add_argument(
        '--element',
        help='Show history for specific element'
    )
    
    parser.add_argument(
        '--cycles',
        action='store_true',
        help='List all cycles'
    )
    
    args = parser.parse_args()
    
    tracker = IterationTracker(args.database)
    
    if args.element:
        history = tracker.get_element_history(args.element)
        if not history:
            print(f"No history found for element: {args.element}")
            return
        
        print(f"ELEMENT HISTORY: {args.element}")
        print("=" * 50)
        for entry in history:
            print(f"Cycle {entry['cycle']:2d}: {entry['total_mass_g']:8.3f}g "
                  f"(He: {entry['helium_mass_g']:8.6f}g, ρ: {entry['density_g_cm3']:.3f}g/cm³)")
    
    elif args.cycles:
        cycles = tracker.list_all_cycles()
        if not cycles:
            print("No cycles found in database")
            return
        
        print("ALL CYCLES")
        print("=" * 40)
        for cycle, count, time_point in cycles:
            print(f"Cycle {cycle:2d}: {count:2d} materials (time: {time_point})")
    
    else:
        tracker.print_status(verbose=args.verbose)

if __name__ == '__main__':
    main()