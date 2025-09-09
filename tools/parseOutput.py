#!/usr/bin/env python3
"""
Optimized ORIGEN Parser using pandas with minimal DataFrame approach

This script uses pandas efficiently by:
1. Only creating DataFrames for the columns we actually need
2. Pre-filtering data before DataFrame creation
3. Using explicit data types to avoid inference overhead
4. Processing sections separately and combining results
"""

import re
import pandas as pd
from mendeleev import element
import json
import sqlite3
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedORIGENParser:
    def __init__(self, output_file_path, endf8_json_path="../endf8_isotopes.json"):
        self.output_file_path = output_file_path
        self.endf8_json_path = endf8_json_path
        self.available_isotopes = self.load_endf8_isotopes()
        self.helium_zaid = 2004
        self.element_cache_file = "element_cache.json"
        self.element_cache = self.load_element_cache()
        
    def load_endf8_isotopes(self):
        """Load available isotopes from ENDF8 JSON file"""
        try:
            json_path = Path(self.endf8_json_path)
            if not json_path.exists():
                script_dir = Path(__file__).parent
                json_path = script_dir.parent / "endf8_isotopes.json"
            
            with open(json_path, 'r') as f:
                data = json.load(f)
                isotopes_set = set(data['isotopes'])
                logger.info(f"Loaded {len(isotopes_set)} available isotopes from {json_path}")
                return isotopes_set
        except Exception as e:
            logger.warning(f"Could not load ENDF8 isotopes file: {e}")
            return set()
    
    def load_element_cache(self):
        """Load element atomic number cache from file"""
        try:
            cache_path = Path(self.element_cache_file)
            if cache_path.exists():
                with open(cache_path, 'r') as f:
                    cache = json.load(f)
                    logger.info(f"Loaded element cache with {len(cache)} elements from {cache_path}")
                    return cache
            else:
                logger.info("No element cache found, will create new cache")
                return {}
        except Exception as e:
            logger.warning(f"Could not load element cache: {e}")
            return {}
    
    def save_element_cache(self):
        """Save element atomic number cache to file"""
        try:
            with open(self.element_cache_file, 'w') as f:
                json.dump(self.element_cache, f, indent=2)
                logger.info(f"Saved element cache with {len(self.element_cache)} elements to {self.element_cache_file}")
        except Exception as e:
            logger.warning(f"Could not save element cache: {e}")
    
    def get_atomic_number(self, element_name):
        """Get atomic number with caching"""
        if element_name in self.element_cache:
            return self.element_cache[element_name]
        
        try:
            # This is the slow mendeleev call
            atomic_num = element(element_name).atomic_number
            self.element_cache[element_name] = atomic_num
            logger.debug(f"Cached atomic number for {element_name}: {atomic_num}")
            return atomic_num
        except Exception as e:
            logger.error(f"Could not get atomic number for {element_name}: {e}")
            return None
    
    def nuclide_to_zaid(self, nuclide_name):
        """Convert nuclide name to ZAID using MCNP metastable handling (ZA = Z*1000 + A + S*100)"""
        try:
            element_part, mass_part = re.split(r'[-_]', nuclide_name)
            element_name = element_part.strip().capitalize()
            
            # Use cached atomic number lookup instead of direct mendeleev call
            z = self.get_atomic_number(element_name)
            if z is None:
                return None
            
            # Extract mass number and handle metastable states
            mass_digits = ''.join(filter(str.isdigit, mass_part))
            a = int(mass_digits)
            
            # Handle metastable states according to MCNP manual
            if mass_part.endswith('m'):
                # Standard metastable: ZA = Z*1000 + A + 400 (S=1)
                zaid = z * 1000 + a + 400
            else:
                # Ground state: ZA = Z*1000 + A
                zaid = z * 1000 + a
            
            # Special case for Am-242: 95242 is metastable, 95642 is ground state
            if element_name == 'Am' and a == 242:
                if mass_part.endswith('m'):
                    zaid = 95242  # Am-242m (metastable)
                else:
                    zaid = 95642  # Am-242 (ground state)
                    
            return zaid
        except Exception as e:
            logger.error(f"Error converting {nuclide_name} to ZAID: {e}")
            return None
    
    def is_isotope_available(self, zaid):
        """Check if isotope is available in ENDF8 library"""
        if not self.available_isotopes:
            return True
        return zaid in self.available_isotopes
    
    def extract_section_data(self, content, section_keyword, case_name=None):
        """Extract and pre-process data from a specific section for a specific case"""
        logger.info(f"Processing section: {section_keyword}")
        
        # Find the section
        section_start = content.find(section_keyword)
        if section_start == -1:
            logger.warning(f"Section '{section_keyword}' not found")
            return pd.DataFrame()
        
        # Extract section content
        section_content = content[section_start:]
        
        # Find the end of this section (next section or end of content)
        next_sections = [
            "=   Nuclide concentrations in grams",
            "========================================="
        ]
        
        section_end = len(section_content)
        for next_section in next_sections:
            pos = section_content.find(next_section)
            if pos != -1 and section_content.find(next_section) != 0:  # Not this section itself
                section_end = min(section_end, pos)
        
        section_content = section_content[:section_end]
        lines = section_content.split('\n')
        
        # Find header line with time columns
        header_line = None
        data_start = 0
        
        for i, line in enumerate(lines):
            if re.match(r'^\s*\d+\.\d+E?[+-]?\d*min', line.strip()):
                header_line = line.strip()
                data_start = i + 1
                break
        
        if not header_line:
            logger.warning(f"No header found in section: {section_keyword}")
            return pd.DataFrame()
        
        # Get time columns and identify the last one
        time_columns = header_line.split()
        last_time_col = time_columns[-1]
        last_col_index = len(time_columns)  # Position in split array (after nuclide name)
        
        logger.info(f"Found {len(time_columns)} time columns, using last: {last_time_col}")
        
        # Pre-process data - extract only nuclide name and last time value
        clean_data = []
        for line in lines[data_start:]:
            line = line.strip()
            if not line or 'totals' in line.lower() or line.startswith('=') or line.startswith('-'):
                continue
                
            parts = line.split()
            if len(parts) < last_col_index + 1:  # +1 for nuclide name
                continue
                
            try:
                nuclide = parts[0]
                last_value = float(parts[last_col_index])  # Last time column value
                if last_value > 1e-12:  # Only keep significant values
                    clean_data.append([nuclide, last_value])
            except (ValueError, IndexError):
                continue
        
        if not clean_data:
            logger.warning(f"No valid data found in section: {section_keyword}")
            return pd.DataFrame()
        
        # Create minimal DataFrame with explicit dtypes
        df = pd.DataFrame(clean_data, columns=['Nuclide', 'Mass'])
        df = df.astype({
            'Nuclide': 'string',
            'Mass': 'float64'
        })
        
        # Add time column info and case info
        df['TimeColumn'] = last_time_col
        df['CaseName'] = case_name if case_name else 'default'
        
        logger.info(f"Extracted {len(df)} nuclides from section")
        return df
    
    def find_all_cases(self, content):
        """Find all case names in the ORIGEN output file"""
        import re
        case_pattern = r"Nuclide concentrations in grams for case '([^']+)'"
        cases = re.findall(case_pattern, content)
        unique_cases = list(set(cases))  # Remove duplicates
        logger.info(f"Found cases: {unique_cases}")
        return unique_cases
    
    def parse_all_sections(self, selected_case=None):
        """Parse all isotope sections using optimized pandas approach"""
        with open(self.output_file_path, 'r') as f:
            content = f.read()
        
        # Find all available cases
        all_cases = self.find_all_cases(content)
        
        if not all_cases:
            logger.warning("No cases found in output file")
            return pd.DataFrame()
        
        # Use selected case or the last case found
        target_case = selected_case if selected_case and selected_case in all_cases else all_cases[-1]
        logger.info(f"Processing case: {target_case}")
        
        # Define sections to look for - they include the case name in the header
        sections = [
            f"Nuclide concentrations in grams for case '{target_case}'",
            f"Nuclide concentrations in grams, light elements for case '{target_case}'",
            f"Nuclide concentrations in grams, actinides for case '{target_case}'", 
            f"Nuclide concentrations in grams, fission products for case '{target_case}'"
        ]
        
        all_dfs = []
        
        # Process each section
        for section in sections:
            try:
                df = self.extract_section_data(content, section)
                if not df.empty:
                    all_dfs.append(df)
                    logger.info(f"Successfully processed section: {section}")
            except Exception as e:
                logger.warning(f"Error processing section '{section}': {e}")
        
        if not all_dfs:
            raise ValueError(f"No data tables found in ORIGEN output file for case '{target_case}'")
        
        # Combine all sections efficiently
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Remove duplicates based on nuclide name, keeping the first occurrence
        combined_df = combined_df.drop_duplicates(subset='Nuclide', keep='first')
        
        logger.info(f"Combined data for case '{target_case}': {len(combined_df)} unique nuclides")
        return combined_df, target_case
    
    def generate_mcnp_materials(self, df, material_id=200, volume=18.24985393344441, case_name=None):
        """Generate MCNP material card from the processed DataFrame"""
        
        # Get the time column and case name (should be consistent across all rows)
        time_column = df['TimeColumn'].iloc[0]
        case_info = case_name if case_name else (df['CaseName'].iloc[0] if 'CaseName' in df.columns else 'unknown')
        logger.info(f"Generating material card for case '{case_info}', time: {time_column}")
        
        # Calculate total mass and density
        total_mass = df['Mass'].sum()
        density = total_mass / volume
        
        processed_zaids = {}
        helium_mass = 0.0
        
        logger.info(f"Processing {len(df)} nuclides")
        
        for _, row in df.iterrows():
            nuclide = row['Nuclide']
            mass = row['Mass']
            
            # Convert to ZAID
            zaid = self.nuclide_to_zaid(nuclide)
            if zaid is None:
                logger.warning(f"Skipping {nuclide}: could not convert to ZAID")
                helium_mass += mass
                continue
            
            # Check if isotope is available in ENDF8
            if not self.is_isotope_available(zaid):
                logger.info(f"Converting {nuclide} (ZAID {zaid}) to helium: not available in ENDF8")
                helium_mass += mass
                continue
            
            # Calculate weight fraction
            weight_fraction = mass / total_mass
            
            # Skip negligible weight fractions
            if weight_fraction < 1e-9:
                continue
            
            # Handle duplicates by summing masses
            if zaid in processed_zaids:
                logger.info(f"Duplicate ZAID {zaid} found, combining masses")
                processed_zaids[zaid] += weight_fraction
            else:
                processed_zaids[zaid] = weight_fraction
        
        # Add helium component if there was mass converted (always include)
        if helium_mass > 0:
            helium_fraction = helium_mass / total_mass
            processed_zaids[self.helium_zaid] = processed_zaids.get(self.helium_zaid, 0) + helium_fraction
            logger.info(f"Added {helium_fraction:.6e} weight fraction as helium ({helium_mass:.6e}g)")
        
        # Generate material card
        material_card = [
            f"c $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$",
            f"c Case: {case_info}",
            f"c Time column: {time_column}",
            f"c Total mass: {total_mass:.6e} g, Density: {density:.6e} g/cm³",
            f"c Isotopes converted to helium: {helium_mass:.6e} g",
            f"M{material_id} nlib=00c"
        ]
        
        # Add components in sorted order
        for zaid in sorted(processed_zaids.keys()):
            weight_fraction = processed_zaids[zaid]
            material_card.append(f"     {zaid} -{weight_fraction:.6e}")
        
        logger.info(f"Generated material card with {len(processed_zaids)} components")
        logger.info(f"Total mass: {total_mass:.6e} g, Density: {density:.6e} g/cm³")
        
        return material_card, {
            'case_name': case_info,
            'time_column': time_column,
            'total_mass_g': total_mass,
            'density_g_cm3': density,
            'helium_mass_g': helium_mass,
            'processed_zaids': processed_zaids,
            'df': df  # Include the original dataframe for database storage
        }
    
    def create_materials_database(self, db_path):
        """Create the materials database with proper schema"""
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            
            # Create materials table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS materials (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_number INTEGER,
                    element_name TEXT,
                    case_name TEXT,
                    time_point TEXT,
                    total_mass_g REAL,
                    density_g_cm3 REAL,
                    helium_mass_g REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create isotopes table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS isotopes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    material_id INTEGER,
                    isotope_name TEXT,
                    zaid INTEGER,
                    mass_grams REAL,
                    weight_fraction REAL,
                    FOREIGN KEY (material_id) REFERENCES materials(id)
                )
            ''')
            
            # Create index for faster queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_materials_cycle_element ON materials(cycle_number, element_name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_isotopes_material ON isotopes(material_id)')
            
            conn.commit()
            logger.info(f"Created materials database: {db_path}")
            
        finally:
            conn.close()
    
    def save_materials_to_database(self, db_path, cycle_number, materials_data):
        """Save material compositions to database"""
        self.create_materials_database(db_path)
        
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            
            for element_name, material_info in materials_data.items():
                # Insert material record
                cursor.execute('''
                    INSERT INTO materials (cycle_number, element_name, case_name, time_point, 
                                         total_mass_g, density_g_cm3, helium_mass_g)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    cycle_number,
                    element_name,
                    material_info['case_name'],
                    material_info['time_column'],
                    material_info['total_mass_g'],
                    material_info['density_g_cm3'],
                    material_info['helium_mass_g']
                ))
                
                material_id = cursor.lastrowid
                
                # Insert isotope records with mass in grams
                df = material_info['df']
                total_mass = material_info['total_mass_g']
                
                for _, row in df.iterrows():
                    nuclide = row['Nuclide']
                    mass_grams = row['Mass']
                    weight_fraction = mass_grams / total_mass
                    
                    # Convert nuclide to ZAID
                    zaid = self.nuclide_to_zaid(nuclide)
                    if zaid is None:
                        continue
                        
                    cursor.execute('''
                        INSERT INTO isotopes (material_id, isotope_name, zaid, mass_grams, weight_fraction)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (material_id, nuclide, zaid, mass_grams, weight_fraction))
                
                logger.debug(f"Saved {len(df)} isotopes for element {element_name}")
            
            conn.commit()
            logger.info(f"Saved materials for cycle {cycle_number} to database")
            
        finally:
            conn.close()
    
    def auto_generate_element_mapping(self, case_names):
        """Auto-generate mapping from case names to element names"""
        element_mapping = {}
        
        for i, case_name in enumerate(case_names, 1):
            # Clean up case name to create element identifier
            if 'Assembly' in case_name and 'Element' in case_name:
                # Use the existing format
                element_name = case_name
            else:
                # Generate generic element name
                element_name = f"element_{i:03d}"
            
            element_mapping[case_name] = element_name
            
        logger.info(f"Generated element mapping for {len(element_mapping)} elements")
        return element_mapping

def main():
    """Main execution function"""
    import argparse
    
    # Set up command line arguments
    parser_args = argparse.ArgumentParser(description='Parse ORIGEN output and generate MCNP material cards')
    parser_args.add_argument('--input-file', '-i', type=str, help='Input SCALE/ORIGEN output file to process')
    parser_args.add_argument('--endf8-json', type=str, help='Path to ENDF8 isotopes JSON file')
    parser_args.add_argument('--case', type=str, help='Specific case to process (e.g., fuelBurn, fuelBurn3)')
    parser_args.add_argument('--list-cases', action='store_true', help='List all available cases and exit')
    parser_args.add_argument('--all-cases', action='store_true', help='Process all cases and generate separate material cards')
    parser_args.add_argument('--output-dir', type=str, default='.', help='Output directory for material cards')
    parser_args.add_argument('--save-db', type=str, help='Save materials to database (provide database path)')
    parser_args.add_argument('--cycle', type=int, default=1, help='Cycle number for database storage')
    args = parser_args.parse_args()
    
    # Determine file paths
    if args.input_file:
        output_file_path = args.input_file
    else:
        # Default file paths (backwards compatibility)
        output_file_path = "scaleRun/PebbleDecay1.out"
        # Check if files exist and adjust paths
        if not Path(output_file_path).exists():
            output_file_path = "../scaleRun/PebbleDecay1.out"
    
    if args.endf8_json:
        endf8_json_path = args.endf8_json
    else:
        endf8_json_path = "endf8_isotopes.json"
        if not Path(endf8_json_path).exists():
            endf8_json_path = "../endf8_isotopes.json"
    
    try:
        # Initialize parser
        parser = OptimizedORIGENParser(output_file_path, endf8_json_path)
        
        # Read content to find cases
        with open(output_file_path, 'r') as f:
            content = f.read()
        all_cases = parser.find_all_cases(content)
        
        # Handle list-cases option
        if args.list_cases:
            print("Available cases in ORIGEN output:")
            for i, case in enumerate(all_cases, 1):
                print(f"  {i}. {case}")
            return
        
        # Determine which cases to process
        cases_to_process = []
        if args.all_cases:
            cases_to_process = all_cases
        elif args.case:
            if args.case in all_cases:
                cases_to_process = [args.case]
            else:
                print(f"Error: Case '{args.case}' not found. Available cases: {all_cases}")
                return
        else:
            # Default: process the last case
            cases_to_process = [all_cases[-1]] if all_cases else []
        
        if not cases_to_process:
            print("No cases to process")
            return
        
        # Collect all material cards
        all_material_cards = []
        all_summaries = []
        
        # Store all materials data for database
        all_materials_data = {}
        
        # Generate element mapping for all cases at once to avoid conflicts
        element_mapping = parser.auto_generate_element_mapping(cases_to_process)
        
        # Process each case
        for i, case in enumerate(cases_to_process):
            logger.info(f"Processing case: {case}")
            
            # Parse sections for this case
            combined_df, processed_case = parser.parse_all_sections(selected_case=case)
            
            # Generate MCNP material card with unique material ID
            material_id = 200 + i  # Start at M200, M201, etc.
            logger.info(f"Generating MCNP material card for case: {processed_case} with material ID: {material_id}")
            material_card, material_info = parser.generate_mcnp_materials(combined_df, material_id=material_id, case_name=processed_case)
            
            all_material_cards.append(material_card)
            
            # Use pre-generated element name for this case
            element_name = element_mapping[processed_case]
            all_materials_data[element_name] = material_info
            
            # Store summary info
            summary_info = {
                'case': processed_case,
                'element_name': element_name,
                'nuclides': len(combined_df),
                'material_id': material_id,
                'preview': material_card[:10]
            }
            all_summaries.append(summary_info)
        
        # Determine output file name
        if len(cases_to_process) == 1:
            output_material_file = Path(args.output_dir) / "mcnp_material_card.txt"
        else:
            output_material_file = Path(args.output_dir) / "mcnp_material_cards_all.txt"
        
        # Write all material cards to single file
        with open(output_material_file, "w") as f:
            f.write("c MCNP Material Cards from ORIGEN Output\n")
            f.write(f"c Generated from: {output_file_path}\n")
            f.write(f"c Number of cases: {len(cases_to_process)}\n")
            f.write("c\n")
            
            for i, material_card in enumerate(all_material_cards):
                if i > 0:
                    f.write("c\n")  # Separator between cards
                f.writelines([line + '\n' for line in material_card])
        
        logger.info(f"All material cards written to: {output_material_file}")
        
        # Save to database if requested
        if args.save_db and all_materials_data:
            try:
                parser.save_materials_to_database(args.save_db, args.cycle, all_materials_data)
                logger.info(f"Materials saved to database: {args.save_db}")
            except Exception as e:
                logger.error(f"Failed to save to database: {e}")
        
        # Print combined summary to console
        print("=" * 70)
        print(f"OPTIMIZED ORIGEN PARSER SUMMARY - All Cases")
        print("=" * 70)
        print(f"Input file: {output_file_path}")
        print(f"Output file: {output_material_file}")
        print(f"Cases processed: {len(cases_to_process)}")
        print(f"ENDF8 isotopes loaded: {len(parser.available_isotopes)}")
        print("")
        
        for summary in all_summaries:
            print(f"Case: {summary['case']} (Material ID: M{summary['material_id']})")
            print(f"  Element: {summary['element_name']}")
            print(f"  Nuclides processed: {summary['nuclides']}")
            print("  Material card preview:")
            for line in summary['preview']:
                print(f"    {line}")
            if len(summary['preview']) < 10:
                print("    ... (truncated)")
            print("")
        
        print("=" * 70)
        
        # Save element cache for future runs
        parser.save_element_cache()
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == '__main__':
    main()