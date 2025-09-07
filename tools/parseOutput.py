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
    
    def extract_section_data(self, content, section_keyword):
        """Extract and pre-process data from a specific section"""
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
            "=   Nuclide concentrations in grams, light elements",
            "=   Nuclide concentrations in grams, actinides", 
            "=   Nuclide concentrations in grams, fission products",
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
            if re.match(r'^\s*\d+\.\d+E[+-]\d+min', line.strip()):
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
        
        # Add time column info
        df['TimeColumn'] = last_time_col
        
        logger.info(f"Extracted {len(df)} nuclides from section")
        return df
    
    def parse_all_sections(self):
        """Parse all isotope sections using optimized pandas approach"""
        with open(self.output_file_path, 'r') as f:
            content = f.read()
        
        sections = [
            "Nuclide concentrations in grams for case 'fuelBurn'",
            "Nuclide concentrations in grams, light elements",
            "Nuclide concentrations in grams, actinides", 
            "Nuclide concentrations in grams, fission products"
        ]
        
        all_dfs = []
        
        for section in sections:
            try:
                df = self.extract_section_data(content, section)
                if not df.empty:
                    all_dfs.append(df)
                    logger.info(f"Successfully processed section: {section}")
            except Exception as e:
                logger.warning(f"Error processing section '{section}': {e}")
        
        if not all_dfs:
            raise ValueError("No data tables found in ORIGEN output file")
        
        # Combine all sections efficiently
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Remove duplicates based on nuclide name, keeping the first occurrence
        combined_df = combined_df.drop_duplicates(subset='Nuclide', keep='first')
        
        logger.info(f"Combined data: {len(combined_df)} unique nuclides")
        return combined_df
    
    def generate_mcnp_materials(self, df, material_id=200, volume=9.7439768643435):
        """Generate MCNP material card from the processed DataFrame"""
        
        # Get the time column (should be consistent across all rows)
        time_column = df['TimeColumn'].iloc[0]
        logger.info(f"Generating material card for time: {time_column}")
        
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
            if weight_fraction < 1e-6:
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
            f"! MCNP Material Card from ORIGEN Output",
            f"! Time column: {time_column}",
            f"! Total mass: {total_mass:.6e} g, Density: {density:.6e} g/cm³",
            f"! Isotopes converted to helium: {helium_mass:.6e} g",
            f"M{material_id} nlib=00c"
        ]
        
        # Add components in sorted order
        for zaid in sorted(processed_zaids.keys()):
            weight_fraction = processed_zaids[zaid]
            material_card.append(f"     {zaid} -{weight_fraction:.6e}")
        
        logger.info(f"Generated material card with {len(processed_zaids)} components")
        logger.info(f"Total mass: {total_mass:.6e} g, Density: {density:.6e} g/cm³")
        
        return material_card

def main():
    """Main execution function"""
    # File paths
    output_file_path = "scaleRun/PebbleDecay1.out"
    endf8_json_path = "endf8_isotopes.json" 
    output_material_file = "mcnp_material_card.txt"
    
    # Check if files exist and adjust paths
    if not Path(output_file_path).exists():
        output_file_path = "../scaleRun/PebbleDecay1.out"
    if not Path(endf8_json_path).exists():
        endf8_json_path = "../endf8_isotopes.json"
    
    try:
        # Initialize parser
        parser = OptimizedORIGENParser(output_file_path, endf8_json_path)
        
        # Parse all sections and combine data
        logger.info("Starting optimized ORIGEN output parsing...")
        combined_df = parser.parse_all_sections()
        
        # Generate MCNP material card
        logger.info("Generating MCNP material card...")
        material_card = parser.generate_mcnp_materials(combined_df, material_id=200)
        
        # Save element cache for future runs
        parser.save_element_cache()
        
        # Write to output file
        with open(output_material_file, "w") as f:
            f.writelines([line + '\n' for line in material_card])
        
        logger.info(f"Material card written to: {output_material_file}")
        
        # Print summary to console
        print("=" * 60)
        print("OPTIMIZED ORIGEN PARSER SUMMARY")
        print("=" * 60)
        print(f"Input file: {output_file_path}")
        print(f"Output file: {output_material_file}")
        print(f"Total nuclides processed: {len(combined_df)}")
        print(f"ENDF8 isotopes loaded: {len(parser.available_isotopes)}")
        print("\nMaterial card preview:")
        print("-" * 40)
        for line in material_card[:10]:
            print(line)
        if len(material_card) > 10:
            print("... (truncated)")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == '__main__':
    main()