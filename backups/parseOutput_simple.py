#!/usr/bin/env python3
"""
Simple ORIGEN Parser - focuses on just getting the last time column working
"""

import re
import json
from pathlib import Path
from mendeleev import element

class SimpleORIGENParser:
    def __init__(self, output_file_path, endf8_json_path="../endf8_isotopes.json"):
        self.output_file_path = output_file_path
        self.endf8_json_path = endf8_json_path
        self.available_isotopes = self.load_endf8_isotopes()
        self.helium_zaid = 2004
        
    def load_endf8_isotopes(self):
        """Load available isotopes from ENDF8 JSON file"""
        try:
            json_path = Path(self.endf8_json_path)
            if not json_path.exists():
                script_dir = Path(__file__).parent
                json_path = script_dir.parent / "endf8_isotopes.json"
            
            with open(json_path, 'r') as f:
                data = json.load(f)
                return set(data['isotopes'])
        except Exception as e:
            print(f"Warning: Could not load ENDF8 isotopes: {e}")
            return set()
    
    def nuclide_to_zaid(self, nuclide_name):
        """Convert nuclide name to ZAID"""
        try:
            element_part, mass_part = re.split(r'[-_]', nuclide_name)
            element_name = element_part.strip().capitalize()
            mass = ''.join(filter(str.isdigit, mass_part))
            
            if mass_part[-1].lower() == 'm':
                mass = str(int(mass) + 400)
            
            z = element(element_name).atomic_number
            return int(f"{z}{int(mass):03d}")
        except Exception:
            return None
    
    def parse_simple(self):
        """Simple parsing approach"""
        with open(self.output_file_path, 'r') as f:
            content = f.read()
        
        # Find the main section
        section_start = content.find("Nuclide concentrations in grams for case 'fuelBurn'")
        if section_start == -1:
            print("Could not find main section")
            return {}
        
        # Extract the section
        section_content = content[section_start:]
        next_section = section_content.find("=   Nuclide concentrations in grams, light elements")
        if next_section != -1:
            section_content = section_content[:next_section]
        
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
            print("Could not find header line")
            return {}
        
        # Parse time columns
        time_columns = header_line.split()
        last_time_col = time_columns[-1]
        print(f"Found {len(time_columns)} time columns, using last one: {last_time_col}")
        
        # Parse data
        nuclide_data = {}
        
        for line in lines[data_start:]:
            line = line.strip()
            if not line or 'totals' in line.lower() or line.startswith('=') or line.startswith('-'):
                continue
                
            parts = line.split()
            if len(parts) < len(time_columns) + 1:  # +1 for nuclide name
                continue
                
            nuclide_name = parts[0]
            try:
                # Get the value from the last time column
                last_value = float(parts[-1])  # Last column
                if last_value > 1e-12:  # Only store significant values
                    nuclide_data[nuclide_name] = last_value
            except ValueError:
                continue
        
        print(f"Parsed {len(nuclide_data)} nuclides from main section")
        return nuclide_data, last_time_col
    
    def generate_mcnp_card(self, nuclide_data, time_col, material_id=200):
        """Generate MCNP material card"""
        total_mass = sum(nuclide_data.values())
        volume = 9.7439768643435
        density = total_mass / volume
        
        processed_zaids = {}
        helium_mass = 0.0
        
        for nuclide, mass in nuclide_data.items():
            zaid = self.nuclide_to_zaid(nuclide)
            if zaid is None:
                print(f"Warning: Could not convert {nuclide} to ZAID")
                helium_mass += mass
                continue
                
            # Check ENDF8 availability
            if self.available_isotopes and zaid not in self.available_isotopes:
                print(f"Converting {nuclide} (ZAID {zaid}) to helium: not in ENDF8")
                helium_mass += mass
                continue
            
            weight_fraction = mass / total_mass
            if weight_fraction >= 1e-6:  # Only include significant fractions
                if zaid in processed_zaids:
                    processed_zaids[zaid] += weight_fraction
                else:
                    processed_zaids[zaid] = weight_fraction
        
        # Add helium if needed (always include if there's any helium mass)
        if helium_mass > 0:
            helium_fraction = helium_mass / total_mass
            processed_zaids[self.helium_zaid] = processed_zaids.get(self.helium_zaid, 0) + helium_fraction
            print(f"Added helium: {helium_fraction:.6e} weight fraction ({helium_mass:.6e} g)")
        
        # Generate card
        card_lines = [
            f"! MCNP Material Card from ORIGEN Output",
            f"! Time column: {time_col}",
            f"! Total mass: {total_mass:.6e} g",
            f"! Density: {density:.6e} g/cm³",
            f"! Isotopes converted to helium: {helium_mass:.6e} g",
            f"M{material_id} nlib=00c"
        ]
        
        for zaid in sorted(processed_zaids.keys()):
            weight_frac = processed_zaids[zaid]
            card_lines.append(f"     {zaid} -{weight_frac:.6e}")
        
        return '\n'.join(card_lines)

def main():
    parser = SimpleORIGENParser("scaleRun/PebbleDecay1.out")
    nuclide_data, time_col = parser.parse_simple()
    
    if nuclide_data:
        mcnp_card = parser.generate_mcnp_card(nuclide_data, time_col)
        
        with open("mcnp_material_card_simple.txt", "w") as f:
            f.write(mcnp_card)
        
        print(f"Generated material card with {len(nuclide_data)} components")
        print(f"Written to: mcnp_material_card_simple.txt")
        print("\nCard preview:")
        print(mcnp_card)
    else:
        print("No data found")

if __name__ == "__main__":
    main()