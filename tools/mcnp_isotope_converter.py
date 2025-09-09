#!/usr/bin/env python3
"""
MCNP Material Converter: Elemental to Isotopic Forms
Converts MCNP elemental material definitions to their isotopic forms
based on available isotopes in specified nuclear data libraries.
Now uses mendeleev library for comprehensive isotope data.
"""

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

try:
    from mendeleev import element
except ImportError:
    print("Error: mendeleev package not installed")
    print("Install with: pip install mendeleev")
    sys.exit(1)


# Custom Exceptions
class MCNPConverterError(Exception):
    """Base exception for MCNP converter errors."""
    pass


class ElementNotFoundError(MCNPConverterError):
    """Raised when an element cannot be found or loaded."""
    pass


class InvalidMaterialCardError(MCNPConverterError):
    """Raised when a material card is malformed or invalid."""
    pass


class LibraryDataError(MCNPConverterError):
    """Raised when there are issues with nuclear data library files."""
    pass


class IsotopeNotAvailableError(MCNPConverterError):
    """Raised when required isotopes are not available in the specified library."""
    pass


# Constants
DEFAULT_LIBRARY_SUFFIX = ".00c"
SUPPORTED_LIBRARIES = [".00c", ".70c", ".31c"]
ISOTOPE_FILE_MAP = {
    ".00c": "endf8_isotopes.json",
    ".70c": "endf7_isotopes.json", 
    ".31c": "jendl31_isotopes.json"
}
LIBRARY_DESCRIPTIONS = {
    ".00c": "ENDF/B-VIII.0",
    ".70c": "ENDF/B-VII.0", 
    ".31c": "JENDL-3.1"
}
MAX_ATOMIC_NUMBER = 118
ZAID_MULTIPLIER = 1000  # For converting Z to elemental ZAID (Z000)


@dataclass
class Isotope:
    """Represents a single isotope"""
    zaid: int  # ZZZAAA format (e.g., 92235 for U-235)
    abundance: float  # Natural abundance (fraction, not percentage)
    
    
@dataclass
class Element:
    """Represents an element with its isotopes"""
    symbol: str
    z_number: int
    name: str
    isotopes: List[Isotope]


class MCNPMaterialConverter:
    """
    MCNP Material Converter: Converts elemental material definitions to isotopic forms.
    
    This converter transforms MCNP material cards from elemental form (e.g., 26000 for iron)
    to their natural isotopic distributions (e.g., Fe-54, Fe-56, Fe-57, Fe-58) using 
    comprehensive isotope data from the mendeleev library.
    
    Features:
        - Uses mendeleev library for accurate natural abundance data
        - Supports multiple nuclear data libraries (.00c, .70c, .31c)
        - Preserves comments from original material cards
        - Handles mixed isotopic/elemental materials
        - Validates isotope availability against library files
        - Clean one-isotope-per-line output formatting
    
    Example:
        ```python
        # Standard format (suffix on each isotope)
        converter = MCNPMaterialConverter(library_suffix=".00c")
        water = "m1 1000 2\\n    8000 1"
        result = converter.convert_material(water)
        # Output:
        # m1 1001.00c 1.999710e+00
        #      1002.00c 2.900000e-04  
        #      8016.00c 9.975715e-01
        #      8017.00c 3.835006e-04
        #      8018.00c 2.045003e-03
        
        # Alternative nlib format
        converter = MCNPMaterialConverter(library_suffix=".00c", use_nlib=True)
        result = converter.convert_material(water)
        # Output:
        # m1 1001 1.999710e+00
        #      1002 2.900000e-04
        #      8016 9.975715e-01
        #      8017 3.835006e-04
        #      8018 2.045003e-03
        #      nlib=00c
        ```
    
    Args:
        library_suffix: Nuclear data library suffix (default: ".00c" for ENDF/B-VIII.0)
        
    Raises:
        ElementNotFoundError: When element data cannot be loaded from mendeleev
        InvalidMaterialCardError: When material card format is invalid  
        IsotopeNotAvailableError: When isotopes are not available in specified library
        LibraryDataError: When nuclear data library files cannot be loaded
    """
    
    def __init__(self, library_suffix: str = DEFAULT_LIBRARY_SUFFIX, use_nlib: bool = False):
        """
        Initialize the converter with a specific library suffix.
        
        Args:
            library_suffix: The library suffix (e.g., ".00c", ".70c", ".31c")
            use_nlib: If True, output 'nlib=XXc' instead of adding suffix to each isotope
            
        Raises:
            LibraryDataError: If library suffix is not supported
        """
        if library_suffix not in SUPPORTED_LIBRARIES:
            raise LibraryDataError(
                f"Unsupported library suffix '{library_suffix}'. "
                f"Supported libraries: {', '.join(SUPPORTED_LIBRARIES)}"
            )
            
        self.library_suffix = library_suffix
        self.use_nlib = use_nlib
        self.elements_db = {}  # Load elements on demand
        self.available_isotopes = self._initialize_available_isotopes()
        
    def _load_element(self, z_number: int) -> Optional[Element]:
        """
        Load element data from mendeleev on demand.
        
        Args:
            z_number: Atomic number (must be 1-118)
            
        Returns:
            Element object or None if element has no natural isotopes
            
        Raises:
            ElementNotFoundError: If atomic number is invalid or element cannot be loaded
        """
        # Input validation
        if not isinstance(z_number, int) or z_number < 1 or z_number > MAX_ATOMIC_NUMBER:
            raise ElementNotFoundError(
                f"Invalid atomic number: {z_number}. Must be integer between 1-{MAX_ATOMIC_NUMBER}"
            )
        
        # Return cached element if available
        if z_number in self.elements_db:
            return self.elements_db[z_number]
        
        try:
            elem = element(z_number)
            isotopes = []
            
            # Get all natural isotopes from mendeleev
            for isotope in elem.isotopes:
                if isotope.abundance is not None and isotope.abundance > 0:
                    # Create ZAID in MCNP format (ZZZAAA)
                    zaid = z_number * ZAID_MULTIPLIER + isotope.mass_number
                    # Convert abundance from percentage to fraction
                    abundance = isotope.abundance / 100.0
                    isotopes.append(Isotope(zaid, abundance))
            
            # Only add element if it has natural isotopes
            if isotopes:
                self.elements_db[z_number] = Element(
                    elem.symbol, 
                    z_number, 
                    elem.name, 
                    isotopes
                )
                return self.elements_db[z_number]
            # Special cases for elements without natural isotopes but common in MCNP
            elif z_number == 94:  # Plutonium
                self.elements_db[94] = Element("Pu", 94, "Plutonium", [
                    Isotope(94238, 0.0),
                    Isotope(94239, 0.0),  # User must specify enrichment
                    Isotope(94240, 0.0),
                    Isotope(94241, 0.0),
                    Isotope(94242, 0.0)
                ])
                return self.elements_db[94]
            else:
                # Element exists but has no natural isotopes (e.g., Tc, Pm)
                return None
                
        except Exception as e:
            raise ElementNotFoundError(
                f"Could not load element data for Z={z_number} ({elem.symbol if 'elem' in locals() else 'unknown'}): {e}"
            )
    
    def _initialize_available_isotopes(self) -> Dict[str, List[int]]:
        """
        Initialize available isotopes for different libraries.
        Loads from configuration files when available.
        
        Returns:
            Dictionary mapping library suffixes to lists of available isotope ZAIDs
            
        Raises:
            LibraryDataError: If critical library files cannot be loaded
        """
        import os
        
        # Initialize with empty lists for all supported libraries
        available = {lib: [] for lib in SUPPORTED_LIBRARIES}
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Try to load isotope data files for each library
        for lib_suffix in SUPPORTED_LIBRARIES:
            if lib_suffix in ISOTOPE_FILE_MAP:
                isotope_file = os.path.join(script_dir, ISOTOPE_FILE_MAP[lib_suffix])
                
                if os.path.exists(isotope_file):
                    try:
                        with open(isotope_file, 'r') as f:
                            data = json.load(f)
                            available[lib_suffix] = data.get("isotopes", [])
                            lib_name = LIBRARY_DESCRIPTIONS.get(lib_suffix, lib_suffix)
                            print(f"Loaded {len(available[lib_suffix])} isotopes for {lib_name}")
                    except (json.JSONDecodeError, IOError) as e:
                        # For current library, this is critical; for others, just warn
                        if lib_suffix == self.library_suffix:
                            raise LibraryDataError(
                                f"Could not load critical isotope data for {lib_suffix}: {e}"
                            )
                        else:
                            print(f"Warning: Could not load isotope data for {lib_suffix}: {e}")
                else:
                    # Missing file for current library is just a warning (will use all isotopes)
                    lib_name = LIBRARY_DESCRIPTIONS.get(lib_suffix, lib_suffix)
                    if lib_suffix == self.library_suffix:
                        print(f"Warning: Isotope file not found for {lib_name}. Will assume all isotopes are available.")
        
        return available
    
    def parse_material_card(self, material_card: str) -> Tuple[List[Tuple[int, float]], Dict[int, str]]:
        """
        Parse an MCNP material card to extract ZAIDs, fractions, and comments.
        
        Args:
            material_card: The material card string (must be non-empty)
            
        Returns:
            Tuple of (List of (ZAID, fraction) tuples, Dict of {ZAID: comment})
            
        Raises:
            InvalidMaterialCardError: If material card format is invalid
        """
        # Input validation
        if not isinstance(material_card, str):
            raise InvalidMaterialCardError("Material card must be a string")
        
        if not material_card.strip():
            raise InvalidMaterialCardError("Material card cannot be empty")
        
        materials = []
        comments = {}
        lines = material_card.strip().split('\n')
        
        # Check if we have at least one material definition line
        has_material_data = False
        
        for line_num, line in enumerate(lines, 1):
            # Skip comment lines and empty lines
            if line.strip().startswith('c') or not line.strip():
                continue
            
            # Extract inline comment if present
            comment = ""
            if '$' in line:
                parts = line.split('$', 1)
                line = parts[0]
                comment = parts[1].strip()
            
            # Parse ZAID and fraction pairs
            tokens = line.split()
            i = 0
            
            while i < len(tokens):
                if tokens[i].startswith('m'):
                    # Skip material card number
                    i += 1
                    continue
                
                # Skip non-numeric tokens (like 'nlib=00c')
                if not tokens[i].replace('.', '').replace('-', '').replace('+', '').replace('e', '').replace('E', '').isdigit():
                    i += 1
                    continue
                    
                try:
                    # Parse ZAID (might have library suffix)
                    zaid_str = tokens[i]
                    if '.' in zaid_str:
                        zaid = int(zaid_str.split('.')[0])
                    else:
                        zaid = int(zaid_str)
                    
                    # Validate ZAID format
                    if zaid <= 0 or zaid > 999999:
                        raise InvalidMaterialCardError(
                            f"Invalid ZAID {zaid} on line {line_num}. Must be positive integer ≤ 999999"
                        )
                    
                    # Parse fraction
                    if i + 1 < len(tokens):
                        try:
                            fraction = float(tokens[i + 1])
                        except ValueError:
                            raise InvalidMaterialCardError(
                                f"Invalid fraction '{tokens[i + 1]}' for ZAID {zaid} on line {line_num}"
                            )
                    else:
                        fraction = 1.0
                    
                    # Validate fraction is not zero (MCNP doesn't allow zero fractions)
                    if fraction == 0.0:
                        raise InvalidMaterialCardError(
                            f"Zero fraction not allowed for ZAID {zaid} on line {line_num}"
                        )
                    
                    materials.append((zaid, fraction))
                    has_material_data = True
                    
                    # Associate comment with this ZAID if present
                    if comment:
                        comments[zaid] = comment
                    
                    i += 2
                    
                except (ValueError, IndexError) as e:
                    if "Invalid" in str(e):
                        raise  # Re-raise our custom validation errors
                    # Skip malformed tokens but continue parsing
                    i += 1
        
        # Validate that we found at least one material
        if not has_material_data:
            raise InvalidMaterialCardError("No valid material data found in material card")
        
        return materials, comments
    
    def convert_element_to_isotopes(self, z_number: int, fraction: float) -> List[Tuple[int, float]]:
        """
        Convert an elemental ZAID to its isotopic components.
        
        Args:
            z_number: Atomic number (Z)
            fraction: Material fraction for this element
            
        Returns:
            List of (isotope_ZAID, adjusted_fraction) tuples
        """
        # Load element data on demand
        element = self._load_element(z_number)
        if not element:
            raise ValueError(f"Element with Z={z_number} not found or has no natural isotopes")
        isotopes = []
        unavailable = []
        
        # Check which isotopes are available in the library
        available_in_lib = self.available_isotopes.get(self.library_suffix, [])
        
        total_available_abundance = 0.0
        for isotope in element.isotopes:
            # If no library data loaded (empty list), include all isotopes
            # Otherwise, only include isotopes that are in the library
            if len(available_in_lib) == 0 or isotope.zaid in available_in_lib:
                total_available_abundance += isotope.abundance
            else:
                unavailable.append(isotope.zaid)
        
        # Renormalize abundances if some isotopes are unavailable
        for isotope in element.isotopes:
            if len(available_in_lib) == 0 or isotope.zaid in available_in_lib:
                # Calculate the fraction for this isotope
                if total_available_abundance > 0:
                    # Renormalize to account for missing isotopes
                    renormalized_abundance = isotope.abundance / total_available_abundance
                    isotope_fraction = fraction * renormalized_abundance
                else:
                    isotope_fraction = fraction * isotope.abundance
                    
                if isotope_fraction != 0:  # Include all isotopes except exactly zero
                    isotopes.append((isotope.zaid, isotope_fraction))
        
        if unavailable:
            print(f"Warning: Isotopes {unavailable} for element {element.symbol} not available in {self.library_suffix}")
            print(f"         Abundances renormalized over available isotopes")
        
        if not isotopes:
            raise ValueError(f"No isotopes available for element Z={z_number} in library {self.library_suffix}")
        
        return isotopes
    
    def convert_material(self, material_card: str, handle_missing: str = "warn") -> str:
        """
        Convert an MCNP material card from elemental to isotopic form.
        
        Args:
            material_card: The original material card
            handle_missing: How to handle missing isotopes ("warn", "skip", "error")
            
        Returns:
            Converted material card string
        """
        materials, original_comments = self.parse_material_card(material_card)
        converted = []
        converted_comments = {}
        warnings = []
        
        # Extract material number if present
        mat_num = ""
        if material_card.strip().startswith('m'):
            mat_num = material_card.strip().split()[0] + " "
        
        for zaid, fraction in materials:
            # Check if this is an elemental form (Z000)
            if zaid % 1000 == 0:
                z_number = zaid // 1000
                try:
                    isotopes = self.convert_element_to_isotopes(z_number, fraction)
                    
                    # Add comment only to the first isotope of this element
                    if zaid in original_comments and isotopes:
                        comment = original_comments[zaid]
                        # Get element symbol for comment
                        elem = self._load_element(z_number)
                        if elem:
                            # Modify comment to indicate natural isotopes
                            if "natural" not in comment.lower():
                                comment = f"{elem.symbol} natural ({comment})" if comment else f"{elem.symbol} natural"
                        
                        # Only add comment to the first isotope
                        first_isotope_zaid = isotopes[0][0]
                        converted_comments[first_isotope_zaid] = comment
                    
                    converted.extend(isotopes)
                except ValueError as e:
                    if handle_missing == "error":
                        raise
                    elif handle_missing == "warn":
                        warnings.append(str(e))
                        converted.append((zaid, fraction))  # Keep original
                        if zaid in original_comments:
                            converted_comments[zaid] = original_comments[zaid]
                    # elif handle_missing == "skip": do nothing
            else:
                # Already isotopic, keep as is
                converted.append((zaid, fraction))
                if zaid in original_comments:
                    converted_comments[zaid] = original_comments[zaid]
        
        # Format output - one isotope per line with comments
        output_lines = [mat_num]
        for i, (zaid, fraction) in enumerate(converted):
            if i > 0:
                output_lines.append("\n     ")
            
            # Add comment if available
            comment_str = ""
            if zaid in converted_comments:
                comment_str = f" $ {converted_comments[zaid]}"
            
            # Format ZAID with or without suffix
            if self.use_nlib:
                output_lines.append(f"{zaid} {fraction:.6e}{comment_str}")
            else:
                output_lines.append(f"{zaid}{self.library_suffix} {fraction:.6e}{comment_str}")
        
        # Add nlib directive if using nlib format
        if self.use_nlib:
            nlib_suffix = self.library_suffix[1:]  # Remove the dot
            result = "".join(output_lines) + f"\n     nlib={nlib_suffix}"
        else:
            result = "".join(output_lines)
        
        if warnings:
            result += "\nc Warnings during conversion:\n"
            for warning in warnings:
                result += f"c   {warning}\n"
        
        return result.strip()
    
    def set_custom_isotopes(self, z_number: int, isotopes: List[Tuple[int, float]]):
        """
        Set custom isotope distributions for an element.
        
        Args:
            z_number: Atomic number
            isotopes: List of (ZAID, abundance) tuples
        """
        if z_number not in self.elements_db:
            self.elements_db[z_number] = Element("Custom", z_number, f"Element-{z_number}", [])
        
        self.elements_db[z_number].isotopes = [
            Isotope(zaid, abundance) for zaid, abundance in isotopes
        ]


def main():
    parser = argparse.ArgumentParser(
        description="Convert MCNP materials from elemental to isotopic form",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert a material card from stdin
  echo "m1 1000 2 8000 1" | python %(prog)s
  
  # Convert from file
  python %(prog)s -i material.txt -o converted.txt
  
  # Use specific library
  python %(prog)s -l .70c -i material.txt
  
  # Use nlib format instead of suffixes on each isotope
  python %(prog)s --use-nlib -i material.txt
  
  # Specify library isotope file
  python %(prog)s --library-file endf7_isotopes.json -i material.txt
        """
    )
    
    parser.add_argument('-i', '--input', help='Input file (default: stdin)')
    parser.add_argument('-o', '--output', help='Output file (default: stdout)')
    parser.add_argument('-l', '--library', default='.00c',
                       help='Library suffix (default: .00c)')
    parser.add_argument('--library-file', 
                       help='JSON file with available isotopes for library')
    parser.add_argument('--handle-missing', choices=['warn', 'skip', 'error'],
                       default='warn',
                       help='How to handle missing isotopes (default: warn)')
    parser.add_argument('--use-nlib', action='store_true',
                       help='Use nlib=XXc format instead of adding suffix to each isotope')
    parser.add_argument('--demo', action='store_true',
                       help='Run demonstration examples')
    
    args = parser.parse_args()
    
    if args.demo:
        run_demo()
        return
    
    # Create converter
    converter = MCNPMaterialConverter(
        library_suffix=args.library,
        use_nlib=args.use_nlib
    )
    
    # Read input
    if args.input:
        with open(args.input, 'r') as f:
            material_card = f.read()
    else:
        print("Enter material card (Ctrl+D when done):", file=sys.stderr)
        material_card = sys.stdin.read()
    
    # Convert
    try:
        converted = converter.convert_material(
            material_card, 
            handle_missing=args.handle_missing
        )
        
        # Write output
        if args.output:
            with open(args.output, 'w') as f:
                f.write(converted)
            print(f"Converted material written to {args.output}", file=sys.stderr)
        else:
            print(converted)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def run_demo():
    """Run demonstration examples"""
    converter = MCNPMaterialConverter(library_suffix=".00c")
    
    examples = [
        ("Water (H2O)", "m1  1000  2\n     8000  1"),
        ("Stainless Steel", "m2  26000  -0.70\n     24000  -0.18\n     28000  -0.12"),
        ("Enriched Uranium (LEU)", "m3  92235  0.05\n     92238  0.95"),
        ("Concrete", "m4  1000  -0.01\n     8000  -0.529\n     11000 -0.016\n     13000 -0.034\n     14000 -0.337\n     20000 -0.044\n     26000 -0.014"),
        ("Air", "m5  7000  -0.755\n     8000  -0.232\n     18000 -0.013"),
    ]
    
    for title, material in examples:
        print(f"\n{'='*60}")
        print(f"Example: {title}")
        print(f"{'='*60}")
        print("Original (elemental):")
        print(material)
        print("\nConverted (isotopic):")
        try:
            converted = converter.convert_material(material)
            print(converted)
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()