#!/usr/bin/env python3
"""
Chemical Compound Isotope Calculator

This script calculates the weight fraction of each isotope in a given chemical compound.
It also supports isotope enrichment for specified elements.

Uses the mendeleev library for isotope data and natural abundances.
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

try:
    from mendeleev import element
except ImportError:
    print("Error: mendeleev package not installed")
    print("Install with: pip install mendeleev")
    sys.exit(1)


@dataclass
class IsotopeInfo:
    """Information about a specific isotope"""
    element_symbol: str
    mass_number: int
    atomic_mass: float
    abundance: float  # Natural abundance (0-1)
    enriched_abundance: Optional[float] = None  # Custom enrichment (0-1)


@dataclass
class ElementComposition:
    """Composition of an element in the compound"""
    symbol: str
    count: float
    enrichments: Dict[int, float]  # mass_number -> enrichment fraction


class ChemicalFormulaParser:
    """Parser for chemical formulas"""
    
    def __init__(self):
        # Regex pattern for parsing chemical formulas
        # Matches: Element (optional isotope) + optional count
        self.pattern = re.compile(r'([A-Z][a-z]?)(?:\-(\d+))?(\d*\.?\d*)')
    
    def parse(self, formula: str) -> Dict[str, ElementComposition]:
        """Parse a chemical formula into element compositions"""
        # Remove spaces and handle parentheses (basic support)
        formula = formula.replace(' ', '')
        
        # TODO: Add support for parentheses groups like Ca(OH)2
        elements = {}
        
        for match in self.pattern.finditer(formula):
            element_symbol = match.group(1)
            isotope = int(match.group(2)) if match.group(2) else None
            count_str = match.group(3)
            count = float(count_str) if count_str else 1.0
            
            if element_symbol not in elements:
                elements[element_symbol] = ElementComposition(
                    symbol=element_symbol,
                    count=0.0,
                    enrichments={}
                )
            
            elements[element_symbol].count += count
            
            # Handle specific isotope notation (e.g., U-235)
            if isotope:
                elements[element_symbol].enrichments[isotope] = 1.0
        
        return elements


class IsotopeCalculator:
    """Main calculator for isotope weight fractions"""
    
    def __init__(self):
        self.parser = ChemicalFormulaParser()
    
    def get_element_isotopes(self, element_symbol: str, 
                           include_artificial: Optional[List[int]] = None) -> List[IsotopeInfo]:
        """
        Get isotopes for an element from mendeleev
        
        Args:
            element_symbol: Element symbol (e.g., 'U', 'H')
            include_artificial: List of mass numbers for artificial isotopes to include
        """
        try:
            elem = element(element_symbol)
            isotopes = []
            
            # Get isotopes with natural abundance > 0
            for isotope in elem.isotopes:
                if isotope.abundance and isotope.abundance > 0:
                    isotopes.append(IsotopeInfo(
                        element_symbol=element_symbol,
                        mass_number=isotope.mass_number,
                        atomic_mass=isotope.mass or isotope.mass_number,
                        abundance=isotope.abundance / 100.0  # Convert from % to fraction
                    ))
            
            # Add artificial isotopes if requested (with 0% natural abundance)
            if include_artificial:
                for mass_number in include_artificial:
                    # Find this specific isotope
                    for isotope in elem.isotopes:
                        if isotope.mass_number == mass_number:
                            # Check if we already have it (shouldn't happen for artificial ones)
                            if not any(iso.mass_number == mass_number for iso in isotopes):
                                isotopes.append(IsotopeInfo(
                                    element_symbol=element_symbol,
                                    mass_number=isotope.mass_number,
                                    atomic_mass=isotope.mass or isotope.mass_number,
                                    abundance=0.0  # Artificial isotope starts with 0% natural abundance
                                ))
                            break
            
            # Sort by mass number
            isotopes.sort(key=lambda x: x.mass_number)
            return isotopes
            
        except Exception as e:
            raise ValueError(f"Could not find element {element_symbol}: {e}")
    
    def apply_enrichments(self, isotopes: List[IsotopeInfo], 
                         enrichments: Dict[int, float]) -> List[IsotopeInfo]:
        """Apply enrichment to specific isotopes"""
        if not enrichments:
            return isotopes
        
        # Create a copy of isotopes with modified abundances
        enriched_isotopes = []
        total_enriched = sum(enrichments.values())
        
        if total_enriched > 1.0:
            raise ValueError("Total enrichment cannot exceed 100%")
        
        remaining_fraction = 1.0 - total_enriched
        
        for isotope in isotopes:
            new_isotope = IsotopeInfo(
                element_symbol=isotope.element_symbol,
                mass_number=isotope.mass_number,
                atomic_mass=isotope.atomic_mass,
                abundance=isotope.abundance,
                enriched_abundance=isotope.abundance
            )
            
            if isotope.mass_number in enrichments:
                # Set specific enrichment
                new_isotope.enriched_abundance = enrichments[isotope.mass_number]
            else:
                # Scale down natural abundance proportionally
                natural_sum = sum(iso.abundance for iso in isotopes 
                                if iso.mass_number not in enrichments)
                if natural_sum > 0:
                    scale_factor = remaining_fraction / natural_sum
                    new_isotope.enriched_abundance = isotope.abundance * scale_factor
                else:
                    new_isotope.enriched_abundance = 0.0
            
            enriched_isotopes.append(new_isotope)
        
        return enriched_isotopes
    
    def calculate_molecular_weight(self, elements: Dict[str, ElementComposition]) -> float:
        """Calculate the molecular weight of the compound"""
        total_weight = 0.0
        
        for element_comp in elements.values():
            # Include artificial isotopes if they're specified in enrichments
            artificial_masses = list(element_comp.enrichments.keys()) if element_comp.enrichments else None
            isotopes = self.get_element_isotopes(element_comp.symbol, artificial_masses)
            enriched_isotopes = self.apply_enrichments(isotopes, element_comp.enrichments)
            
            # Calculate average atomic mass for this element
            avg_atomic_mass = sum(iso.atomic_mass * (iso.enriched_abundance or iso.abundance) 
                                for iso in enriched_isotopes)
            
            total_weight += avg_atomic_mass * element_comp.count
        
        return total_weight
    
    def calculate_isotope_fractions(self, formula: str, 
                                  enrichments: Optional[Dict[str, Dict[int, float]]] = None) -> Dict[str, float]:
        """
        Calculate weight fractions of all isotopes in a compound
        
        Args:
            formula: Chemical formula (e.g., "H2O", "UO2", "CaCO3")
            enrichments: Dict of element -> {mass_number: fraction} for enriched isotopes
        
        Returns:
            Dict mapping isotope names (e.g., "H-1", "O-16") to weight fractions
        """
        elements = self.parser.parse(formula)
        
        # Apply user-specified enrichments
        if enrichments:
            for element_symbol, element_enrichments in enrichments.items():
                if element_symbol in elements:
                    elements[element_symbol].enrichments.update(element_enrichments)
        
        # Calculate molecular weight
        molecular_weight = self.calculate_molecular_weight(elements)
        
        # Calculate isotope weight fractions
        isotope_fractions = {}
        
        for element_comp in elements.values():
            # Include artificial isotopes if they're specified in enrichments
            artificial_masses = list(element_comp.enrichments.keys()) if element_comp.enrichments else None
            isotopes = self.get_element_isotopes(element_comp.symbol, artificial_masses)
            enriched_isotopes = self.apply_enrichments(isotopes, element_comp.enrichments)
            
            for isotope in enriched_isotopes:
                abundance = isotope.enriched_abundance or isotope.abundance
                isotope_weight = (isotope.atomic_mass * abundance * element_comp.count)
                weight_fraction = isotope_weight / molecular_weight
                
                isotope_name = f"{isotope.element_symbol}-{isotope.mass_number}"
                isotope_fractions[isotope_name] = weight_fraction
        
        return isotope_fractions


def main():
    """Main function with command-line interface"""
    parser = argparse.ArgumentParser(
        description="Calculate isotope weight fractions in chemical compounds",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic water molecule
  python isotope_calculator.py H2O
  
  # Uranium dioxide
  python isotope_calculator.py UO2
  
  # Enriched uranium (20% U-235)
  python isotope_calculator.py UO2 --enrich U:235:0.20
  
  # Multiple enrichments
  python isotope_calculator.py H2O --enrich H:2:0.1 --enrich O:18:0.05
  
  # Output to JSON file
  python isotope_calculator.py CaCO3 --output results.json
        """)
    
    parser.add_argument('formula', help='Chemical formula (e.g., H2O, UO2, CaCO3)')
    parser.add_argument('--enrich', action='append', metavar='ELEMENT:MASS:FRACTION',
                       help='Enrich isotope: element:mass_number:fraction (can be used multiple times)')
    parser.add_argument('--output', '-o', help='Output file (JSON format)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Show detailed information')
    
    args = parser.parse_args()
    
    # Parse enrichments
    enrichments = {}
    if args.enrich:
        for enrich_str in args.enrich:
            try:
                element, mass_str, fraction_str = enrich_str.split(':')
                mass_number = int(mass_str)
                fraction = float(fraction_str)
                
                if fraction < 0 or fraction > 1:
                    raise ValueError("Enrichment fraction must be between 0 and 1")
                
                if element not in enrichments:
                    enrichments[element] = {}
                enrichments[element][mass_number] = fraction
                
            except ValueError as e:
                print(f"Error parsing enrichment '{enrich_str}': {e}")
                sys.exit(1)
    
    # Calculate isotope fractions
    calculator = IsotopeCalculator()
    
    try:
        fractions = calculator.calculate_isotope_fractions(args.formula, enrichments)
        
        # Prepare output
        output_data = {
            'formula': args.formula,
            'enrichments': enrichments if enrichments else None,
            'isotope_fractions': fractions,
            'total_fraction': sum(fractions.values())
        }
        
        # Output results
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(output_data, f, indent=2)
            print(f"Results written to {args.output}")
        else:
            print(f"\nIsotope Weight Fractions for {args.formula}:")
            print("-" * 50)
            
            # Sort by weight fraction (descending)
            sorted_fractions = sorted(fractions.items(), key=lambda x: x[1], reverse=True)
            
            for isotope_name, fraction in sorted_fractions:
                print(f"{isotope_name:>8}: {fraction:8.6f} ({fraction*100:6.3f}%)")
            
            print("-" * 50)
            print(f"{'Total:':>8} {sum(fractions.values()):8.6f} ({sum(fractions.values())*100:6.3f}%)")
            
            if args.verbose and enrichments:
                print(f"\nEnrichments applied:")
                for element, element_enrichments in enrichments.items():
                    for mass, fraction in element_enrichments.items():
                        print(f"  {element}-{mass}: {fraction*100:.1f}%")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()