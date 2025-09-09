#!/usr/bin/env python3
"""
Assembly Results Collection Utility

This script collects and aggregates results from parallel SCALE assembly runs.
It processes multiple output files and combines them for analysis.
"""

import argparse
import sys
import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ScaleResultsCollector:
    """Collect and aggregate results from parallel SCALE runs (assembly or element-based)"""
    
    def __init__(self, mode: str = 'assembly'):
        self.mode = mode  # 'assembly' or 'element'
        self.assembly_results = {}  # Dict of assembly_name: results
        self.element_results = {}   # Dict of element_filename: results
        self.element_mapping = None # Element to assembly mapping
        self.total_elements = 0
        self.successful_assemblies = 0
        self.failed_assemblies = 0
        self.successful_elements = 0
        self.failed_elements = 0
    
    def load_element_mapping(self, input_dir: str) -> Optional[Dict]:
        """Load element mapping from JSON file if available"""
        mapping_file = Path(input_dir) / "element_mapping.json"
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load element mapping: {e}")
        return None
    
    def collect_results(self, input_dir: str, output_file: Optional[str] = None) -> bool:
        """Collect results from all output files (assembly or element-based)"""
        input_path = Path(input_dir)
        if not input_path.exists():
            logger.error(f"Input directory not found: {input_dir}")
            return False
        
        # Load element mapping if in element mode
        if self.mode == 'element':
            self.element_mapping = self.load_element_mapping(input_dir)
            if self.element_mapping:
                logger.info(f"Loaded element mapping with {len(self.element_mapping)} elements")
        
        # Find output files based on mode
        if self.mode == 'element':
            pattern = "element_*.out"
            job_type = "element"
        else:
            pattern = "assembly_*.out"
            job_type = "assembly"
            
        output_files = list(input_path.glob(pattern))
        if not output_files:
            logger.error(f"No {job_type} output files found in {input_dir} (pattern: {pattern})")
            return False
        
        logger.info(f"Found {len(output_files)} {job_type} output files")
        
        # Process each output file
        if self.mode == 'element':
            success = self.collect_element_results(output_files)
        else:
            success = self.collect_assembly_results(output_files)
            
        if output_file:
            self.save_combined_results(output_file)
        
        self.print_summary()
        return success
    
    def extract_assembly_name(self, filename: str) -> str:
        """Extract assembly name from output filename"""
        # Convert "assembly_Assembly_MTR-F-001.out" back to "Assembly MTR-F-001"
        match = re.match(r'assembly_(.+)\.out', filename)
        if match:
            safe_name = match.group(1)
            # Convert safe filename back to original name
            return safe_name.replace('_', ' ').replace('-', '/')
        return filename
        
    def extract_element_info(self, filename: str) -> Dict:
        """Extract element information from element output filename"""
        # Extract info from "element_Assembly_MTR-F-001_E001.out" or similar
        if self.element_mapping and filename in self.element_mapping:
            mapping = self.element_mapping[filename]
            return {
                'assembly': mapping['assembly'],
                'element_key': mapping['element_key'],
                'element_number': mapping['element_number'],
                'safe_assembly': mapping['safe_assembly']
            }
        else:
            # Fallback parsing from filename
            match = re.match(r'element_(.+)_E(\d+)\.out', filename)
            if match:
                safe_assembly = match.group(1)
                element_number = int(match.group(2))
                return {
                    'assembly': safe_assembly.replace('_', ' ').replace('-', '/'),
                    'element_key': f'Element #{element_number}',
                    'element_number': element_number,
                    'safe_assembly': safe_assembly
                }
            return {'assembly': 'Unknown', 'element_key': filename, 'element_number': 0, 'safe_assembly': 'unknown'}
    
    def collect_assembly_results(self, output_files: List[Path]) -> bool:
        """Collect results from assembly output files"""
        for output_file_path in sorted(output_files):
            assembly_name = self.extract_assembly_name(output_file_path.name)
            logger.debug(f"Processing {assembly_name}: {output_file_path}")
            
            try:
                result = self.parse_job_output(output_file_path, job_type='assembly')
                if result:
                    self.assembly_results[assembly_name] = result
                    self.successful_assemblies += 1
                    self.total_elements += result.get('element_count', 0)
                else:
                    logger.warning(f"Failed to parse results from {assembly_name}")
                    self.failed_assemblies += 1
                    
            except Exception as e:
                logger.error(f"Error processing {assembly_name}: {e}")
                self.failed_assemblies += 1
        
        return True
        
    def collect_element_results(self, output_files: List[Path]) -> bool:
        """Collect results from element output files and group by assembly"""
        # Group elements by assembly for summary
        assembly_elements = defaultdict(list)
        
        for output_file_path in sorted(output_files):
            element_filename = output_file_path.name
            logger.debug(f"Processing element: {element_filename}")
            
            try:
                result = self.parse_job_output(output_file_path, job_type='element')
                if result:
                    self.element_results[element_filename] = result
                    self.successful_elements += 1
                    
                    # Group by assembly if mapping is available
                    if self.element_mapping and element_filename in self.element_mapping:
                        assembly_name = self.element_mapping[element_filename]['assembly']
                        assembly_elements[assembly_name].append((element_filename, result))
                    else:
                        assembly_elements["Ungrouped"].append((element_filename, result))
                else:
                    logger.warning(f"Failed to parse results from {element_filename}")
                    self.failed_elements += 1
                    
            except Exception as e:
                logger.error(f"Error processing {element_filename}: {e}")
                self.failed_elements += 1
        
        # Create assembly summaries from element results
        for assembly_name, elements in assembly_elements.items():
            successful_elements = [elem for filename, elem in elements if elem['execution_status'] == 'completed']
            failed_elements = [elem for filename, elem in elements if elem['execution_status'] != 'completed']
            
            assembly_summary = {
                'assembly_name': assembly_name,
                'element_count': len(elements),
                'successful_elements': len(successful_elements),
                'failed_elements': len(failed_elements),
                'execution_status': 'completed' if len(failed_elements) == 0 else 'partial' if len(successful_elements) > 0 else 'failed',
                'total_execution_time': sum(elem.get('execution_time', 0) or 0 for filename, elem in elements),
                'elements': {filename: result for filename, result in elements}
            }
            
            self.assembly_results[assembly_name] = assembly_summary
            if assembly_summary['execution_status'] in ['completed', 'partial']:
                self.successful_assemblies += 1
            else:
                self.failed_assemblies += 1
        
        self.total_elements = len(output_files)
        return True
    
    def parse_job_output(self, file_path: Path, job_type: str = 'assembly') -> Optional[Dict]:
        """Parse a single assembly output file and corresponding .msg file for key results"""
        try:
            # Import the msg parser (it may be in the same directory or in tools/)
            try:
                from scale_msg_parser import ScaleMsgParser
            except ImportError:
                # Try importing from the same directory as this script
                import sys
                sys.path.append(str(file_path.parent))
                from scale_msg_parser import ScaleMsgParser
            
            msg_parser = ScaleMsgParser()
            
            if job_type == 'element':
                element_info = self.extract_element_info(file_path.name)
                result = {
                    'filename': file_path.name,
                    'assembly_name': element_info['assembly'],
                    'element_key': element_info['element_key'],
                    'element_number': element_info['element_number'],
                    'file_path': str(file_path),
                    'file_size_mb': file_path.stat().st_size / (1024 * 1024),
                    'execution_status': 'unknown',
                    'total_mass_g': {},
                    'final_isotopes': {},
                    'execution_time': None,
                    'return_code': None,
                    'start_time': None,
                    'finish_time': None
                }
            else:
                result = {
                    'assembly_name': self.extract_assembly_name(file_path.name),
                    'file_path': str(file_path),
                    'file_size_mb': file_path.stat().st_size / (1024 * 1024),
                    'element_count': 0,
                    'execution_status': 'unknown',
                    'total_mass_g': {},
                    'final_isotopes': {},
                    'execution_time': None,
                    'return_code': None,
                    'start_time': None,
                    'finish_time': None
                }
            
            # First check the .msg file for execution status
            msg_file = file_path.with_suffix('.msg')
            if msg_file.exists():
                try:
                    msg_data = msg_parser.parse_msg_file(msg_file)
                    result.update({
                        'execution_status': msg_data['status'],
                        'return_code': msg_data.get('return_code'),
                        'execution_time': msg_data.get('run_time_seconds'),
                        'start_time': msg_data.get('job_started'),
                        'finish_time': msg_data.get('finish_time')
                    })
                except Exception as e:
                    logger.debug(f"Could not parse .msg file {msg_file}: {e}")
            
            # Parse the output file if it exists
            if not file_path.exists():
                result['execution_status'] = 'not_started'
                return result
            
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Fallback status check if .msg file wasn't available or parsed
            if result['execution_status'] == 'unknown':
                if "normally terminated" in content.lower():
                    result['execution_status'] = 'completed'
                elif "error" in content.lower() or "failed" in content.lower():
                    result['execution_status'] = 'failed'
                else:
                    result['execution_status'] = 'unknown'
            
            # Count elements processed (only for assembly mode)
            if job_type == 'assembly':
                element_matches = re.findall(r'case\(element_\d+_burn\)', content)
                result['element_count'] = len(element_matches)
            else:
                result['element_count'] = 1  # Single element per file
            
            # Extract execution time from output file if not available from .msg
            if result['execution_time'] is None:
                time_match = re.search(r'elapsed time:\s*(\d+\.?\d*)\s*seconds', content, re.IGNORECASE)
                if time_match:
                    result['execution_time'] = float(time_match.group(1))
            
            # Parse final isotope concentrations (simplified extraction)
            # This would need to be customized based on SCALE output format
            isotope_pattern = r'(\w+\d+)\s+([\d\.E\-\+]+)\s+grams'
            isotope_matches = re.findall(isotope_pattern, content)
            for isotope, mass in isotope_matches[-50:]:  # Take last 50 as final values
                result['final_isotopes'][isotope] = float(mass)
            
            # Calculate total mass
            if result['final_isotopes']:
                result['total_mass_g'] = sum(result['final_isotopes'].values())
            
            return result
            
        except Exception as e:
            logger.error(f"Error parsing {file_path}: {e}")
            return None
    
    def save_combined_results(self, output_file: str):
        """Save combined results to JSON file"""
        try:
            combined_data = {
                'summary': {
                    'mode': self.mode,
                    'total_assemblies': len(self.assembly_results),
                    'successful_assemblies': self.successful_assemblies,
                    'failed_assemblies': self.failed_assemblies,
                    'total_elements': self.total_elements,
                    'collection_timestamp': str(Path().cwd()),
                },
                'assembly_results': self.assembly_results
            }
            
            if self.mode == 'element':
                combined_data['summary'].update({
                    'total_element_files': len(self.element_results),
                    'successful_elements': self.successful_elements,
                    'failed_elements': self.failed_elements
                })
                combined_data['element_results'] = self.element_results
                if self.element_mapping:
                    combined_data['element_mapping'] = self.element_mapping
            
            with open(output_file, 'w') as f:
                json.dump(combined_data, f, indent=2)
            
            logger.info(f"Combined results saved to: {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving combined results: {e}")
    
    def print_summary(self):
        """Print summary of collected results"""
        mode_title = "ELEMENT" if self.mode == 'element' else "ASSEMBLY"
        logger.info("\n" + "="*60)
        logger.info(f"{mode_title} RESULTS COLLECTION SUMMARY")
        logger.info("="*60)
        logger.info(f"Collection mode: {self.mode}")
        logger.info(f"Total assemblies found: {len(self.assembly_results)}")
        logger.info(f"Successful assemblies: {self.successful_assemblies}")
        logger.info(f"Failed assemblies: {self.failed_assemblies}")
        
        if self.mode == 'element':
            logger.info(f"Total element files: {len(self.element_results)}")
            logger.info(f"Successful elements: {self.successful_elements}")
            logger.info(f"Failed elements: {self.failed_elements}")
            logger.info(f"Total elements processed: {self.total_elements}")
            
            if self.element_mapping:
                total_assemblies = len(set(info['assembly'] for info in self.element_mapping.values()))
                logger.info(f"Element mapping loaded: {len(self.element_mapping)} elements from {total_assemblies} assemblies")
        else:
            logger.info(f"Total elements processed: {self.total_elements}")
        
        if self.assembly_results:
            logger.info("\nAssembly Status:")
            for assembly_name, result in sorted(self.assembly_results.items()):
                if self.mode == 'element':
                    status = result['execution_status']
                    elements = result['element_count']
                    successful = result['successful_elements']
                    failed = result['failed_elements']
                    exec_time = result.get('total_execution_time', 0)
                    
                    status_symbol = "✅" if status == 'completed' else "🟡" if status == 'partial' else "❌"
                    logger.info(f"  {status_symbol} {assembly_name}: {elements} elements "
                              f"({successful} ok, {failed} failed), {exec_time:.1f}s total")
                else:
                    status = result['execution_status']
                    elements = result['element_count']
                    size_mb = result.get('file_size_mb', 0)
                    exec_time = result.get('execution_time', 'N/A')
                    
                    status_symbol = "✅" if status == 'completed' else "❌" if status == 'failed' else "❓"
                    logger.info(f"  {status_symbol} {assembly_name}: {elements} elements, "
                              f"{size_mb:.1f}MB, {exec_time}s")
    
    def generate_mcnp_materials(self, output_dir: str):
        """Generate MCNP material cards from collected results"""
        materials_file = Path(output_dir) / "combined_mcnp_materials.txt"
        
        try:
            with open(materials_file, 'w') as f:
                f.write("c Combined MCNP Material Cards from Assembly Results\n")
                f.write(f"c Generated from {len(self.assembly_results)} assemblies\n")
                f.write(f"c Total elements: {self.total_elements}\n\n")
                
                material_id = 1100  # Starting material ID
                
                for assembly_name, result in sorted(self.assembly_results.items()):
                    if result['execution_status'] == 'completed' and result['final_isotopes']:
                        f.write(f"c {assembly_name} - {result['element_count']} elements\n")
                        f.write(f"m{material_id} nlib=00c\n")
                        
                        # Convert isotopes to MCNP format (simplified)
                        for isotope, mass in sorted(result['final_isotopes'].items()):
                            if mass > 1e-10:  # Only include significant masses
                                # This is a simplified conversion - would need proper ZAID mapping
                                f.write(f"c      {isotope} {mass:.6e} grams\n")
                        
                        f.write("\n")
                        material_id += 1
            
            logger.info(f"MCNP material cards written to: {materials_file}")
            
        except Exception as e:
            logger.error(f"Error generating MCNP materials: {e}")

def main():
    """Main command line interface"""
    parser = argparse.ArgumentParser(
        description="Collect and aggregate results from parallel SCALE runs (assembly or element-based)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect assembly results from current directory
  python collect_assembly_results.py --input-dir . --mode assembly
  
  # Collect element results with assembly grouping
  python collect_assembly_results.py --input-dir . --mode element --check-msg
  
  # Auto-detect mode based on available files
  python collect_assembly_results.py --input-dir . --check-msg
  
  # Collect element results and save to JSON with complete status info
  python collect_assembly_results.py --input-dir . --mode element --check-msg --output combined_results.json
  
  # Generate MCNP material cards from element results
  python collect_assembly_results.py --input-dir . --mode element --mcnp-materials --check-msg
        """
    )
    
    parser.add_argument("--input-dir", "-i", required=True,
                        help="Directory containing output files (assembly or element-based)")
    
    parser.add_argument("--mode", "-m", choices=['assembly', 'element', 'auto'], default='auto',
                        help="Collection mode: assembly, element, or auto-detect (default: auto)")
    
    parser.add_argument("--output", "-o", 
                        help="Output JSON file for combined results")
    
    parser.add_argument("--mcnp-materials", action="store_true",
                        help="Generate MCNP material cards from results")
    
    parser.add_argument("--check-msg", action="store_true",
                        help="Use .msg files for accurate completion status (recommended)")
    
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate input directory
    if not Path(args.input_dir).exists():
        logger.error(f"Input directory not found: {args.input_dir}")
        sys.exit(1)
    
    # Auto-detect mode if requested
    mode = args.mode
    if mode == 'auto':
        input_path = Path(args.input_dir)
        element_files = list(input_path.glob("element_*.out"))
        assembly_files = list(input_path.glob("assembly_*.out"))
        
        if element_files and not assembly_files:
            mode = 'element'
            logger.info(f"Auto-detected element mode ({len(element_files)} element files found)")
        elif assembly_files and not element_files:
            mode = 'assembly' 
            logger.info(f"Auto-detected assembly mode ({len(assembly_files)} assembly files found)")
        elif element_files and assembly_files:
            mode = 'element'  # Prefer element mode if both exist
            logger.info(f"Both file types found, using element mode ({len(element_files)} element files)")
        else:
            logger.error("No output files found (neither assembly_*.out nor element_*.out)")
            sys.exit(1)
    
    # Collect results
    try:
        collector = ScaleResultsCollector(mode=mode)
        
        if not collector.collect_results(args.input_dir, args.output):
            logger.error(f"Failed to collect {mode} results")
            sys.exit(1)
        
        if args.mcnp_materials:
            collector.generate_mcnp_materials(args.input_dir)
        
        logger.info(f"{mode.title()} results collection completed successfully!")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()