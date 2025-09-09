#!/usr/bin/env python3
"""
Parallel parseOutput Processor

This module provides parallel processing capabilities for parsing multiple SCALE/ORIGEN 
output files using the OptimizedORIGENParser, generating MCNP material cards in batch
with multithreading for maximum performance.
"""

import os
import sys
import time
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Callable, Tuple, Union
from datetime import datetime
import threading
import json
import sqlite3

# Import the parseOutput module
sys.path.append(str(Path(__file__).parent))
from parseOutput import OptimizedORIGENParser

logger = logging.getLogger(__name__)

def parse_element_file_process_safe(element_file_path: str, endf8_json_path: Optional[str], job_index: int) -> Tuple[str, bool, Optional[str], Optional[str], Optional[dict]]:
    """
    Process-safe function for parsing a single element file (picklable for ProcessPoolExecutor)
    
    Args:
        element_file_path: Path to the element output file
        endf8_json_path: Path to ENDF8 isotopes JSON file
        job_index: Job index for unique material ID generation
        
    Returns:
        Tuple of (element_name, success, error_message, material_card, material_info)
    """
    element_file = Path(element_file_path)
    element_name = element_file.stem
    
    try:
        # Import here to avoid pickling issues
        from parseOutput import OptimizedORIGENParser
        
        # Create parser instance for this file
        parser = OptimizedORIGENParser(
            output_file_path=str(element_file),
            endf8_json_path=endf8_json_path
        )
        
        # Parse all sections to get the data
        combined_df, processed_case = parser.parse_all_sections()
        
        # Generate MCNP material card with unique material ID
        material_id = 200 + job_index
        
        material_card, material_info = parser.generate_mcnp_materials(
            combined_df, 
            material_id=material_id, 
            case_name=processed_case
        )
        
        # Add element information to material_info
        material_info['element_name'] = element_name
        material_info['material_id'] = material_id
        
        return element_name, True, None, material_card, material_info
        
    except Exception as e:
        error_msg = f"Error parsing {element_name}: {str(e)}"
        return element_name, False, error_msg, None, None

class ElementParseJob:
    """Represents a single element parsing job"""
    
    def __init__(self, element_file: Path):
        self.element_file = element_file
        self.element_name = element_file.stem
        self.start_time = None
        self.end_time = None
        self.status = 'not_started'
        self.material_card = None
        self.material_info = None
        self.error = None
        self.runtime_seconds = None
        
    def __repr__(self):
        return f"ElementParseJob({self.element_name}, status={self.status})"

class ParallelParseOutputProcessor:
    """Parallel processor for SCALE/ORIGEN output files using parseOutput logic"""
    
    def __init__(self, max_workers: int = 16, executor_type: str = "thread", 
                 endf8_json_path: Optional[str] = None):
        """
        Initialize parallel processor
        
        Args:
            max_workers: Maximum number of concurrent workers
            executor_type: "thread" or "process" for ThreadPoolExecutor or ProcessPoolExecutor
            endf8_json_path: Path to ENDF8 isotopes JSON file
        """
        self.max_workers = max_workers
        self.executor_type = executor_type
        self.endf8_json_path = endf8_json_path or "../endf8_isotopes.json"
        self.jobs: Dict[str, ElementParseJob] = {}
        self.executor = None
        self.progress_callback = None
        self.stop_monitoring = False
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    def add_element_files(self, input_dir: Path, pattern: str = "element_*.out"):
        """Add all matching element output files from a directory"""
        input_dir = Path(input_dir)
        element_files = list(input_dir.glob(pattern))
        
        if not element_files:
            raise ValueError(f"No element files found matching '{pattern}' in {input_dir}")
        
        logger.info(f"Found {len(element_files)} element files")
        for element_file in sorted(element_files):
            job = ElementParseJob(element_file)
            self.jobs[job.element_name] = job
        
        return len(element_files)
    
    def _parse_single_element(self, job: ElementParseJob) -> Tuple[str, bool, Optional[str]]:
        """
        Parse a single element output file
        
        Args:
            job: ElementParseJob instance
            
        Returns:
            Tuple of (element_name, success, error_message)
        """
        job.start_time = datetime.now()
        job.status = 'running'
        
        logger.info(f"Starting element parsing: {job.element_name}")
        
        try:
            # Create parser instance for this file
            parser = OptimizedORIGENParser(
                output_file_path=str(job.element_file),
                endf8_json_path=self.endf8_json_path
            )
            
            # Parse all sections to get the data
            combined_df, processed_case = parser.parse_all_sections()
            
            # Generate MCNP material card with unique material ID
            # Use job index as offset to avoid ID conflicts
            job_index = list(self.jobs.keys()).index(job.element_name)
            material_id = 200 + job_index
            
            material_card, material_info = parser.generate_mcnp_materials(
                combined_df, 
                material_id=material_id, 
                case_name=processed_case
            )
            
            # Store results
            job.material_card = material_card
            job.material_info = material_info
            job.material_info['element_name'] = job.element_name
            job.material_info['material_id'] = material_id
            
            job.end_time = datetime.now()
            job.runtime_seconds = (job.end_time - job.start_time).total_seconds()
            job.status = 'completed'
            
            logger.info(f"Completed element: {job.element_name} "
                       f"({job.runtime_seconds:.1f}s, {len(combined_df)} nuclides, "
                       f"material ID: M{material_id})")
            
            return job.element_name, True, None
            
        except Exception as e:
            job.status = 'failed'
            job.end_time = datetime.now()
            job.error = str(e)
            logger.error(f"Failed element {job.element_name}: {e}")
            return job.element_name, False, str(e)
    
    def _monitor_progress(self):
        """Background thread to monitor parsing progress"""
        while not self.stop_monitoring:
            try:
                status_counts = {'not_started': 0, 'running': 0, 'completed': 0, 'failed': 0}
                
                for job in self.jobs.values():
                    status_counts[job.status] = status_counts.get(job.status, 0) + 1
                
                total = len(self.jobs)
                completed = status_counts['completed'] + status_counts['failed']
                
                if completed > 0 and total > 0:
                    logger.info(f"Progress: {completed}/{total} "
                              f"({completed/total*100:.1f}%) - "
                              f"Running: {status_counts['running']}, "
                              f"Completed: {status_counts['completed']}, "
                              f"Failed: {status_counts['failed']}")
                
                time.sleep(5)  # Update every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in progress monitoring: {e}")
                time.sleep(10)
    
    def process_all_parallel(self, progress_callback: Optional[Callable] = None) -> Dict[str, bool]:
        """
        Process all element files in parallel
        
        Args:
            progress_callback: Function to call with progress updates
            
        Returns:
            Dictionary mapping element names to success status
        """
        if not self.jobs:
            raise ValueError("No element files added to processor")
        
        logger.info(f"Starting parallel processing of {len(self.jobs)} elements "
                   f"with {self.max_workers} {self.executor_type} workers")
        
        # Start background monitoring thread
        self.stop_monitoring = False
        monitor_thread = threading.Thread(target=self._monitor_progress, daemon=True)
        monitor_thread.start()
        
        results = {}
        failed_jobs = []
        
        executor_class = ThreadPoolExecutor if self.executor_type == "thread" else ProcessPoolExecutor
        
        try:
            with executor_class(max_workers=self.max_workers) as executor:
                self.executor = executor
                
                # Submit all jobs
                future_to_job = {}
                job_index = 0
                
                if self.executor_type == "thread":
                    # Use thread-based approach with job objects
                    for job in self.jobs.values():
                        future = executor.submit(self._parse_single_element, job)
                        future_to_job[future] = job
                else:
                    # Use process-based approach with process-safe function
                    for job in self.jobs.values():
                        future = executor.submit(
                            parse_element_file_process_safe,
                            str(job.element_file),
                            self.endf8_json_path,
                            job_index
                        )
                        future_to_job[future] = job
                        job_index += 1
                
                # Process completed jobs
                completed = 0
                for future in as_completed(future_to_job):
                    job = future_to_job[future]
                    
                    try:
                        if self.executor_type == "thread":
                            element_name, success, error_msg = future.result()
                            # Job object already has material_card and material_info populated
                        else:
                            # Process-based execution returns more data
                            element_name, success, error_msg, material_card, material_info = future.result()
                            # Populate job object with returned data
                            if success and material_card and material_info:
                                job.material_card = material_card
                                job.material_info = material_info
                                job.status = 'completed'
                            else:
                                job.status = 'failed'
                                job.error = error_msg
                        
                        results[element_name] = success
                        
                        if not success:
                            failed_jobs.append(element_name)
                        
                        completed += 1
                        
                        # Progress callback
                        if progress_callback:
                            progress_callback(completed, len(self.jobs), element_name, success)
                        
                    except Exception as e:
                        logger.error(f"Error getting result for {job.element_name}: {e}")
                        results[job.element_name] = False
                        failed_jobs.append(job.element_name)
        
        finally:
            self.stop_monitoring = True
        
        # Final summary
        successful = sum(results.values())
        logger.info(f"Parallel processing complete: {successful}/{len(self.jobs)} elements successful")
        
        if failed_jobs:
            logger.warning(f"Failed elements: {', '.join(failed_jobs)}")
        
        return results
    
    def save_combined_materials(self, output_file: Path):
        """Save all material cards to a single file"""
        successful_jobs = [job for job in self.jobs.values() if job.status == 'completed' and job.material_card]
        
        if not successful_jobs:
            logger.warning("No successful jobs to save")
            return
        
        with open(output_file, "w") as f:
            f.write("c MCNP Material Cards from Parallel ORIGEN Processing\n")
            f.write(f"c Generated from: {len(successful_jobs)} element files\n")
            f.write(f"c Processing time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("c\n")
            
            for i, job in enumerate(successful_jobs):
                if i > 0:
                    f.write("c\n")  # Separator between cards
                f.write(f"c Element: {job.element_name}\n")
                f.writelines([line + '\n' for line in job.material_card])
        
        logger.info(f"Combined material cards written to: {output_file}")
    
    def save_to_database(self, db_path: Path, cycle_number: int = 1):
        """Save all materials to SQLite database"""
        successful_jobs = [job for job in self.jobs.values() if job.status == 'completed' and job.material_info]
        
        if not successful_jobs:
            logger.warning("No successful jobs to save to database")
            return
        
        # Create database using parseOutput's method
        first_job = successful_jobs[0]
        dummy_parser = OptimizedORIGENParser("dummy", self.endf8_json_path)
        dummy_parser.create_materials_database(db_path)
        
        # Prepare materials data for database
        materials_data = {}
        for job in successful_jobs:
            materials_data[job.element_name] = job.material_info
        
        dummy_parser.save_materials_to_database(db_path, cycle_number, materials_data)
        logger.info(f"Materials saved to database: {db_path}")
    
    def save_summary_json(self, output_file: Path):
        """Save processing summary to JSON"""
        summary = {
            'processing_info': {
                'total_elements': len(self.jobs),
                'successful_elements': len([j for j in self.jobs.values() if j.status == 'completed']),
                'failed_elements': len([j for j in self.jobs.values() if j.status == 'failed']),
                'max_workers': self.max_workers,
                'executor_type': self.executor_type,
                'processing_date': datetime.now().isoformat()
            },
            'elements': {}
        }
        
        for job in self.jobs.values():
            element_summary = {
                'status': job.status,
                'runtime_seconds': job.runtime_seconds,
                'error': job.error
            }
            
            if job.material_info:
                element_summary.update({
                    'material_id': job.material_info.get('material_id'),
                    'total_mass_g': job.material_info.get('total_mass_g'),
                    'density_g_cm3': job.material_info.get('density_g_cm3'),
                    'helium_mass_g': job.material_info.get('helium_mass_g'),
                    'isotope_count': len(job.material_info.get('processed_zaids', {}))
                })
            
            summary['elements'][job.element_name] = element_summary
        
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Processing summary saved to: {output_file}")
    
    def get_status_summary(self) -> Dict:
        """Get current status summary of all jobs"""
        status_counts = {'not_started': 0, 'running': 0, 'completed': 0, 'failed': 0}
        
        for job in self.jobs.values():
            status_counts[job.status] = status_counts.get(job.status, 0) + 1
        
        return {
            'total_elements': len(self.jobs),
            'status_counts': status_counts,
            'elements': {name: job.status for name, job in self.jobs.items()}
        }

def main():
    """Command line interface for the parallel processor"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Process SCALE element output files in parallel using parseOutput logic")
    parser.add_argument("--input-dir", "-i", required=True,
                       help="Directory containing element output files")
    parser.add_argument("--workers", "-w", type=int, default=16,
                       help="Number of parallel workers (default: 16)")
    parser.add_argument("--executor", "-e", choices=["thread", "process"], default="thread",
                       help="Executor type: thread or process (default: thread)")
    parser.add_argument("--pattern", "-p", default="element_*.out",
                       help="Pattern to match element files (default: element_*.out)")
    parser.add_argument("--output", "-o", default="mcnp_materials_parallel.txt",
                       help="Output file for combined material cards")
    parser.add_argument("--save-db", type=str,
                       help="Save materials to database (provide database path)")
    parser.add_argument("--cycle", type=int, default=1,
                       help="Cycle number for database storage")
    parser.add_argument("--summary", type=str,
                       help="Save processing summary to JSON file")
    parser.add_argument("--endf8-json", type=str,
                       help="Path to ENDF8 isotopes JSON file")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create processor
    processor = ParallelParseOutputProcessor(
        max_workers=args.workers,
        executor_type=args.executor,
        endf8_json_path=args.endf8_json
    )
    
    try:
        # Add element files
        num_elements = processor.add_element_files(Path(args.input_dir), args.pattern)
        print(f"Found {num_elements} element files to process")
        
        # Progress callback
        def progress_update(completed, total, element_name, success):
            status = "✅" if success else "❌"
            print(f"{status} {element_name} ({completed}/{total})")
        
        # Process elements
        print(f"Starting parallel processing with {args.workers} {args.executor} workers...")
        start_time = time.time()
        
        results = processor.process_all_parallel(progress_callback=progress_update)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Save outputs
        processor.save_combined_materials(Path(args.output))
        
        if args.save_db:
            processor.save_to_database(Path(args.save_db), args.cycle)
        
        if args.summary:
            processor.save_summary_json(Path(args.summary))
        
        # Final summary
        successful = sum(results.values())
        print(f"\nProcessing Summary:")
        print(f"  Total elements: {len(results)}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {len(results) - successful}")
        print(f"  Processing time: {processing_time:.1f} seconds")
        print(f"  Average time per element: {processing_time/len(results):.2f} seconds")
        print(f"  Output file: {args.output}")
        
        if successful < len(results):
            print(f"\nFailed elements:")
            for element_name, success in results.items():
                if not success:
                    job = processor.jobs[element_name]
                    print(f"  - {element_name}: {job.error}")
        
        sys.exit(0 if successful == len(results) else 1)
        
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()