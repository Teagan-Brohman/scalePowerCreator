#!/usr/bin/env python3
"""
SCALE Parallel Runner

This module provides utilities for running multiple SCALE jobs in parallel
using Python's concurrent.futures and monitoring their progress via .msg files.
"""

import os
import sys
import time
import subprocess
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Dict, List, Optional, Callable, Tuple
from datetime import datetime
import threading

# Import our custom msg parser
from scale_msg_parser import ScaleMsgParser

logger = logging.getLogger(__name__)

class ScaleJob:
    """Represents a single SCALE job"""
    
    def __init__(self, inp_file: Path):
        self.inp_file = inp_file
        self.name = inp_file.stem
        self.msg_file = inp_file.with_suffix('.msg')
        self.out_file = inp_file.with_suffix('.out')
        self.start_time = None
        self.end_time = None
        self.process = None
        self.future = None
        self.status = 'not_started'
        self.return_code = None
        self.runtime_seconds = None
        
    def __repr__(self):
        return f"ScaleJob({self.name}, status={self.status})"

class ScaleParallelRunner:
    """Parallel executor for SCALE jobs with .msg file monitoring"""
    
    def __init__(self, max_workers: int = 8, scale_command: str = "D:\\Scale2\\SCALE-6.3.1\\bin\\scalerte.exe"):
        """
        Initialize parallel runner
        
        Args:
            max_workers: Maximum number of concurrent SCALE jobs
            scale_command: Command to run SCALE (e.g., 'scalerte', 'scale')
        """
        self.max_workers = max_workers
        self.scale_command = scale_command
        self.msg_parser = ScaleMsgParser()
        self.jobs: Dict[str, ScaleJob] = {}
        self.executor = None
        self.status_callback = None
        self.stop_monitoring = False
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    def add_job(self, inp_file: Path) -> ScaleJob:
        """Add a SCALE job to the runner"""
        job = ScaleJob(inp_file)
        self.jobs[job.name] = job
        logger.debug(f"Added job: {job.name}")
        return job
    
    def add_jobs_from_directory(self, directory: Path, pattern: str = "element_*.inp"):
        """Add all matching input files from a directory"""
        directory = Path(directory)
        inp_files = list(directory.glob(pattern))
        
        if not inp_files:
            raise ValueError(f"No input files found matching '{pattern}' in {directory}")
        
        logger.info(f"Found {len(inp_files)} input files")
        for inp_file in sorted(inp_files):
            self.add_job(inp_file)
        
        return len(inp_files)
    
    def _run_single_job(self, job: ScaleJob) -> Tuple[str, bool, Optional[int]]:
        """
        Run a single SCALE job
        
        Args:
            job: ScaleJob instance
            
        Returns:
            Tuple of (job_name, success, return_code)
        """
        job.start_time = datetime.now()
        job.status = 'running'
        
        logger.info(f"Starting job: {job.name}")
        
        try:
            # Change to the directory containing the input file
            work_dir = job.inp_file.parent
            
            # Run SCALE command
            cmd = [self.scale_command, str(job.inp_file.name)]
            
            logger.debug(f"Running command: {' '.join(cmd)} in {work_dir}")
            
            job.process = subprocess.Popen(
                cmd,
                cwd=work_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for process to complete
            stdout, stderr = job.process.communicate()
            job.return_code = job.process.returncode
            job.end_time = datetime.now()
            
            # Check final status from .msg file
            if job.msg_file.exists():
                msg_data = self.msg_parser.parse_msg_file(job.msg_file)
                job.status = msg_data['status']
                job.runtime_seconds = msg_data.get('run_time_seconds')
                if msg_data.get('return_code') is not None:
                    job.return_code = msg_data['return_code']
            else:
                job.status = 'completed' if job.return_code == 0 else 'failed'
            
            success = job.status == 'completed' and job.return_code == 0
            
            if success:
                logger.info(f"Completed job: {job.name} ({job.runtime_seconds}s, return code: {job.return_code})")
            else:
                logger.warning(f"Failed job: {job.name} (return code: {job.return_code} - non-zero indicates failure)")
                logger.debug(f"stderr: {stderr}")
            
            return job.name, success, job.return_code
            
        except Exception as e:
            job.status = 'failed'
            job.end_time = datetime.now()
            logger.error(f"Exception running job {job.name}: {e}")
            return job.name, False, -1
    
    def _monitor_jobs(self):
        """Background thread to monitor job progress via .msg files"""
        while not self.stop_monitoring:
            try:
                for job in self.jobs.values():
                    if job.status == 'running' and job.msg_file.exists():
                        # Update status from .msg file
                        msg_data = self.msg_parser.parse_msg_file(job.msg_file)
                        if msg_data['status'] != job.status:
                            job.status = msg_data['status']
                            if self.status_callback:
                                self.status_callback(job, msg_data)
                
                time.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                logger.error(f"Error in job monitoring: {e}")
                time.sleep(5)
    
    def run_all_parallel(self, progress_callback: Optional[Callable] = None, 
                        monitor_interval: int = 5) -> Dict[str, bool]:
        """
        Run all jobs in parallel
        
        Args:
            progress_callback: Function to call with progress updates
            monitor_interval: Seconds between progress reports
            
        Returns:
            Dictionary mapping job names to success status
        """
        if not self.jobs:
            raise ValueError("No jobs added to runner")
        
        logger.info(f"Starting {len(self.jobs)} jobs with {self.max_workers} workers")
        
        # Start background monitoring thread
        self.stop_monitoring = False
        monitor_thread = threading.Thread(target=self._monitor_jobs, daemon=True)
        monitor_thread.start()
        
        results = {}
        failed_jobs = []
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                self.executor = executor
                
                # Submit all jobs
                future_to_job = {}
                for job in self.jobs.values():
                    future = executor.submit(self._run_single_job, job)
                    job.future = future
                    future_to_job[future] = job
                
                # Process completed jobs
                completed = 0
                for future in as_completed(future_to_job):
                    job = future_to_job[future]
                    
                    try:
                        job_name, success, return_code = future.result()
                        results[job_name] = success
                        
                        if not success:
                            failed_jobs.append(job_name)
                        
                        completed += 1
                        
                        # Progress callback
                        if progress_callback:
                            progress_callback(completed, len(self.jobs), job_name, success)
                        
                        logger.info(f"Progress: {completed}/{len(self.jobs)} "
                                  f"({completed/len(self.jobs)*100:.1f}%)")
                        
                    except Exception as e:
                        logger.error(f"Error getting result for {job.name}: {e}")
                        results[job.name] = False
                        failed_jobs.append(job.name)
        
        finally:
            self.stop_monitoring = True
        
        # Final summary
        successful = sum(results.values())
        logger.info(f"Execution complete: {successful}/{len(self.jobs)} jobs successful")
        
        if failed_jobs:
            logger.warning(f"Failed jobs: {', '.join(failed_jobs)}")
        
        return results
    
    def get_status_summary(self) -> Dict:
        """Get current status summary of all jobs"""
        status_counts = {'not_started': 0, 'running': 0, 'completed': 0, 'failed': 0, 'error': 0}
        
        for job in self.jobs.values():
            if job.msg_file.exists():
                job.status = self.msg_parser.get_job_status(job.msg_file)
            status_counts[job.status] = status_counts.get(job.status, 0) + 1
        
        return {
            'total_jobs': len(self.jobs),
            'status_counts': status_counts,
            'jobs': {name: job.status for name, job in self.jobs.items()}
        }
    
    def get_detailed_status(self) -> List[Dict]:
        """Get detailed status of all jobs"""
        detailed = []
        
        for job in self.jobs.values():
            job_info = {
                'name': job.name,
                'status': job.status,
                'start_time': job.start_time,
                'end_time': job.end_time,
                'return_code': job.return_code,
                'runtime_seconds': job.runtime_seconds
            }
            
            # Update from .msg file if available
            if job.msg_file.exists():
                msg_data = self.msg_parser.parse_msg_file(job.msg_file)
                job_info.update({
                    'status': msg_data['status'],
                    'return_code': msg_data.get('return_code'),
                    'runtime_seconds': msg_data.get('run_time_seconds'),
                    'last_update': msg_data.get('last_update')
                })
            
            detailed.append(job_info)
        
        return detailed
    
    def cancel_all(self):
        """Cancel all running jobs"""
        logger.info("Canceling all jobs...")
        self.stop_monitoring = True
        
        for job in self.jobs.values():
            if job.process and job.process.poll() is None:
                logger.info(f"Terminating job: {job.name}")
                job.process.terminate()
                job.status = 'cancelled'
        
        if self.executor:
            self.executor.shutdown(wait=False)

def main():
    """Command line interface for the parallel runner"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run SCALE jobs in parallel")
    parser.add_argument("--directory", "-d", default=".", 
                       help="Directory containing assembly input files")
    parser.add_argument("--workers", "-w", type=int, default=8,
                       help="Number of parallel workers (default: 8)")
    parser.add_argument("--pattern", "-p", default="assembly_*.inp",
                       help="Pattern to match input files (default: assembly_*.inp)")
    parser.add_argument("--scale-cmd", default="scalerte",
                       help="SCALE command to run (default: scalerte)")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create runner
    runner = ScaleParallelRunner(max_workers=args.workers, scale_command=args.scale_cmd)
    
    try:
        # Add jobs
        num_jobs = runner.add_jobs_from_directory(Path(args.directory), args.pattern)
        print(f"Found {num_jobs} jobs to run")
        
        # Progress callback
        def progress_update(completed, total, job_name, success):
            status = "✅" if success else "❌"
            print(f"{status} {job_name} ({completed}/{total})")
        
        # Run jobs
        print(f"Starting parallel execution with {args.workers} workers...")
        results = runner.run_all_parallel(progress_callback=progress_update)
        
        # Summary
        successful = sum(results.values())
        print(f"\nExecution Summary:")
        print(f"  Total jobs: {len(results)}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {len(results) - successful}")
        
        if successful < len(results):
            print(f"\nFailed jobs:")
            for job_name, success in results.items():
                if not success:
                    print(f"  - {job_name}")
        
        sys.exit(0 if successful == len(results) else 1)
        
    except KeyboardInterrupt:
        print("\nCanceling jobs...")
        runner.cancel_all()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()