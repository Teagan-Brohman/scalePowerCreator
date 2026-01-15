#!/usr/bin/env python3
"""
Complete Nuclear Simulation Workflow Automation

This script automates the entire nuclear simulation pipeline:
1. Generate ORIGEN power/time cards from burnup database (optional)
2. Verify ORIGEN cards against database (optional)
3. Generate SCALE input files from flux data and power/time cards
4. Run SCALE jobs in parallel 
5. Process outputs to generate MCNP material cards
6. Clean up and archive results

Usage:
    # Generate everything from database (recommended)
    python complete_workflow.py --flux-json flux_data.json --burnup-db combined_yearly_data.db --year 2023
    
    # Use existing ORIGEN cards
    python complete_workflow.py --flux-json flux_data.json --power-time origen_cards_2023.txt --skip-origen-generation
    
    # Generate with date range
    python complete_workflow.py --flux-json flux_data.json --start-date 2020-01-01 --end-date 2023-12-31
    
    # High-performance execution with process-based MCNP parsing (recommended for large datasets)
    python complete_workflow.py --flux-json flux_data.json --year 2023 \
                               --parse-workers 32 --parse-executor process
    
    # Resume from specific step
    python complete_workflow.py --resume-from mcnp-generation --run-dir scale_runs/2025-09-08_19-03-30_2023
"""

import os
import sys
import time
import shutil
import subprocess
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

class WorkflowStep:
    """Represents a single workflow step with status tracking"""
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.status = 'pending'  # pending, running, completed, failed, skipped
        self.start_time = None
        self.end_time = None
        self.error_message = None
        self.output_files = []
    
    def start(self):
        self.status = 'running'
        self.start_time = datetime.now()
        logger.info(f"Starting step: {self.description}")
    
    def complete(self, output_files: List[str] = None):
        self.status = 'completed'
        self.end_time = datetime.now()
        self.output_files = output_files or []
        duration = (self.end_time - self.start_time).total_seconds()
        logger.info(f"Completed step: {self.description} ({duration:.1f}s)")
    
    def fail(self, error_message: str):
        self.status = 'failed'
        self.end_time = datetime.now()
        self.error_message = error_message
        logger.error(f"Failed step: {self.description} - {error_message}")
    
    def skip(self, reason: str):
        self.status = 'skipped'
        logger.info(f"Skipped step: {self.description} - {reason}")

class CompleteWorkflow:
    """Complete nuclear simulation workflow orchestrator"""
    
    def __init__(self, flux_json: str, power_time: Optional[str] = None, run_name: Optional[str] = None,
                 scale_workers: int = 8, parse_workers: int = 16, parse_executor: str = 'thread',
                 scale_command: str = 'D:\\Scale2\\SCALE-6.3.1\\bin\\scalerte.exe',
                 burnup_db: str = 'combined_yearly_data.db', year: Optional[int] = None,
                 start_date: Optional[str] = None, end_date: Optional[str] = None,
                 skip_origen_generation: bool = False, skip_origen_verification: bool = False,
                 origen_tolerance_power: float = 1e-8, origen_tolerance_time: float = 0.001):
        """
        Initialize workflow
        
        Args:
            flux_json: Path to flux data JSON file
            power_time: Path to ORIGEN power/time cards (optional, will be generated if None)
            run_name: Name for this run (auto-generated if None)
            scale_workers: Number of parallel SCALE workers
            parse_workers: Number of parallel parsing workers
            parse_executor: Executor type for parsing ('thread' or 'process')
            scale_command: SCALE command to execute
            burnup_db: Path to burnup database for ORIGEN generation
            year: Optional year filter for ORIGEN generation
            start_date: Optional start date filter (YYYY-MM-DD)
            end_date: Optional end date filter (YYYY-MM-DD)
            skip_origen_generation: Skip ORIGEN generation if power_time exists
            skip_origen_verification: Skip ORIGEN verification step
            origen_tolerance_power: Power tolerance for verification (MW)
            origen_tolerance_time: Time tolerance for verification (minutes)
        """
        self.flux_json = Path(flux_json)
        self.power_time = Path(power_time) if power_time else None
        self.scale_workers = scale_workers
        self.parse_workers = parse_workers
        self.parse_executor = parse_executor
        self.scale_command = scale_command
        
        # ORIGEN generation parameters
        self.burnup_db = Path(burnup_db)
        self.year = year
        self.start_date = start_date
        self.end_date = end_date
        self.skip_origen_generation = skip_origen_generation
        self.skip_origen_verification = skip_origen_verification
        self.origen_tolerance_power = origen_tolerance_power
        self.origen_tolerance_time = origen_tolerance_time
        
        # Generate run directory (temporarily, will be updated after date extraction)
        if run_name:
            self.run_name = run_name
        else:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            self.run_name = f"workflow_{timestamp}"
        
        self.run_dir = Path(f"scale_runs/{self.run_name}")
        self._temp_run_name = self.run_name  # Store temporary name
        self.working_dir = Path.cwd()
        
        # Workflow steps
        self.steps = {
            'setup': WorkflowStep('setup', 'Setting up workflow directories'),
            'origen-generation': WorkflowStep('origen-generation', 'Generating ORIGEN power/time cards'),
            'origen-verification': WorkflowStep('origen-verification', 'Verifying ORIGEN cards against database'),
            'scale-generation': WorkflowStep('scale-generation', 'Generating SCALE input files'),
            'scale-execution': WorkflowStep('scale-execution', 'Running SCALE jobs in parallel'),
            'mcnp-generation': WorkflowStep('mcnp-generation', 'Generating MCNP material cards'),
            'cleanup': WorkflowStep('cleanup', 'Cleaning up and archiving results')
        }
        
        # Results tracking
        self.results = {
            'run_name': self.run_name,
            'run_dir': str(self.run_dir),
            'start_time': None,
            'end_time': None,
            'total_duration': None,
            'steps': {},
            'files_generated': {},
            'statistics': {}
        }
        
        # Extract date metadata from ORIGEN cards (if available)
        self.date_metadata = self._extract_origen_date_metadata() if self.power_time else {
            'first_date': None,
            'last_date': None, 
            'date_range_str': 'unknown',
            'filename': None
        }
        
        # Update run name with date information if available
        self._update_run_name_with_dates()
        
        # Setup logging
        self.setup_logging()
        
        # Log date information if detected
        if self.date_metadata['date_range_str'] != 'unknown':
            logger.info(f"Detected date range from ORIGEN cards: {self.date_metadata['date_range_str']}")
            logger.info(f"Run directory: {self.run_dir}")
    
    def _extract_origen_date_metadata(self) -> Dict:
        """Extract date metadata from ORIGEN cards file"""
        metadata = {
            'first_date': None,
            'last_date': None,
            'date_range_str': 'unknown',
            'filename': self.power_time.name if self.power_time else None
        }
        
        try:
            if not self.power_time or not self.power_time.exists():
                logger.warning(f"ORIGEN cards file not found: {self.power_time}")
                return metadata
            
            with open(self.power_time, 'r') as f:
                content = f.read()
            
            # Look for date range in header comments
            import re
            date_range_match = re.search(r'# Date range: (.+?) to (.+?)\n', content)
            if date_range_match:
                metadata['first_date'] = date_range_match.group(1).strip()
                metadata['last_date'] = date_range_match.group(2).strip()
                
                # Extract years for range string
                first_year = metadata['first_date'][:4] if len(metadata['first_date']) >= 4 else metadata['first_date']
                last_year = metadata['last_date'][:4] if len(metadata['last_date']) >= 4 else metadata['last_date']
                
                if first_year == last_year:
                    metadata['date_range_str'] = first_year
                else:
                    metadata['date_range_str'] = f"{first_year}-{last_year}"
            else:
                # Try single date patterns
                start_match = re.search(r'# Start date: (.+?)\n', content)
                end_match = re.search(r'# End date: (.+?)\n', content)
                
                if start_match:
                    metadata['first_date'] = start_match.group(1).strip()
                    year = metadata['first_date'][:4] if len(metadata['first_date']) >= 4 else metadata['first_date']
                    metadata['date_range_str'] = f"from_{year}"
                elif end_match:
                    metadata['last_date'] = end_match.group(1).strip()  
                    year = metadata['last_date'][:4] if len(metadata['last_date']) >= 4 else metadata['last_date']
                    metadata['date_range_str'] = f"to_{year}"
                    
        except Exception as e:
            logger.warning(f"Could not extract date metadata from ORIGEN cards: {e}")
            
        return metadata
    
    def _update_run_name_with_dates(self):
        """Update run name and directory to include date information"""
        if (not hasattr(self, '_temp_run_name') or 
            self.date_metadata['date_range_str'] == 'unknown'):
            return
            
        # Only update if using auto-generated name (not user-provided)
        if self.run_name == self._temp_run_name:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            self.run_name = f"workflow_{timestamp}_{self.date_metadata['date_range_str']}"
            self.run_dir = Path(f"scale_runs/{self.run_name}")
            
            # Update results tracking
            self.results['run_name'] = self.run_name
            self.results['run_dir'] = str(self.run_dir)
            self.results['date_metadata'] = self.date_metadata
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'workflow_{self.run_name}.log')
            ]
        )
    
    def setup_directories(self) -> bool:
        """Setup workflow directories"""
        step = self.steps['setup']
        step.start()
        
        try:
            # Create run directory
            self.run_dir.mkdir(parents=True, exist_ok=True)
            
            # Create subdirectories
            subdirs = ['inputs', 'outputs', 'logs', 'mcnp_cards', 'archive']
            for subdir in subdirs:
                (self.run_dir / subdir).mkdir(exist_ok=True)
            
            # Validate input files
            if not self.flux_json.exists():
                raise FileNotFoundError(f"Flux JSON file not found: {self.flux_json}")
            
            # Only check power_time if we're not generating it
            if self.skip_origen_generation and self.power_time and not self.power_time.exists():
                raise FileNotFoundError(f"Power/time file not found: {self.power_time}")
            elif not self.skip_origen_generation and not self.burnup_db.exists():
                raise FileNotFoundError(f"Burnup database not found: {self.burnup_db}")
            
            step.complete([str(self.run_dir)])
            return True
            
        except Exception as e:
            step.fail(str(e))
            return False
    
    def generate_origen_cards(self) -> bool:
        """Generate ORIGEN power/time cards from burnup database"""
        step = self.steps['origen-generation']
        step.start()
        
        try:
            # Skip if ORIGEN cards already exist and skip flag is set
            if self.skip_origen_generation and self.power_time and self.power_time.exists():
                logger.info(f"Skipping ORIGEN generation - using existing file: {self.power_time}")
                step.skip("Using existing ORIGEN cards")
                return True
            
            # Check if burnup database exists
            if not self.burnup_db.exists():
                raise FileNotFoundError(f"Burnup database not found: {self.burnup_db}")
            
            # Prepare command for ORIGEN generation
            cmd = ['python3', str(Path('generate_origen_cards.py').resolve())]
            cmd.extend(['--db', str(self.burnup_db)])
            
            # Add date filters if provided
            if self.year:
                cmd.extend(['--year', str(self.year)])
            if self.start_date:
                cmd.extend(['--start-date', self.start_date])
            if self.end_date:
                cmd.extend(['--end-date', self.end_date])
            
            # Use verbose logging
            cmd.append('--verbose')
            
            logger.info(f"Running ORIGEN generation: {' '.join(cmd)}")
            
            # Execute generation
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"ORIGEN generation failed: {result.stderr}")
            
            # Find the generated file (it will have date-based naming)
            generated_files = list(Path('.').glob('origen_cards*.txt'))
            if not generated_files:
                raise RuntimeError("No ORIGEN cards file was generated")
            
            # Use the most recently generated file
            generated_file = max(generated_files, key=lambda f: f.stat().st_mtime)
            
            # Move to run directory with error handling
            target_file = self.run_dir / generated_file.name
            try:
                if target_file.exists():
                    # File already exists, use shutil.move as fallback (overwrites)
                    shutil.move(str(generated_file), str(target_file))
                else:
                    generated_file.rename(target_file)
            except OSError as e:
                # Fallback to shutil.move for cross-device moves or permission issues
                logger.warning(f"rename() failed, using shutil.move: {e}")
                shutil.move(str(generated_file), str(target_file))
            
            # Update power_time path for subsequent steps
            self.power_time = target_file
            
            # Re-extract date metadata from generated file (but don't update run directory)
            self.date_metadata = self._extract_origen_date_metadata()
            
            # Parse output for statistics
            origen_stats = self._parse_origen_output(result.stdout)
            self.results['statistics'].update(origen_stats)
            self.results['files_generated']['origen_cards'] = str(target_file)
            
            step.complete([str(target_file)])
            logger.info(f"ORIGEN generation completed: {target_file}")
            return True
            
        except Exception as e:
            step.fail(str(e))
            return False
    
    def verify_origen_cards(self) -> bool:
        """Verify ORIGEN cards against database"""
        step = self.steps['origen-verification']
        step.start()
        
        try:
            # Skip if verification is disabled
            if self.skip_origen_verification:
                logger.info("Skipping ORIGEN verification")
                step.skip("Verification disabled")
                return True
            
            # Check if ORIGEN cards exist
            if not self.power_time or not self.power_time.exists():
                raise FileNotFoundError(f"ORIGEN cards file not found: {self.power_time}")
            
            # Check if burnup database exists
            if not self.burnup_db.exists():
                raise FileNotFoundError(f"Burnup database not found: {self.burnup_db}")
            
            # Prepare command for verification
            cmd = ['python3', str(Path('verify_origen_cards.py').resolve())]
            cmd.extend(['--file', str(self.power_time)])
            cmd.extend(['--db', str(self.burnup_db)])
            cmd.extend(['--tolerance-power', str(self.origen_tolerance_power)])
            cmd.extend(['--tolerance-time', str(self.origen_tolerance_time)])
            
            # Add same filters as generation
            if self.year:
                cmd.extend(['--year', str(self.year)])
            if self.start_date:
                cmd.extend(['--start-date', self.start_date])
            if self.end_date:
                cmd.extend(['--end-date', self.end_date])
            
            # Use verbose logging
            cmd.append('--verbose')
            
            logger.info(f"Running ORIGEN verification: {' '.join(cmd)}")
            
            # Execute verification
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            # Save verification report
            verification_report = self.run_dir / f"origen_verification_report.txt"
            with open(verification_report, 'w') as f:
                f.write("ORIGEN Cards Verification Report\n")
                f.write("="*50 + "\n")
                f.write(f"Command: {' '.join(cmd)}\n")
                f.write(f"Return code: {result.returncode}\n\n")
                f.write("STDOUT:\n")
                f.write(result.stdout)
                f.write("\nSTDERR:\n")
                f.write(result.stderr)
            
            # Verification warnings don't fail the workflow
            if result.returncode != 0:
                logger.warning(f"ORIGEN verification completed with warnings. See: {verification_report}")
                logger.warning(f"Verification output: {result.stdout}")
            else:
                logger.info("ORIGEN verification passed successfully")
            
            # Parse verification statistics
            verification_stats = self._parse_verification_output(result.stdout)
            self.results['statistics'].update(verification_stats)
            self.results['files_generated']['verification_report'] = str(verification_report)
            
            step.complete([str(verification_report)])
            return True
            
        except Exception as e:
            step.fail(str(e))
            return False
    
    def _parse_origen_output(self, output: str) -> Dict:
        """Parse ORIGEN generation output for statistics"""
        stats = {}
        
        lines = output.split('\n')
        for line in lines:
            if 'Total entries:' in line:
                stats['origen_total_entries'] = int(line.split(':')[1].strip())
            elif 'Shutdown periods:' in line:
                stats['origen_shutdown_periods'] = int(line.split(':')[1].strip())
            elif 'Power periods:' in line:
                stats['origen_power_periods'] = int(line.split(':')[1].strip())
            elif 'Date range:' in line:
                stats['origen_date_range'] = line.split(':', 1)[1].strip()
        
        return stats
    
    def _parse_verification_output(self, output: str) -> Dict:
        """Parse verification output for statistics"""
        stats = {}
        
        lines = output.split('\n')
        for line in lines:
            if 'Total entries verified:' in line:
                stats['verification_total_entries'] = int(line.split(':')[1].strip())
            elif 'Power discrepancies:' in line:
                stats['verification_power_discrepancies'] = int(line.split(':')[1].strip())
            elif 'Time discrepancies:' in line:
                stats['verification_time_discrepancies'] = int(line.split(':')[1].strip())
        
        return stats
    
    def generate_scale_inputs(self) -> bool:
        """Generate SCALE input files"""
        step = self.steps['scale-generation']
        step.start()
        
        try:
            # Prepare command
            cmd = [
                'python3', 'generate_scale_input.py',
                '--flux-json', str(self.flux_json),
                '--power-time', str(self.power_time),
                '--split-by-element',
                '--output-dir', str(self.run_dir / 'inputs')
            ]
            
            logger.info(f"Running command: {' '.join(cmd)}")
            
            # Execute generation
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"SCALE generation failed: {result.stderr}")
            
            # Find generated files
            input_files = list((self.run_dir / 'inputs').glob('*.inp'))
            if not input_files:
                raise RuntimeError("No SCALE input files were generated")
            
            self.results['files_generated']['scale_inputs'] = len(input_files)
            self.results['files_generated']['input_files'] = [str(f) for f in input_files]
            
            step.complete([str(f) for f in input_files])
            logger.info(f"Generated {len(input_files)} SCALE input files")
            return True
            
        except Exception as e:
            step.fail(str(e))
            return False
    
    def run_scale_parallel(self) -> bool:
        """Run SCALE jobs in parallel"""
        step = self.steps['scale-execution']
        step.start()

        try:
            # Check if scale_parallel_runner exists
            scale_runner = Path('tools/scale_parallel_runner.py')
            if not scale_runner.exists():
                raise FileNotFoundError("scale_parallel_runner.py not found in tools/")

            # Preflight: check if SCALE executable exists (Windows only, unless using WSL)
            scale_cmd = self.scale_command
            is_wsl = scale_cmd.strip().startswith('wsl ')
            if not is_wsl:
                # Only check if not using WSL
                exe_path = scale_cmd.split()[0]
                print(f"Checking SCALE executable: {exe_path}")
                # Check both absolute path existence and PATH lookup
                exe_exists = Path(exe_path).exists() or shutil.which(exe_path)
                if not exe_exists:
                    logger.error(f"SCALE executable not found: {exe_path}\nCheck your --scale-cmd argument and ensure the path is correct and the file is executable.")
                    step.fail(f"SCALE executable not found: {exe_path}")
                    return False

            # Prepare command
            cmd = [
                'python3', str(scale_runner),
                '--directory', str(self.run_dir / 'inputs'),
                '--workers', str(self.scale_workers),
                '--scale-cmd', self.scale_command,
                '--pattern', 'element_*.inp',
                '--verbose'
            ]

            logger.info(f"Running parallel SCALE execution: {' '.join(cmd)}")

            # Execute parallel SCALE jobs
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                timeout=7200  # 2 hour timeout
            )

            if result.returncode != 0:
                raise RuntimeError(f"SCALE parallel execution failed: {result.stderr}")

            # Count output files
            output_files = list((self.run_dir / 'inputs').glob('*.out'))
            msg_files = list((self.run_dir / 'inputs').glob('*.msg'))

            self.results['files_generated']['scale_outputs'] = len(output_files)
            self.results['files_generated']['msg_files'] = len(msg_files)

            # Parse execution results from stdout
            stdout_lines = result.stdout.split('\n')
            for line in stdout_lines:
                if 'Successful:' in line:
                    self.results['statistics']['successful_jobs'] = int(line.split(':')[1].strip())
                elif 'Failed:' in line:
                    self.results['statistics']['failed_jobs'] = int(line.split(':')[1].strip())

            step.complete([str(f) for f in output_files])
            logger.info(f"SCALE execution completed: {len(output_files)} output files generated")
            return True

        except Exception as e:
            step.fail(str(e))
            return False
    
    def generate_mcnp_cards(self) -> bool:
        """Generate MCNP material cards from SCALE outputs"""
        step = self.steps['mcnp-generation']
        step.start()
        
        try:
            # Check if parallel parser exists
            parser_script = Path('tools/parallel_parseOutput_processor.py')
            if not parser_script.exists():
                raise FileNotFoundError("parallel_parseOutput_processor.py not found in tools/")
            
            # Prepare output paths with date information
            date_suffix = f"_{self.date_metadata['date_range_str']}" if self.date_metadata['date_range_str'] != 'unknown' else ""
            mcnp_output = self.run_dir / 'mcnp_cards' / f'mcnp_materials_all{date_suffix}.txt'
            summary_output = self.run_dir / 'mcnp_cards' / f'processing_summary{date_suffix}.json'
            db_output = self.run_dir / 'mcnp_cards' / f'materials_database{date_suffix}.db'
            
            # Prepare command
            cmd = [
                'python3', str(parser_script),
                '--input-dir', str(self.run_dir / 'inputs'),
                '--workers', str(self.parse_workers),
                '--executor', self.parse_executor,
                '--output', str(mcnp_output),
                '--summary', str(summary_output),
                '--save-db', str(db_output),
                '--verbose'
            ]
            
            logger.info(f"Running parallel MCNP card generation: {' '.join(cmd)}")
            
            # Execute parallel parsing
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"MCNP card generation failed: {result.stderr}")
            
            # Verify output files
            output_files = [mcnp_output, summary_output, db_output]
            existing_files = [f for f in output_files if f.exists()]
            
            if not existing_files:
                raise RuntimeError("No MCNP output files were generated")
            
            # Load summary statistics if available
            if summary_output.exists():
                with open(summary_output, 'r') as f:
                    summary_data = json.load(f)
                    self.results['statistics'].update(summary_data.get('processing_info', {}))
            
            self.results['files_generated']['mcnp_materials'] = str(mcnp_output)
            self.results['files_generated']['processing_summary'] = str(summary_output)
            self.results['files_generated']['materials_database'] = str(db_output)
            
            step.complete([str(f) for f in existing_files])
            logger.info(f"MCNP card generation completed: {len(existing_files)} files generated")
            return True
            
        except Exception as e:
            step.fail(str(e))
            return False
    
    def cleanup_and_archive(self, cleanup_level: str = 'minimal') -> bool:
        """Clean up and archive results"""
        step = self.steps['cleanup']
        step.start()
        
        try:
            archive_dir = self.run_dir / 'archive'
            
            if cleanup_level == 'minimal':
                # Keep all files, just organize them
                logger.info("Minimal cleanup: organizing files")
                
            elif cleanup_level == 'moderate':
                # Remove intermediate files but keep results
                logger.info("Moderate cleanup: removing intermediate files")
                logger.info(str(self.run_dir))
                
                # Archive large output files
                large_files = list((self.run_dir / 'inputs').glob('*.out'))
                if large_files:
                    shutil.make_archive(
                        str(archive_dir / 'scale_outputs'),
                        'zip',
                        str(self.run_dir / 'inputs'),
                        base_dir='.'
                    )
                    # Remove original large files
                    for f in large_files:
                        f.unlink()
                
            elif cleanup_level == 'aggressive':
                # Keep only final results
                logger.info("Aggressive cleanup: keeping only final results")
                
                # Archive all intermediate files
                shutil.make_archive(
                    str(archive_dir / 'all_intermediate'),
                    'zip',
                    str(self.run_dir / 'inputs'),
                    base_dir='.'
                )
                
                # Remove input directory
                shutil.rmtree(self.run_dir / 'inputs')
            
            # Create workflow summary
            summary_file = self.run_dir / 'workflow_summary.json'
            with open(summary_file, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
            
            step.complete([str(summary_file)])
            logger.info(f"Cleanup completed with level: {cleanup_level}")
            return True
            
        except Exception as e:
            step.fail(str(e))
            return False
    
    def run_complete_workflow(self, resume_from: Optional[str] = None, 
                            cleanup_level: str = 'minimal') -> bool:
        """Execute the complete workflow"""
        
        self.results['start_time'] = datetime.now()
        logger.info(f"Starting complete workflow: {self.run_name}")
        
        # Define step order
        step_order = ['setup', 'origen-generation', 'origen-verification', 'scale-generation', 'scale-execution', 'mcnp-generation', 'cleanup']
        
        # Find resume point
        start_index = 0
        if resume_from:
            if resume_from in step_order:
                start_index = step_order.index(resume_from)
                logger.info(f"Resuming workflow from step: {resume_from}")
            else:
                logger.error(f"Invalid resume step: {resume_from}")
                return False
        
        # Execute steps
        try:
            for i, step_name in enumerate(step_order):
                if i < start_index:
                    self.steps[step_name].skip("Skipped due to resume point")
                    continue
                
                success = False
                
                if step_name == 'setup':
                    success = self.setup_directories()
                elif step_name == 'origen-generation':
                    success = self.generate_origen_cards()
                elif step_name == 'origen-verification':
                    success = self.verify_origen_cards()
                elif step_name == 'scale-generation':
                    success = self.generate_scale_inputs()
                elif step_name == 'scale-execution':
                    success = self.run_scale_parallel()
                elif step_name == 'mcnp-generation':
                    success = self.generate_mcnp_cards()
                elif step_name == 'cleanup':
                    success = self.cleanup_and_archive(cleanup_level)
                
                if not success:
                    logger.error(f"Workflow failed at step: {step_name}")
                    return False
                
                # Update results
                self.results['steps'][step_name] = {
                    'status': self.steps[step_name].status,
                    'duration': (self.steps[step_name].end_time - self.steps[step_name].start_time).total_seconds() if self.steps[step_name].end_time else None,
                    'output_files': self.steps[step_name].output_files
                }
            
            # Workflow completed successfully
            self.results['end_time'] = datetime.now()
            self.results['total_duration'] = (self.results['end_time'] - self.results['start_time']).total_seconds()
            
            logger.info(f"Workflow completed successfully in {self.results['total_duration']:.1f} seconds")
            logger.info(f"Results available in: {self.run_dir}")
            
            # Print summary
            self.print_summary()
            return True
            
        except Exception as e:
            logger.error(f"Workflow failed with exception: {e}")
            return False
    
    def print_summary(self):
        """Print workflow execution summary"""
        print("\n" + "="*80)
        print(f"WORKFLOW SUMMARY: {self.run_name}")
        print("="*80)
        print(f"Run Directory: {self.run_dir}")
        print(f"Total Duration: {self.results['total_duration']:.1f} seconds")
        print("")
        
        print("STEPS:")
        for step_name, step_data in self.results['steps'].items():
            status_emoji = "✅" if step_data['status'] == 'completed' else "❌" if step_data['status'] == 'failed' else "⏭️"
            duration_str = f"({step_data['duration']:.1f}s)" if step_data['duration'] else ""
            print(f"  {status_emoji} {step_name}: {step_data['status']} {duration_str}")
        
        print(f"\nFILES GENERATED:")
        for file_type, count_or_path in self.results['files_generated'].items():
            if isinstance(count_or_path, int):
                print(f"  - {file_type}: {count_or_path} files")
            else:
                print(f"  - {file_type}: {count_or_path}")
        
        if self.results['statistics']:
            print(f"\nSTATISTICS:")
            for key, value in self.results['statistics'].items():
                print(f"  - {key}: {value}")
        
        print("="*80)

def main():
    """Command line interface for the complete workflow"""
    parser = argparse.ArgumentParser(description="Complete nuclear simulation workflow automation")
    
    # Required arguments
    parser.add_argument('--flux-json', required=True, help='Path to flux data JSON file')
    parser.add_argument('--power-time', help='Path to ORIGEN power/time cards (optional, will be generated if not provided)')
    
    # Optional arguments
    parser.add_argument('--run-name', help='Name for this workflow run')
    parser.add_argument('--scale-workers', type=int, default=8, help='Number of parallel SCALE workers')
    parser.add_argument('--parse-workers', type=int, default=16, help='Number of parallel parsing workers')
    parser.add_argument('--parse-executor', choices=['thread', 'process'], default='thread', 
                       help='Executor type for parsing: thread (default) or process (faster for large datasets)')
    parser.add_argument('--scale-cmd', default='D:\\Scale2\\SCALE-6.3.1\\bin\\scalerte.exe', help='SCALE command to execute')
    parser.add_argument('--cleanup', choices=['minimal', 'moderate', 'aggressive'], 
                       default='minimal', help='Cleanup level')
    
    # ORIGEN generation arguments
    parser.add_argument('--burnup-db', default='combined_yearly_data.db', help='Path to burnup database')
    parser.add_argument('--year', type=int, help='Process data for specific year only')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD format)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD format)')
    parser.add_argument('--skip-origen-generation', action='store_true', 
                       help='Skip ORIGEN generation if power-time file is provided')
    parser.add_argument('--skip-origen-verification', action='store_true',
                       help='Skip ORIGEN verification step')
    parser.add_argument('--origen-tolerance-power', type=float, default=1e-8,
                       help='Power tolerance for ORIGEN verification (MW)')
    parser.add_argument('--origen-tolerance-time', type=float, default=0.001,
                       help='Time tolerance for ORIGEN verification (minutes)')
    
    # Resume options
    parser.add_argument('--resume-from', 
                       choices=['setup', 'origen-generation', 'origen-verification', 'scale-generation', 'scale-execution', 'mcnp-generation', 'cleanup'],
                       help='Resume workflow from specific step')
    parser.add_argument('--run-dir', help='Existing run directory for resume')
    
    # Verbosity
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate arguments

    # Check flux_json file exists (required for SCALE input generation)
    if not Path(args.flux_json).exists():
        logger.error(f"Flux JSON file not found: {args.flux_json}")
        sys.exit(1)

    # Validate ORIGEN generation requirements
    if args.skip_origen_generation:
        # If skipping ORIGEN generation, power-time file MUST be provided and exist
        if not args.power_time:
            logger.error("--power-time must be provided when using --skip-origen-generation")
            sys.exit(1)
        if not Path(args.power_time).exists():
            logger.error(f"Power-time file not found: {args.power_time}")
            sys.exit(1)
    else:
        # If not skipping ORIGEN generation, either power-time must be provided OR burnup-db must exist
        if not args.power_time and not Path(args.burnup_db).exists():
            logger.error("Either --power-time must be provided or --burnup-db must exist for ORIGEN generation")
            sys.exit(1)
        # If power-time is provided, validate it exists
        if args.power_time and not Path(args.power_time).exists():
            logger.error(f"Power-time file not found: {args.power_time}")
            sys.exit(1)
    
    try:
        # Create workflow instance
        if args.resume_from and args.run_dir:
            # Extract run name from run directory
            run_name = Path(args.run_dir).name
            workflow = CompleteWorkflow(
                flux_json=args.flux_json,
                power_time=args.power_time,
                run_name=run_name,
                scale_workers=args.scale_workers,
                parse_workers=args.parse_workers,
                parse_executor=args.parse_executor,
                scale_command=args.scale_cmd,
                burnup_db=args.burnup_db,
                year=args.year,
                start_date=args.start_date,
                end_date=args.end_date,
                skip_origen_generation=args.skip_origen_generation,
                skip_origen_verification=args.skip_origen_verification,
                origen_tolerance_power=args.origen_tolerance_power,
                origen_tolerance_time=args.origen_tolerance_time
            )
        else:
            workflow = CompleteWorkflow(
                flux_json=args.flux_json,
                power_time=args.power_time,
                run_name=args.run_name,
                scale_workers=args.scale_workers,
                parse_workers=args.parse_workers,
                parse_executor=args.parse_executor,
                scale_command=args.scale_cmd,
                burnup_db=args.burnup_db,
                year=args.year,
                start_date=args.start_date,
                end_date=args.end_date,
                skip_origen_generation=args.skip_origen_generation,
                skip_origen_verification=args.skip_origen_verification,
                origen_tolerance_power=args.origen_tolerance_power,
                origen_tolerance_time=args.origen_tolerance_time
            )
        
        # Run workflow
        success = workflow.run_complete_workflow(
            resume_from=args.resume_from,
            cleanup_level=args.cleanup
        )
        
        if success:
            print(f"\n🎉 Workflow completed successfully!")
            print(f"📁 Results: {workflow.run_dir}")
            print(f"📊 MCNP Materials: {workflow.run_dir}/mcnp_cards/mcnp_materials_all.txt")
            sys.exit(0)
        else:
            print(f"\n❌ Workflow failed. Check logs for details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print(f"\n⏹️ Workflow interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Workflow failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()