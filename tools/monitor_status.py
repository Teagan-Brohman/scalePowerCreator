#!/usr/bin/env python3
"""
SCALE Job Monitor

This module provides real-time monitoring of SCALE jobs by parsing .msg files
and displaying a status dashboard.
"""

import os
import sys
import time
import json
import argparse
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict
import threading

# Import our custom modules
from scale_msg_parser import ScaleMsgParser

class ScaleJobMonitor:
    """Monitor SCALE jobs in a directory and display status"""
    
    def __init__(self, directory: Path, refresh_interval: int = 5):
        """
        Initialize job monitor
        
        Args:
            directory: Directory to monitor for SCALE jobs
            refresh_interval: Seconds between status refreshes
        """
        self.directory = Path(directory)
        self.refresh_interval = refresh_interval
        self.msg_parser = ScaleMsgParser()
        self.stop_monitoring = False
        self.element_mapping = None  # Will load from JSON if available
        
        # Terminal control sequences
        self.CLEAR_SCREEN = '\033[2J\033[H'
        self.COLORS = {
            'GREEN': '\033[92m',
            'RED': '\033[91m', 
            'YELLOW': '\033[93m',
            'BLUE': '\033[94m',
            'MAGENTA': '\033[95m',
            'CYAN': '\033[96m',
            'WHITE': '\033[97m',
            'BOLD': '\033[1m',
            'END': '\033[0m'
        }
    
    def find_jobs(self, pattern: str = "assembly_*.inp") -> List[Path]:
        """Find all input files in the directory"""
        inp_files = list(self.directory.glob(pattern))
        return sorted(inp_files)
        
    def load_element_mapping(self) -> Optional[Dict]:
        """Load element mapping from JSON file if available"""
        mapping_file = self.directory / "element_mapping.json"
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load element mapping: {e}")
        return None
        
    def group_jobs_by_assembly(self, job_data: List[Dict]) -> Dict[str, List[Dict]]:
        """Group job data by assembly if element mapping is available"""
        if not self.element_mapping:
            return {"All Jobs": job_data}
            
        groups = defaultdict(list)
        ungrouped = []
        
        for job in job_data:
            job_filename = job['inp_file'].name
            if job_filename in self.element_mapping:
                assembly = self.element_mapping[job_filename]['assembly']
                groups[assembly].append(job)
            else:
                ungrouped.append(job)
        
        # Add ungrouped jobs if any
        if ungrouped:
            groups["Ungrouped"] = ungrouped
            
        return dict(groups)
    
    def get_job_status_data(self, inp_files: List[Path]) -> List[Dict]:
        """Get status data for all jobs"""
        job_data = []
        
        for inp_file in inp_files:
            msg_file = inp_file.with_suffix('.msg')
            out_file = inp_file.with_suffix('.out')
            
            job_info = {
                'name': inp_file.stem,
                'inp_file': inp_file,
                'msg_file': msg_file,
                'out_file': out_file,
                'status': 'not_started',
                'return_code': None,
                'runtime_seconds': None,
                'start_time': None,
                'finish_time': None,
                'last_update': None,
                'file_sizes': {
                    'inp': inp_file.stat().st_size if inp_file.exists() else 0,
                    'msg': msg_file.stat().st_size if msg_file.exists() else 0,
                    'out': out_file.stat().st_size if out_file.exists() else 0
                }
            }
            
            # Parse .msg file if it exists
            if msg_file.exists():
                try:
                    msg_data = self.msg_parser.parse_msg_file(msg_file)
                    job_info.update({
                        'status': msg_data['status'],
                        'return_code': msg_data.get('return_code'),
                        'runtime_seconds': msg_data.get('run_time_seconds'),
                        'start_time': msg_data.get('job_started'),
                        'finish_time': msg_data.get('finish_time'),
                        'last_update': msg_data.get('last_update')
                    })
                except Exception as e:
                    job_info['status'] = 'error'
                    job_info['error'] = str(e)
            
            job_data.append(job_info)
        
        return job_data
    
    def get_summary_stats(self, job_data: List[Dict]) -> Dict:
        """Calculate summary statistics"""
        stats = {
            'total': len(job_data),
            'not_started': 0,
            'running': 0,
            'completed': 0,
            'failed': 0,
            'error': 0,
            'total_runtime': 0,
            'avg_runtime': 0,
            'estimated_remaining': None
        }
        
        completed_runtimes = []
        running_jobs = 0
        
        for job in job_data:
            status = job['status']
            stats[status] = stats.get(status, 0) + 1
            
            if job['runtime_seconds']:
                completed_runtimes.append(job['runtime_seconds'])
                stats['total_runtime'] += job['runtime_seconds']
            
            if status == 'running':
                running_jobs += 1
        
        # Calculate averages and estimates
        if completed_runtimes:
            stats['avg_runtime'] = sum(completed_runtimes) / len(completed_runtimes)
            
            # Estimate remaining time
            if running_jobs > 0 and stats['not_started'] > 0:
                estimated_per_job = stats['avg_runtime']
                remaining_jobs = stats['not_started'] + running_jobs
                stats['estimated_remaining'] = estimated_per_job * remaining_jobs
        
        return stats
        
    def get_assembly_stats(self, assembly_jobs: List[Dict]) -> Dict:
        """Calculate statistics for a single assembly"""
        stats = {
            'total': len(assembly_jobs),
            'not_started': 0,
            'running': 0,
            'completed': 0,
            'failed': 0,
            'error': 0
        }
        
        for job in assembly_jobs:
            status = job['status']
            stats[status] = stats.get(status, 0) + 1
            
        return stats
    
    def format_time(self, seconds: Optional[float]) -> str:
        """Format time duration"""
        if seconds is None:
            return "N/A"
        
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"
    
    def format_size(self, bytes_size: int) -> str:
        """Format file size"""
        if bytes_size < 1024:
            return f"{bytes_size}B"
        elif bytes_size < 1024*1024:
            return f"{bytes_size/1024:.1f}KB"
        else:
            return f"{bytes_size/(1024*1024):.1f}MB"
    
    def get_status_color(self, status: str) -> str:
        """Get color code for status"""
        colors = {
            'not_started': self.COLORS['WHITE'],
            'running': self.COLORS['YELLOW'],
            'completed': self.COLORS['GREEN'],
            'failed': self.COLORS['RED'],
            'error': self.COLORS['MAGENTA']
        }
        return colors.get(status, self.COLORS['WHITE'])
    
    def get_status_symbol(self, status: str) -> str:
        """Get symbol for status"""
        symbols = {
            'not_started': '⏳',
            'running': '🔄',
            'completed': '✅',
            'failed': '❌',
            'error': '⚠️'
        }
        return symbols.get(status, '❓')
    
    def display_status(self, job_data: List[Dict], stats: Dict, show_grouped: bool = False):
        """Display the status dashboard"""
        print(self.CLEAR_SCREEN, end='')
        
        # Header
        print(f"{self.COLORS['BOLD']}{self.COLORS['CYAN']}SCALE Parallel Job Monitor{self.COLORS['END']}")
        print(f"{self.COLORS['BLUE']}Directory: {self.directory}{self.COLORS['END']}")
        print(f"Last update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show element mapping status
        if self.element_mapping:
            total_assemblies = len(set(info['assembly'] for info in self.element_mapping.values()))
            print(f"{self.COLORS['CYAN']}Element mode: {len(self.element_mapping)} elements from {total_assemblies} assemblies{self.COLORS['END']}")
        
        print("=" * 80)
        
        # Summary stats
        print(f"\n{self.COLORS['BOLD']}SUMMARY{self.COLORS['END']}")
        total = stats['total']
        print(f"Total Jobs: {total}")
        print(f"Not Started: {self.COLORS['WHITE']}{stats['not_started']}{self.COLORS['END']} | "
              f"Running: {self.COLORS['YELLOW']}{stats['running']}{self.COLORS['END']} | "
              f"Completed: {self.COLORS['GREEN']}{stats['completed']}{self.COLORS['END']} | "
              f"Failed: {self.COLORS['RED']}{stats['failed']}{self.COLORS['END']} | "
              f"Error: {self.COLORS['MAGENTA']}{stats['error']}{self.COLORS['END']}")
        
        if total > 0:
            progress = (stats['completed'] + stats['failed']) / total * 100
            print(f"Progress: {progress:.1f}% ({stats['completed'] + stats['failed']}/{total})")
        
        if stats['avg_runtime'] > 0:
            print(f"Average Runtime: {self.format_time(stats['avg_runtime'])}")
            
        if stats['estimated_remaining']:
            print(f"Estimated Remaining: {self.format_time(stats['estimated_remaining'])}")
        
        # Display jobs grouped by assembly if element mapping is available
        if show_grouped and self.element_mapping:
            self.display_grouped_status(job_data)
        else:
            self.display_job_list(job_data)
        
        # Instructions
        print("\n" + "=" * 80)
        print("Press Ctrl+C to exit monitoring")
        if self.element_mapping:
            print("Use --grouped to show assembly groupings")
        
        if stats['running'] == 0 and stats['not_started'] == 0:
            print(f"\n{self.COLORS['BOLD']}{self.COLORS['GREEN']}All jobs completed!{self.COLORS['END']}")
            
    def display_grouped_status(self, job_data: List[Dict]):
        """Display jobs grouped by assembly"""
        grouped_jobs = self.group_jobs_by_assembly(job_data)
        
        print(f"\n{self.COLORS['BOLD']}JOBS BY ASSEMBLY{self.COLORS['END']}")
        
        for assembly_name, assembly_jobs in grouped_jobs.items():
            assembly_stats = self.get_assembly_stats(assembly_jobs)
            
            # Assembly header with stats
            print(f"\n{self.COLORS['BOLD']}{self.COLORS['BLUE']}{assembly_name}{self.COLORS['END']} "
                  f"({assembly_stats['total']} elements)")
            print(f"  Status: {self.COLORS['GREEN']}{assembly_stats['completed']}{self.COLORS['END']} completed, "
                  f"{self.COLORS['YELLOW']}{assembly_stats['running']}{self.COLORS['END']} running, "
                  f"{self.COLORS['WHITE']}{assembly_stats['not_started']}{self.COLORS['END']} pending, "
                  f"{self.COLORS['RED']}{assembly_stats['failed']}{self.COLORS['END']} failed")
            
            # Element details for this assembly
            for job in assembly_jobs[:10]:  # Show first 10 elements
                status = job['status']
                color = self.get_status_color(status)
                symbol = self.get_status_symbol(status)
                
                # Get element info from mapping
                element_info = ""
                job_filename = job['inp_file'].name
                if job_filename in self.element_mapping:
                    element_key = self.element_mapping[job_filename]['element_key']
                    element_number = self.element_mapping[job_filename]['element_number']
                    element_info = f"E{element_number:03d}"
                
                runtime = self.format_time(job['runtime_seconds'])
                return_code = str(job['return_code']) if job['return_code'] is not None else "-"
                
                print(f"    {color}{symbol}{self.COLORS['END']} {element_info:<6} "
                      f"{status:<12} {runtime:<8} RC:{return_code}")
            
            # Show summary if more than 10 elements
            if len(assembly_jobs) > 10:
                remaining = len(assembly_jobs) - 10
                print(f"    ... and {remaining} more elements")
                
    def display_job_list(self, job_data: List[Dict]):
        """Display simple job list (original format)"""
        print(f"\n{self.COLORS['BOLD']}JOB STATUS{self.COLORS['END']}")
        print(f"{'Status':<8} {'Job Name':<25} {'Runtime':<10} {'Return':<6} {'Files':<15}")
        print("-" * 80)
        
        for job in job_data:
            status = job['status']
            color = self.get_status_color(status)
            symbol = self.get_status_symbol(status)
            
            job_name = job['name']
            if len(job_name) > 25:
                job_name = job_name[:22] + "..."
            
            runtime = self.format_time(job['runtime_seconds'])
            return_code = str(job['return_code']) if job['return_code'] is not None else "-"
            
            # File sizes
            msg_size = self.format_size(job['file_sizes']['msg'])
            out_size = self.format_size(job['file_sizes']['out'])
            files = f"{msg_size}/{out_size}"
            
            print(f"{color}{symbol} {status:<6}{self.COLORS['END']} "
                  f"{job_name:<25} {runtime:<10} {return_code:<6} {files:<15}")
    
    def run_monitor(self, pattern: str = "assembly_*.inp", show_grouped: bool = False):
        """Run the monitoring loop"""
        print(f"Starting SCALE job monitor...")
        print(f"Directory: {self.directory}")
        print(f"Pattern: {pattern}")
        print(f"Refresh interval: {self.refresh_interval}s")
        
        # Try to load element mapping
        self.element_mapping = self.load_element_mapping()
        if self.element_mapping:
            print(f"Loaded element mapping with {len(self.element_mapping)} elements")
            if show_grouped:
                print("Assembly grouping enabled")
        
        print("Press Ctrl+C to exit\n")
        
        try:
            while not self.stop_monitoring:
                # Find jobs
                inp_files = self.find_jobs(pattern)
                
                if not inp_files:
                    print(f"No jobs found matching pattern '{pattern}' in {self.directory}")
                    time.sleep(self.refresh_interval)
                    continue
                
                # Get status data
                job_data = self.get_job_status_data(inp_files)
                stats = self.get_summary_stats(job_data)
                
                # Display status
                self.display_status(job_data, stats, show_grouped)
                
                # Check if all jobs are done
                if stats['not_started'] == 0 and stats['running'] == 0:
                    print(f"\n{self.COLORS['BOLD']}Monitoring complete - all jobs finished{self.COLORS['END']}")
                    break
                
                time.sleep(self.refresh_interval)
                
        except KeyboardInterrupt:
            print(f"\n{self.COLORS['YELLOW']}Monitoring stopped by user{self.COLORS['END']}")
        except Exception as e:
            print(f"\n{self.COLORS['RED']}Error during monitoring: {e}{self.COLORS['END']}")
    
    def stop(self):
        """Stop the monitoring loop"""
        self.stop_monitoring = True

def main():
    """Command line interface for the monitor"""
    parser = argparse.ArgumentParser(
        description="Monitor SCALE parallel job execution",
        epilog="""Examples:
  # Monitor assembly-based jobs
  python monitor_status.py --pattern "assembly_*.inp"
  
  # Monitor element-based jobs with assembly grouping
  python monitor_status.py --element-mode
  
  # Monitor with custom pattern and grouping
  python monitor_status.py --pattern "element_*.inp" --grouped
  
  # Single status check for elements
  python monitor_status.py --element-mode --once""",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--directory", "-d", default=".", 
                       help="Directory to monitor (default: current directory)")
    parser.add_argument("--refresh", "-r", type=int, default=5,
                       help="Refresh interval in seconds (default: 5)")
    parser.add_argument("--pattern", "-p", default="assembly_*.inp",
                       help="Pattern to match input files (default: assembly_*.inp for assemblies, element_*.inp for elements)")
    parser.add_argument("--once", action="store_true",
                       help="Run once and exit (no continuous monitoring)")
    parser.add_argument("--grouped", "-g", action="store_true",
                       help="Show element jobs grouped by assembly (requires element_mapping.json)")
    parser.add_argument("--element-mode", "-e", action="store_true",
                       help="Automatically use element_*.inp pattern and enable grouping")
    
    args = parser.parse_args()
    
    # Adjust pattern and grouping for element mode
    pattern = args.pattern
    show_grouped = args.grouped
    
    if args.element_mode:
        pattern = "element_*.inp"
        show_grouped = True
        print("Element mode enabled: using pattern 'element_*.inp' with assembly grouping")
    
    # Create monitor
    monitor = ScaleJobMonitor(Path(args.directory), args.refresh)
    
    if args.once:
        # Single status check
        monitor.element_mapping = monitor.load_element_mapping()
        inp_files = monitor.find_jobs(pattern)
        if inp_files:
            job_data = monitor.get_job_status_data(inp_files)
            stats = monitor.get_summary_stats(job_data)
            monitor.display_status(job_data, stats, show_grouped)
        else:
            print(f"No jobs found matching '{pattern}' in {args.directory}")
    else:
        # Continuous monitoring
        monitor.run_monitor(pattern, show_grouped)

if __name__ == "__main__":
    main()