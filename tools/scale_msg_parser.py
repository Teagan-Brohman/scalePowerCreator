#!/usr/bin/env python3
"""
SCALE Message File Parser

This module provides utilities for parsing SCALE .msg files to determine
job status, completion, and execution details.
"""

import re
import logging
from pathlib import Path
from typing import Dict, Optional, Union
from datetime import datetime

logger = logging.getLogger(__name__)

class ScaleMsgParser:
    """Parser for SCALE .msg files to extract job status and completion information"""
    
    def __init__(self):
        # Regex patterns for parsing .msg files
        self.patterns = {
            'job_started': r'SCALE Job started on\s+(.+)',
            'host_name': r'With Host name\s+(\S+)',
            'process_id': r'and process id\s+(\d+)',
            'input_file': r'Input file named\s+(.+)',
            'output_file': r'and output file named\s+(.+)',
            'job_finished': r'Scale job\s+(.+?)\s+is finished',
            'output_stored': r'Output is stored in\s+(.+)',
            'return_code': r'Process finished with\s+(\d+)\s+return code',
            'run_time': r'ran in\s+(\d+)\s+secs',
            'finish_time': r'finished at\s+(.+)',
            'executing': r'Now executing\s+(\S+)'
        }
    
    def parse_msg_file(self, msg_path: Union[str, Path]) -> Dict:
        """
        Parse a SCALE .msg file for complete job information
        
        Args:
            msg_path: Path to the .msg file
            
        Returns:
            Dictionary containing job status information
        """
        msg_path = Path(msg_path)
        
        result = {
            'msg_file': str(msg_path),
            'status': 'unknown',
            'job_started': None,
            'host_name': None,
            'process_id': None,
            'input_file': None,
            'output_file': None,
            'job_finished': None,
            'return_code': None,
            'run_time_seconds': None,
            'finish_time': None,
            'executing_modules': [],
            'last_update': None
        }
        
        if not msg_path.exists():
            result['status'] = 'not_started'
            return result
        
        try:
            # Get file modification time
            result['last_update'] = datetime.fromtimestamp(msg_path.stat().st_mtime)
            
            with open(msg_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Parse all patterns
            for key, pattern in self.patterns.items():
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    if key == 'job_started':
                        result['job_started'] = match.group(1).strip()
                    elif key == 'host_name':
                        result['host_name'] = match.group(1).strip()
                    elif key == 'process_id':
                        result['process_id'] = int(match.group(1))
                    elif key == 'input_file':
                        result['input_file'] = match.group(1).strip()
                    elif key == 'output_file':
                        result['output_file'] = match.group(1).strip()
                    elif key == 'job_finished':
                        result['job_finished'] = match.group(1).strip()
                    elif key == 'output_stored':
                        result['output_stored'] = match.group(1).strip()
                    elif key == 'return_code':
                        result['return_code'] = int(match.group(1))
                    elif key == 'run_time':
                        result['run_time_seconds'] = int(match.group(1))
                    elif key == 'finish_time':
                        result['finish_time'] = match.group(1).strip()
            
            # Find all executing modules
            executing_matches = re.findall(self.patterns['executing'], content, re.IGNORECASE)
            result['executing_modules'] = executing_matches
            
            # Determine job status
            result['status'] = self._determine_status(result, content)
            
            return result
            
        except Exception as e:
            logger.error(f"Error parsing {msg_path}: {e}")
            result['status'] = 'error'
            result['error'] = str(e)
            return result
    
    def _determine_status(self, parsed_data: Dict, content: str) -> str:
        """
        Determine job status based on parsed data and content
        
        Args:
            parsed_data: Dictionary of parsed message data
            content: Raw content of the .msg file
            
        Returns:
            Status string: 'not_started', 'running', 'completed', 'failed', 'error'
        """
        # First check the actual SCALE output file if available
        if parsed_data.get('output_file'):
            scale_status = self._check_scale_output_status(parsed_data['output_file'])
            if scale_status is not None:
                return scale_status
        
        # Fallback to .msg file analysis
        # Check for completion markers
        if parsed_data.get('return_code') is not None:
            if parsed_data['return_code'] == 0:
                return 'completed'
            else:
                return 'failed'
        
        # Check for job finished message (even without return code)
        if parsed_data.get('job_finished'):
            return 'completed'
        
        # Check for error indicators using word boundary matching to avoid false positives
        # Use regex patterns to match whole words only, excluding benign contexts
        error_patterns = [
            r'\berror\b(?!\s+(?:tolerance|bound|estimate|bar|margin|limit|threshold|norm))',  # 'error' but not 'error tolerance', etc.
            r'\bfailed\b',
            r'\babort(?:ed)?\b',
            r'\bexception\b',
            r'\bterminated abnormally\b',
            r'\bfatal\b',
            r'\bcrash(?:ed)?\b',
            r'\bsegmentation fault\b'
        ]

        # Additional exclusion patterns - these are benign uses of 'error'
        benign_patterns = [
            r'(?:maximum|max|relative|absolute|truncation|convergence|numerical|roundoff|round-off)\s+error',
            r'error\s+(?:tolerance|bound|estimate|bar|margin|limit|threshold|norm)',
            r'no\s+error',
            r'zero\s+error',
            r'error\s*[=<>:]',  # error = 0.001, error < 1e-5, etc.
        ]

        content_lower = content.lower()

        for pattern in error_patterns:
            match = re.search(pattern, content_lower)
            if match:
                matched_text = match.group(0)
                # Check if this match is part of a benign context
                is_benign = False
                for benign in benign_patterns:
                    # Search in a window around the match to check for benign context
                    start = max(0, match.start() - 30)
                    end = min(len(content_lower), match.end() + 30)
                    context = content_lower[start:end]
                    if re.search(benign, context):
                        is_benign = True
                        break

                if not is_benign:
                    logger.debug(f"Found error indicator '{matched_text}' in msg file")
                    return 'failed'
        
        # Check if job has started
        if parsed_data.get('job_started'):
            return 'running'
        
        return 'not_started'
    
    def _check_scale_output_status(self, output_file_path: str) -> Optional[str]:
        """
        Check SCALE output file for actual completion status
        
        Args:
            output_file_path: Path to SCALE output file (.out)
            
        Returns:
            'completed' if successful, 'failed' if errors detected, None if indeterminate
        """
        try:
            output_path = Path(output_file_path)
            if not output_path.exists():
                return None
            
            # Read the last part of the file to find the summary section
            with open(output_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Read last 50KB to capture the summary section
                f.seek(0, 2)  # Go to end
                file_size = f.tell()
                read_size = min(50000, file_size)  # Read last 50KB or entire file
                f.seek(max(0, file_size - read_size))
                content = f.read().lower()
            
            # Look for SCALE summary patterns
            if '------------------------ end summary ------------------------' in content:
                # Extract the summary section
                summary_start = content.rfind('-------------------------- summary --------------------------')
                if summary_start != -1:
                    summary_section = content[summary_start:]
                    
                    # Check for failure indicators in summary
                    failure_patterns = [
                        'terminated due to errors',
                        'completion code 1',
                        'failed',
                        'error',
                        'abort'
                    ]
                    
                    for pattern in failure_patterns:
                        if pattern in summary_section:
                            logger.debug(f"Found failure pattern '{pattern}' in SCALE output summary")
                            return 'failed'
                    
                    # Check for success indicators
                    success_patterns = [
                        'origen finished',
                        'scale finished',
                        'completed successfully'
                    ]
                    
                    for pattern in success_patterns:
                        if pattern in summary_section:
                            logger.debug(f"Found success pattern '{pattern}' in SCALE output summary")
                            return 'completed'
            
            # If we can't determine from summary, return None to fall back to .msg analysis
            return None
            
        except Exception as e:
            logger.debug(f"Error checking SCALE output status: {e}")
            return None
    
    def is_job_complete(self, msg_path: Union[str, Path]) -> bool:
        """
        Quick check if job is complete
        
        Args:
            msg_path: Path to the .msg file
            
        Returns:
            True if job is complete (success or failure), False if still running or not started
        """
        try:
            parsed = self.parse_msg_file(msg_path)
            return parsed['status'] in ['completed', 'failed']
        except Exception:
            return False
    
    def is_job_successful(self, msg_path: Union[str, Path]) -> bool:
        """
        Check if job completed successfully
        
        Args:
            msg_path: Path to the .msg file
            
        Returns:
            True if job completed with return code 0, False otherwise
        """
        try:
            parsed = self.parse_msg_file(msg_path)
            return parsed['status'] == 'completed' and parsed.get('return_code') == 0
        except Exception:
            return False
    
    def get_job_status(self, msg_path: Union[str, Path]) -> str:
        """
        Get current job status
        
        Args:
            msg_path: Path to the .msg file
            
        Returns:
            Status string: 'not_started', 'running', 'completed', 'failed', 'error'
        """
        try:
            parsed = self.parse_msg_file(msg_path)
            return parsed['status']
        except Exception:
            return 'error'
    
    def get_job_runtime(self, msg_path: Union[str, Path]) -> Optional[int]:
        """
        Get job runtime in seconds
        
        Args:
            msg_path: Path to the .msg file
            
        Returns:
            Runtime in seconds, or None if not available
        """
        try:
            parsed = self.parse_msg_file(msg_path)
            return parsed.get('run_time_seconds')
        except Exception:
            return None
    
    def get_return_code(self, msg_path: Union[str, Path]) -> Optional[int]:
        """
        Get job return code
        
        Args:
            msg_path: Path to the .msg file
            
        Returns:
            Return code, or None if not available
        """
        try:
            parsed = self.parse_msg_file(msg_path)
            return parsed.get('return_code')
        except Exception:
            return None

def main():
    """Test the ScaleMsgParser with example files"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python scale_msg_parser.py <msg_file>")
        sys.exit(1)
    
    parser = ScaleMsgParser()
    msg_file = sys.argv[1]
    
    print(f"Parsing: {msg_file}")
    result = parser.parse_msg_file(msg_file)
    
    print("\nParsed Results:")
    print("=" * 50)
    for key, value in result.items():
        print(f"{key:<20}: {value}")
    
    print("\nQuick Status Checks:")
    print("=" * 50)
    print(f"Is complete:    {parser.is_job_complete(msg_file)}")
    print(f"Is successful:  {parser.is_job_successful(msg_file)}")
    print(f"Current status: {parser.get_job_status(msg_file)}")
    print(f"Runtime:        {parser.get_job_runtime(msg_file)} seconds")
    print(f"Return code:    {parser.get_return_code(msg_file)}")

if __name__ == "__main__":
    main()