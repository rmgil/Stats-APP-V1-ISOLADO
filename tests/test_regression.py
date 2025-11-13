#!/usr/bin/env python3
"""
Regression test to ensure file distribution remains the same between old and new implementations.
Tests that files are placed in the same folders regardless of the new encoding detection and unknown state.
"""

import sys
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Set, Tuple
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import regex patterns from main
from main import WORD_MYSTERY, WORD_PKO

class OldPipeline:
    """
    Simulates the old classification logic without chardet and unknown state.
    """
    @staticmethod
    def classify_file(file_path: Path) -> str:
        filename = file_path.name
        
        # Try to read with different encodings (old method)
        content = None
        for encoding in ['utf-8', 'latin1', 'cp1252']:
            try:
                content = file_path.read_text(encoding=encoding, errors='ignore')
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            content = ""  # Old behavior: empty content, check filename only
        
        # Check for mystery words
        if WORD_MYSTERY.search(filename) or WORD_MYSTERY.search(content):
            return "MYSTERIES"
        
        # Check for PKO words
        if WORD_PKO.search(filename) or WORD_PKO.search(content):
            return "PKO"
        
        # Everything else goes to NON-KO (including empty/binary files in old logic)
        return "NON-KO"


class NewPipeline:
    """
    Simulates the new classification logic with chardet and unknown state.
    """
    @staticmethod
    def classify_file(file_path: Path) -> str:
        import chardet
        
        filename = file_path.name
        
        # Read file in binary mode
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read()
        except:
            # If we can't read, it goes to NON-KO (unknown internally)
            return "NON-KO"
        
        # Check if empty
        if not raw_data:
            # Empty files are unknown, but go to NON-KO folder
            return "NON-KO"
        
        # Detect encoding with chardet
        detection = chardet.detect(raw_data)
        detected_encoding = detection.get('encoding', 'utf-8') if detection else 'utf-8'
        confidence = detection.get('confidence', 0) if detection else 0
        
        # Try to decode
        if confidence < 0.7:
            encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
            if detected_encoding and detected_encoding not in encodings_to_try:
                encodings_to_try.insert(0, detected_encoding)
        else:
            encodings_to_try = [detected_encoding, 'utf-8', 'latin1']
        
        content = None
        for encoding in encodings_to_try:
            try:
                if encoding:
                    content = raw_data.decode(encoding, errors='replace')
                    break
            except (UnicodeDecodeError, LookupError):
                continue
        
        if content is None:
            content = raw_data.decode('utf-8', errors='replace')
        
        # Check if too many replacement characters (binary file)
        if content.count('�') > len(content) * 0.3:
            # Unknown files go to NON-KO folder
            return "NON-KO"
        
        # Check for mystery words
        if WORD_MYSTERY.search(filename) or WORD_MYSTERY.search(content):
            return "MYSTERIES"
        
        # Check for PKO words
        if WORD_PKO.search(filename) or WORD_PKO.search(content):
            return "PKO"
        
        # Regular files go to NON-KO
        return "NON-KO"


def run_test():
    """
    Main test function that compares old and new pipeline results.
    """
    fixtures_dir = Path("tests/fixtures/test_files")
    
    if not fixtures_dir.exists():
        print(f"❌ Fixtures directory not found: {fixtures_dir}")
        return False
    
    # List all test files
    test_files = list(fixtures_dir.glob("*.txt"))
    
    if not test_files:
        print(f"❌ No test files found in {fixtures_dir}")
        return False
    
    print(f"Testing {len(test_files)} files...")
    print("-" * 60)
    
    # Collect results from both pipelines
    old_results = {}
    new_results = {}
    
    for file in test_files:
        old_folder = OldPipeline.classify_file(file)
        new_folder = NewPipeline.classify_file(file)
        
        old_results[file.name] = old_folder
        new_results[file.name] = new_folder
        
        status = "✅" if old_folder == new_folder else "❌"
        print(f"{status} {file.name:30} Old: {old_folder:10} New: {new_folder:10}")
    
    print("-" * 60)
    
    # Compare results
    differences = []
    for filename in old_results:
        if old_results[filename] != new_results[filename]:
            differences.append({
                'file': filename,
                'old': old_results[filename],
                'new': new_results[filename]
            })
    
    if differences:
        print(f"\n❌ REGRESSION TEST FAILED: {len(differences)} differences found!\n")
        print("Differences:")
        print("-" * 60)
        for diff in differences:
            print(f"File: {diff['file']}")
            print(f"  Old pipeline: {diff['old']}")
            print(f"  New pipeline: {diff['new']}")
            print()
        return False
    else:
        print(f"\n✅ REGRESSION TEST PASSED: All {len(test_files)} files classified identically!")
        print("\nDistribution summary:")
        
        # Count files per folder
        folder_counts = {"PKO": 0, "MYSTERIES": 0, "NON-KO": 0}
        for folder in old_results.values():
            folder_counts[folder] += 1
        
        for folder, count in folder_counts.items():
            print(f"  {folder}: {count} files")
        
        return True


if __name__ == "__main__":
    success = run_test()
    sys.exit(0 if success else 1)