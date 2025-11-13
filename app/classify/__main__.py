#!/usr/bin/env python3
"""
CLI for classification pipeline.
Usage: python -m app.classify input_dir=./uploads out_zip=./out.zip [--dry-run]
"""

import os
import sys
import shutil
import tempfile
import uuid
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import from main
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import the same functions used by the drag-and-drop route
from main import (
    process_txt_tree,
    unpack_any,
    app
)


def process_directory(input_dir: Path, output_zip: Path, dry_run: bool = False):
    """
    Process files in input directory and create output ZIP.
    Uses the same pipeline as the web interface.
    
    Args:
        input_dir: Directory containing files to process
        output_zip: Path for output ZIP file
        dry_run: If True, only create manifest without ZIP
    
    Returns:
        dict with processing statistics
    """
    # Generate session ID for tracking
    session_id = uuid.uuid4().hex
    
    # Create temporary directories (same as upload route)
    temp_root = Path(tempfile.gettempdir())
    tmp_base = temp_root / f"txt_filter_cli_{session_id}"
    work_dir = tmp_base / 'work'
    out_dir = tmp_base / 'out'
    
    try:
        # Create directories
        work_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(exist_ok=True)
        
        print(f"Processing files from: {input_dir}")
        
        # Check if input is archive or directory
        if input_dir.is_file():
            # Extract archive first (same as web route)
            print("Extracting archive...")
            success = unpack_any(input_dir, work_dir)
            if not success:
                raise Exception(f"Failed to extract archive: {input_dir}")
        else:
            # Copy directory contents to work directory
            print("Copying directory contents...")
            if input_dir.exists():
                for item in input_dir.iterdir():
                    if item.is_file():
                        shutil.copy2(item, work_dir)
                    elif item.is_dir():
                        shutil.copytree(item, work_dir / item.name, dirs_exist_ok=True)
            else:
                raise Exception(f"Input directory not found: {input_dir}")
        
        # Track timing for manifest
        start_time = datetime.now()
        
        # Process files (same function as web route)
        print("Processing TXT and XML files...")
        stats = process_txt_tree(work_dir, out_dir)
        
        end_time = datetime.now()
        
        # Create classification manifest (same as web route)
        manifest = {
            "run_id": session_id,
            "started_at": start_time.isoformat(),
            "finished_at": end_time.isoformat(),
            "totals": {
                "PKO": stats.get('pko', 0),
                "mystery": stats.get('mystery', 0),
                "non-KO": stats.get('nonko', 0),
                "unknown": stats.get('unknown', 0)
            },
            "files": stats.get('file_classifications', [])
        }
        
        # Save manifest to output directory
        manifest_path = out_dir / 'classification_manifest.json'
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        
        print(f"\nProcessing complete:")
        print(f"  Total files: {stats.get('processed', 0)}")
        print(f"  PKO: {stats.get('pko', 0)}")
        print(f"  Mystery: {stats.get('mystery', 0)}")
        print(f"  Non-KO: {stats.get('nonko', 0)}")
        print(f"  Unknown: {stats.get('unknown', 0)}")
        
        if dry_run:
            print(f"\nDRY RUN mode - manifest saved at: {manifest_path}")
            print("No ZIP file created.")
            return manifest
        
        # Create ZIP file (same as web route)
        print(f"\nCreating ZIP file: {output_zip}")
        
        # Ensure parent directory exists
        output_zip.parent.mkdir(parents=True, exist_ok=True)
        
        # Create ZIP
        zip_path_without_suffix = str(output_zip.with_suffix(''))
        shutil.make_archive(zip_path_without_suffix, 'zip', str(out_dir))
        
        # Verify ZIP was created
        if output_zip.exists():
            file_size = output_zip.stat().st_size
            print(f"✓ ZIP created successfully ({file_size:,} bytes)")
        else:
            raise Exception("Failed to create ZIP file")
        
        return manifest
        
    finally:
        # Cleanup temporary directories
        if tmp_base.exists() and not dry_run:
            shutil.rmtree(tmp_base, ignore_errors=True)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Classify tournament text files into PKO, Mystery, and Non-KO categories.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python -m app.classify input_dir=./uploads out_zip=./result.zip
  python -m app.classify input_dir=./archive.zip out_zip=./output.zip
  python -m app.classify input_dir=./data out_zip=./classified.zip --dry-run
        '''
    )
    
    # Parse custom format arguments
    args_list = []
    input_dir = None
    out_zip = None
    dry_run = False
    
    for arg in sys.argv[1:]:
        if arg.startswith('input_dir='):
            input_dir = arg.split('=', 1)[1]
        elif arg.startswith('out_zip='):
            out_zip = arg.split('=', 1)[1]
        elif arg == '--dry-run':
            dry_run = True
        elif arg != sys.argv[0] and not arg.endswith('.py'):
            # Unknown argument
            print(f"Unknown argument: {arg}")
            print("\nUsage: python -m app.classify input_dir=PATH out_zip=PATH [--dry-run]")
            sys.exit(1)
    
    # Validate required arguments
    if not input_dir:
        print("Error: input_dir is required")
        print("\nUsage: python -m app.classify input_dir=PATH out_zip=PATH [--dry-run]")
        sys.exit(1)
    
    if not out_zip and not dry_run:
        print("Error: out_zip is required (unless using --dry-run)")
        print("\nUsage: python -m app.classify input_dir=PATH out_zip=PATH [--dry-run]")
        sys.exit(1)
    
    # Set default output for dry-run
    if dry_run and not out_zip:
        out_zip = "./dry_run_output.zip"
    
    # Convert to Path objects
    input_path = Path(input_dir)
    output_path = Path(out_zip)
    
    # Check if input exists
    if not input_path.exists():
        print(f"Error: Input path does not exist: {input_path}")
        sys.exit(1)
    
    # Initialize Flask app context (needed for logging)
    with app.app_context():
        try:
            # Process files
            manifest = process_directory(input_path, output_path, dry_run)
            
            # Success
            print("\n✓ Classification completed successfully!")
            if not dry_run:
                print(f"Output saved to: {output_path}")
            
        except Exception as e:
            print(f"\n✗ Error during processing: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()