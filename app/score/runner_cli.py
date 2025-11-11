
"""
CLI runner for scoring system
"""
import argparse
import logging
import json
import os
import sys
from app.score.runner import build_scorecard

def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Generate player scorecard from stats")
    parser.add_argument("stat_counts", help="Path to stat_counts.json from Phase 5")
    parser.add_argument("-c", "--config", default="app/score/config.yml", help="Config file path")
    parser.add_argument("-o", "--output", default="scores", help="Output directory")
    parser.add_argument("-f", "--force", action="store_true", help="Force rebuild (ignore cache)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    # Validate inputs
    if not os.path.exists(args.stat_counts):
        print(f"Error: stat_counts file not found: {args.stat_counts}")
        sys.exit(1)
        
    if not os.path.exists(args.config):
        print(f"Error: config file not found: {args.config}")
        sys.exit(1)
    
    try:
        print(f"Building scorecard...")
        print(f"  Stats: {args.stat_counts}")
        print(f"  Config: {args.config}")
        print(f"  Output: {args.output}")
        print(f"  Force: {args.force}")
        
        result = build_scorecard(args.stat_counts, args.config, args.output, args.force)
        
        print(f"\n✅ Success!")
        print(f"  Scorecard: {result['scorecard_path']}")
        if result.get('overall') is not None:
            print(f"  Overall Score: {result['overall']:.2f}/100")
        else:
            print(f"  Overall Score: Not enough data")
            
        # Show exports
        exports_dir = os.path.join(args.output, "exports")
        if os.path.exists(exports_dir):
            print(f"  Exports: {exports_dir}/")
            for f in os.listdir(exports_dir):
                print(f"    - {f}")
        
    except Exception as e:
        print(f"Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
