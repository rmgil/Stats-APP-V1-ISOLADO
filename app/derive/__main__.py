"""
CLI for hand enrichment with derived data.

Usage:
    python -m app.derive --input hands.jsonl --output hands_enriched.jsonl
"""
import argparse
import logging
import sys
from app.derive.runner import enrich_hands

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    parser = argparse.ArgumentParser(
        description='Enrich poker hands with derived data (Phase 3)'
    )
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input JSONL file with parsed hands'
    )
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Output JSONL file for enriched hands'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        print(f"Enriching hands from {args.input}...")
        result = enrich_hands(args.input, args.output)
        
        if "error" in result:
            print(f"❌ Error: {result['error']}")
            sys.exit(1)
        
        print(f"✅ Success!")
        print(f"  Hands processed: {result['hands']}")
        print(f"  Output: {result['output']}")
        print(f"  Statistics: {result['stats_path']}")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()