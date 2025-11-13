# app/stats/runner.py
import argparse
import os
import sys
import logging
from app.stats.engine import run_stats

def main():
    ap = argparse.ArgumentParser(description="Compute stats from enriched hands using DSL")
    ap.add_argument("--in", dest="in_jsonl", required=True, help="parsed/hands_enriched.jsonl")
    ap.add_argument("--dsl", dest="dsl_path", default="app/stats/dsl/stats.yml")
    ap.add_argument("--out", dest="out_dir", default="stats")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO),
                        format="%(levelname)s:%(name)s:%(message)s")

    if not os.path.exists(args.in_jsonl):
        print(f"❌ Input not found: {args.in_jsonl}")
        sys.exit(1)
    if not os.path.exists(args.dsl_path):
        print(f"❌ DSL not found: {args.dsl_path}")
        sys.exit(1)
    os.makedirs(args.out_dir, exist_ok=True)

    res = run_stats(args.in_jsonl, args.dsl_path, args.out_dir)
    print("\n✅ Stats OK")
    print(f"   Hands processed : {res['hands_processed']}")
    print(f"   Stats computed  : {res['stats_computed']}")
    print(f"   Months generated: {res['months_generated']}")
    print(f"   Output file     : {res['output_path']}")
    print(f"   Index dir       : {res['index_dir']}")
    if res["errors"]:
        print(f"   ⚠️ Errors       : {res['errors']} (see stats_errors.log)")

if __name__ == "__main__":
    main()