"""
Main runner for partitioning operations.
Provides CLI and programmatic interfaces.
"""
import os
import json
import collections
import logging
import argparse
import sys
from typing import Dict, Optional, List
from app.partition.groups import groups_for_hand, NONKO_6MAX_PREF, NONKO_9MAX_PREF, PKO_PREF, MYSTERY_PREF, POSTFLOP_ALL
from app.partition.months import month_bucket, make_hand_id, partition_by_month, generate_month_summary
from app.partition.groups import partition_by_group, multi_partition

logger = logging.getLogger(__name__)


def write_nonko_combined(counts_path: str, out_path: str):
    """
    Create combined NON-KO summary by month.
    
    Args:
        counts_path: Path to partition_counts.json
        out_path: Path for output nonko_combined.json
    """
    import json
    import os
    
    with open(counts_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    counts = data.get("counts", {}) or {}
    out = {}
    
    for month, by_group in counts.items():
        x = by_group.get("nonko_9max_pref", {}).get("hands", 0)
        y = by_group.get("nonko_6max_pref", {}).get("hands", 0)
        out[month] = {
            "hands_nonko_9max_pref": x,
            "hands_nonko_6max_pref": y,
            "hands_nonko_pref_total": x + y
        }
    
    with open(out_path, "w", encoding="utf-8") as fo:
        json.dump(out, fo, ensure_ascii=False, indent=2)
    
    logger.info(f"[partition] Created NON-KO combined summary at {out_path}")


def build_partitions(in_jsonl: str, out_dir: str) -> Dict:
    """
    Lê hands_enriched.jsonl e escreve:
      - partition_counts.json (sumário por mês × grupo)
      - index/<YYYY-MM>__<group>.ids (um hand_id por linha)
    Retorna dicionário com totais e métricas.
    
    Args:
        in_jsonl: Input JSONL file path
        out_dir: Output directory path
        
    Returns:
        Dict with totals, metrics and error info
    """
    os.makedirs(out_dir, exist_ok=True)
    index_dir = os.path.join(out_dir, "index")
    os.makedirs(index_dir, exist_ok=True)
    
    counts = collections.defaultdict(lambda: collections.defaultdict(lambda: {"hands": 0}))
    totals = {NONKO_6MAX_PREF: 0, NONKO_9MAX_PREF: 0, PKO_PREF: 0, MYSTERY_PREF: 0, POSTFLOP_ALL: 0}
    id_files = {}  # (month, group) -> file handle
    
    def _fh(month, group):
        key = (month, group)
        if key not in id_files:
            path = os.path.join(index_dir, f"{month}__{group}.ids")
            id_files[key] = open(path, "w", encoding="utf-8")
        return id_files[key]
    
    errors = []
    hands_processed = 0
    last_line_num = 0
    
    with open(in_jsonl, "r", encoding="utf-8") as fi:
        for line_num, line in enumerate(fi, 1):
            last_line_num = line_num
            try:
                hand = json.loads(line)
                hands_processed += 1
                month = month_bucket(
                    hand.get("timestamp_utc", ""),
                    fallback_month=hand.get("month"),
                    debug_context=f"partition-runner:{line_num}",
                )
                hgroups = groups_for_hand(hand)
                hid = make_hand_id(hand)
                for g in hgroups:
                    counts[month][g]["hands"] += 1
                    totals[g] += 1
                    _fh(month, g).write(hid + "\n")
            except Exception as e:
                errors.append({"line": line_num, "error": str(e)})
                logger.error(f"[partition] Error processing line {line_num}: {e}")
    
    for f in id_files.values():
        try:
            f.close()
        except:
            pass
    
    counts_json = {m: {g: v for g, v in d.items()} for m, d in counts.items()}
    counts_path = os.path.join(out_dir, "partition_counts.json")
    with open(counts_path, "w", encoding="utf-8") as fo:
        json.dump({"input": in_jsonl, "totals": totals, "counts": counts_json}, fo, ensure_ascii=False, indent=2)
    
    success_rate = round((hands_processed / max(1, last_line_num)) * 100, 2)
    
    # Generate NON-KO combined summary
    nonko_combined_path = os.path.join(out_dir, "nonko_combined.json")
    write_nonko_combined(counts_path, nonko_combined_path)
    
    return {
        "input": in_jsonl,
        "out_dir": out_dir,
        "counts_path": counts_path,
        "nonko_combined_path": nonko_combined_path,
        "totals": totals,
        "hands_processed": hands_processed,
        "errors": errors,
        "success_rate": success_rate
    }


def run_partition(
    input_file: str,
    output_dir: str,
    partition_type: str = 'month',
    group_by: Optional[str] = None,
    group_fields: Optional[List[str]] = None
) -> dict:
    """
    Run partitioning operation.
    
    Args:
        input_file: Input JSONL file with hands
        output_dir: Output directory for partitions
        partition_type: Type of partition ('month' or 'group')
        group_by: Field to group by (for group partition)
        group_fields: Multiple fields for multi-partition
        
    Returns:
        Result dictionary with partition info
    """
    print(f"Partitioning {input_file}...")
    print(f"Output directory: {output_dir}")
    print(f"Partition type: {partition_type}")
    
    if partition_type == 'month':
        # Partition by month
        output_files = partition_by_month(input_file, output_dir)
        summary = generate_month_summary(output_files)
        
        # Save summary
        summary_file = os.path.join(output_dir, 'month_summary.json')
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n✓ Created {len(output_files)} monthly partitions")
        print(f"✓ Summary saved to {summary_file}")
        return summary
    
    elif partition_type == 'group':
        if group_fields:
            # Multi-partition
            results = multi_partition(input_file, output_dir, group_fields)
            
            # Save summary
            summary_file = os.path.join(output_dir, 'partition_summary.json')
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
            
            print(f"\n✓ Created partitions for {len(group_fields)} fields")
            print(f"✓ Summary saved to {summary_file}")
            return results
        
        elif group_by:
            # Single partition
            output_files = partition_by_group(input_file, output_dir, group_by)
            
            summary = {
                'partition_by': group_by,
                'total_groups': len(output_files),
                'groups': {}
            }
            
            for key, filepath in output_files.items():
                hand_count = sum(1 for _ in open(filepath, 'r'))
                summary['groups'][key] = {
                    'file': filepath,
                    'hands': hand_count
                }
            
            # Save summary
            summary_file = os.path.join(output_dir, f'{group_by}_summary.json')
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2)
            
            print(f"\n✓ Created {len(output_files)} {group_by} partitions")
            print(f"✓ Summary saved to {summary_file}")
            return summary
        
        else:
            raise ValueError("For group partition, specify --group-by or --group-fields")
    
    else:
        raise ValueError(f"Unknown partition type: {partition_type}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Partition poker hands into index files by month and group'
    )
    parser.add_argument(
        '--in',
        dest='input',
        required=True,
        help='Input JSONL file with enriched hands'
    )
    parser.add_argument(
        '--out',
        dest='output',
        required=True,
        help='Output directory for partitions'
    )
    parser.add_argument(
        '--type', '-t',
        choices=['build', 'month', 'group'],
        default='build',
        help='Partition type (default: build)'
    )
    parser.add_argument(
        '--group-by', '-g',
        help='Field to group by (for type=group)'
    )
    parser.add_argument(
        '--group-fields', '-f',
        nargs='+',
        help='Multiple fields for multi-partition (for type=group)'
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        sys.exit(1)
    
    try:
        if args.type == 'build':
            # New partition builder
            print(f"Building partitions from {args.input}...")
            result = build_partitions(args.input, args.output)
            
            print(f"\n✅ Partitioning complete!")
            print(f"  Output directory: {result['out_dir']}")
            print(f"  Counts file: {result['counts_path']}")
            print(f"  Hands processed: {result['hands_processed']}")
            print(f"  Success rate: {result['success_rate']}%")
            
            if result['totals']:
                print("\nTotals by group:")
                for group, count in result['totals'].items():
                    if count > 0:
                        print(f"  {group}: {count}")
            
            if result['errors']:
                print(f"\n⚠ {len(result['errors'])} errors encountered")
                errors_log = os.path.join(args.output, "partition_errors.log")
                with open(errors_log, 'w') as f:
                    json.dump(result['errors'], f, indent=2)
                print(f"  See {errors_log} for details")
                
        else:
            # Legacy partition modes
            result = run_partition(
                args.input,
                args.output,
                args.type,
                args.group_by,
                args.group_fields
            )
            print("\n✅ Partitioning complete!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()