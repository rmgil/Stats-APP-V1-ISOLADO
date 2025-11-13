"""
Partition validation module for integrity checks.
"""
import json
import os
from typing import Dict
from app.partition.groups import groups_for_hand
from app.partition.months import month_bucket, make_hand_id


def validate_partitions(counts_path: str, hands_jsonl: str) -> Dict:
    """
    Reconta as mãos diretamente do JSONL e compara com partition_counts.json.
    Retorna discrepâncias (se houver) e estatísticas.
    
    Args:
        counts_path: Path to partition_counts.json
        hands_jsonl: Path to hands JSONL file
        
    Returns:
        Validation report dictionary
    """
    if not os.path.exists(counts_path):
        return {"error": "counts_path not found"}
    if not os.path.exists(hands_jsonl):
        return {"error": "hands_jsonl not found"}

    with open(counts_path, "r", encoding="utf-8") as f:
        counts_data = json.load(f)
    expected = counts_data.get("counts", {}) or {}

    # Recontagem
    actual = {}
    with open(hands_jsonl, "r", encoding="utf-8") as fi:
        for line in fi:
            h = json.loads(line)
            month = month_bucket(h.get("timestamp_utc"))
            for g in groups_for_hand(h):
                actual.setdefault(month, {}).setdefault(g, 0)
                actual[month][g] += 1

    # Comparar
    diffs = []
    months = set(expected.keys()) | set(actual.keys())
    for m in sorted(months):
        eg = expected.get(m, {})
        ag = actual.get(m, {})
        groups = set(eg.keys()) | set(ag.keys())
        for g in sorted(groups):
            e = (eg.get(g) or {}).get("hands", 0)
            a = ag.get(g, 0)
            if e != a:
                diffs.append({"month": m, "group": g, "expected": e, "actual": a})

    report = {
        "counts_path": counts_path,
        "hands_jsonl": hands_jsonl,
        "differences": diffs,
        "ok": (len(diffs) == 0)
    }

    # gravar ao lado do counts
    out_path = os.path.join(os.path.dirname(counts_path), "validation_report.json")
    with open(out_path, "w", encoding="utf-8") as fo:
        json.dump(report, fo, ensure_ascii=False, indent=2)
    report["report_path"] = out_path
    return report


def validate_with_summary(counts_path: str, hands_jsonl: str) -> Dict:
    """
    Extended validation with summary statistics.
    
    Args:
        counts_path: Path to partition_counts.json
        hands_jsonl: Path to hands JSONL file
        
    Returns:
        Extended validation report with summary
    """
    base_report = validate_partitions(counts_path, hands_jsonl)
    
    if base_report.get("error"):
        return base_report
    
    # Add summary statistics
    with open(counts_path, "r", encoding="utf-8") as f:
        counts_data = json.load(f)
    
    totals = counts_data.get("totals", {})
    counts = counts_data.get("counts", {})
    
    summary = {
        "total_months": len(counts),
        "total_hands_in_counts": sum(totals.values()),
        "groups_with_data": [g for g, v in totals.items() if v > 0],
        "validation_status": "PASSED" if base_report["ok"] else "FAILED",
        "discrepancy_count": len(base_report.get("differences", []))
    }
    
    base_report["summary"] = summary
    
    # Update the saved report
    if "report_path" in base_report:
        with open(base_report["report_path"], "w", encoding="utf-8") as f:
            json.dump(base_report, f, ensure_ascii=False, indent=2)
    
    return base_report


if __name__ == "__main__":
    # CLI validation
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python -m app.partition.validator <counts_path> <hands_jsonl>")
        sys.exit(1)
    
    counts_path = sys.argv[1]
    hands_jsonl = sys.argv[2]
    
    print(f"Validating partitions...")
    print(f"  Counts: {counts_path}")
    print(f"  Hands:  {hands_jsonl}")
    
    result = validate_with_summary(counts_path, hands_jsonl)
    
    if result.get("error"):
        print(f"✗ Error: {result['error']}")
        sys.exit(1)
    
    summary = result.get("summary", {})
    print(f"\nValidation {summary['validation_status']}:")
    print(f"  Total months: {summary['total_months']}")
    print(f"  Total hands: {summary['total_hands_in_counts']}")
    print(f"  Active groups: {', '.join(summary['groups_with_data'])}")
    
    if not result["ok"]:
        print(f"\n⚠ Found {summary['discrepancy_count']} discrepancies!")
        for diff in result["differences"][:5]:  # Show first 5
            print(f"  {diff['month']} / {diff['group']}: expected {diff['expected']}, got {diff['actual']}")
    else:
        print(f"✓ All counts match!")
    
    print(f"\nReport saved to: {result['report_path']}")