"""
Monthly Parity Validation Tests

Validates that monthly statistics aggregate correctly to match overall totals.
Critical checks:
1. Sum of monthly valid_hands == aggregate valid_hands
2. Sum of monthly opportunities per stat == aggregate opportunities
3. Monthly structure mirrors aggregate structure
"""

import json
import os
from typing import Dict, Any, List


class MonthlyParityValidator:
    """Validates that monthly data aggregates correctly to match overall totals."""
    
    def __init__(self, pipeline_result: Dict[str, Any]):
        self.pipeline_result = pipeline_result
        self.aggregate = pipeline_result.get('aggregate', {})
        self.months_data = pipeline_result.get('months', {})
        self.errors = []
        self.warnings = []
        
    def validate_all(self) -> bool:
        """Run all parity checks. Returns True if all pass."""
        print("=" * 80)
        print("MONTHLY PARITY VALIDATION")
        print("=" * 80)
        
        # Check if we have monthly data
        if not self.months_data:
            self.errors.append("No monthly data found in pipeline_result")
            return False
        
        print(f"📊 Found {len(self.months_data)} months to validate")
        print(f"Months: {', '.join(sorted(self.months_data.keys()))}")
        print()
        
        # Run all validation checks
        self._validate_overall_valid_hands()
        self._validate_group_valid_hands()
        self._validate_preflop_stats()
        self._validate_postflop_stats()
        self._validate_discard_stats()
        
        # Print summary
        self._print_summary()
        
        return len(self.errors) == 0
    
    def _validate_overall_valid_hands(self):
        """Validate that sum of monthly valid_hands matches aggregate."""
        print("🔍 Validating Overall Valid Hands...")
        
        aggregate_valid = self.aggregate.get('overall', {}).get('valid_hands', 0)
        aggregate_total = self.aggregate.get('overall', {}).get('total_hands', 0)
        
        monthly_valid_sum = 0
        monthly_total_sum = 0
        
        for month, month_data in self.months_data.items():
            monthly_valid_sum += month_data.get('overall', {}).get('valid_hands', 0)
            monthly_total_sum += month_data.get('overall', {}).get('total_hands', 0)
        
        # Check valid_hands parity
        if monthly_valid_sum != aggregate_valid:
            self.errors.append(
                f"Valid hands mismatch: sum of monthly ({monthly_valid_sum}) != "
                f"aggregate ({aggregate_valid}). Difference: {monthly_valid_sum - aggregate_valid}"
            )
        else:
            print(f"  ✅ Valid hands match: {aggregate_valid}")
        
        # Check total_hands parity
        if monthly_total_sum != aggregate_total:
            self.errors.append(
                f"Total hands mismatch: sum of monthly ({monthly_total_sum}) != "
                f"aggregate ({aggregate_total}). Difference: {monthly_total_sum - aggregate_total}"
            )
        else:
            print(f"  ✅ Total hands match: {aggregate_total}")
        
        print()
    
    def _validate_group_valid_hands(self):
        """Validate valid_hands per group (nonko_9max, nonko_6max, pko)."""
        print("🔍 Validating Group Valid Hands...")
        
        aggregate_groups = self.aggregate.get('groups', {})
        
        for group_name, aggregate_group in aggregate_groups.items():
            aggregate_valid = aggregate_group.get('valid_hands', 0)
            
            monthly_sum = 0
            for month_data in self.months_data.values():
                monthly_group = month_data.get('groups', {}).get(group_name, {})
                monthly_sum += monthly_group.get('valid_hands', 0)
            
            if monthly_sum != aggregate_valid:
                self.errors.append(
                    f"Group '{group_name}' valid_hands mismatch: "
                    f"sum of monthly ({monthly_sum}) != aggregate ({aggregate_valid})"
                )
            else:
                print(f"  ✅ {group_name}: {aggregate_valid} hands")
        
        print()
    
    def _validate_preflop_stats(self):
        """Validate preflop stat opportunities sum correctly."""
        print("🔍 Validating Preflop Stats...")
        
        aggregate_groups = self.aggregate.get('groups', {})
        errors_found = 0
        stats_checked = 0
        
        for group_name, aggregate_group in aggregate_groups.items():
            preflop = aggregate_group.get('preflop', {})
            
            for stat_name, aggregate_stat in preflop.items():
                if not isinstance(aggregate_stat, dict):
                    continue
                
                stats_checked += 1
                aggregate_opps = aggregate_stat.get('opportunities', 0)
                
                monthly_sum = 0
                for month_data in self.months_data.values():
                    monthly_stat = month_data.get('groups', {}).get(group_name, {}).get('preflop', {}).get(stat_name, {})
                    monthly_sum += monthly_stat.get('opportunities', 0)
                
                if monthly_sum != aggregate_opps:
                    self.errors.append(
                        f"Preflop '{group_name}/{stat_name}' opportunities mismatch: "
                        f"sum of monthly ({monthly_sum}) != aggregate ({aggregate_opps})"
                    )
                    errors_found += 1
        
        if errors_found == 0:
            print(f"  ✅ All {stats_checked} preflop stats match")
        else:
            print(f"  ❌ {errors_found} preflop stats have mismatches")
        
        print()
    
    def _validate_postflop_stats(self):
        """Validate postflop stat opportunities sum correctly."""
        print("🔍 Validating Postflop Stats...")
        
        aggregate_groups = self.aggregate.get('groups', {})
        errors_found = 0
        stats_checked = 0
        
        for group_name, aggregate_group in aggregate_groups.items():
            postflop = aggregate_group.get('postflop', {})
            
            for stat_name, aggregate_stat in postflop.items():
                if not isinstance(aggregate_stat, dict):
                    continue
                
                stats_checked += 1
                aggregate_opps = aggregate_stat.get('opportunities', 0)
                
                monthly_sum = 0
                for month_data in self.months_data.values():
                    monthly_stat = month_data.get('groups', {}).get(group_name, {}).get('postflop', {}).get(stat_name, {})
                    monthly_sum += monthly_stat.get('opportunities', 0)
                
                if monthly_sum != aggregate_opps:
                    self.errors.append(
                        f"Postflop '{group_name}/{stat_name}' opportunities mismatch: "
                        f"sum of monthly ({monthly_sum}) != aggregate ({aggregate_opps})"
                    )
                    errors_found += 1
        
        if errors_found == 0:
            print(f"  ✅ All {stats_checked} postflop stats match")
        else:
            print(f"  ❌ {errors_found} postflop stats have mismatches")
        
        print()
    
    def _validate_discard_stats(self):
        """Validate discard stats sum correctly."""
        print("🔍 Validating Discard Stats...")
        
        aggregate_discards = self.aggregate.get('discard_stats', {})
        
        # Validate each discard category
        for discard_type in ['mystery_hands', 'fewer_than_4_players']:
            aggregate_count = aggregate_discards.get(discard_type, 0)
            
            monthly_sum = 0
            for month_data in self.months_data.values():
                monthly_sum += month_data.get('discard_stats', {}).get(discard_type, 0)
            
            if monthly_sum != aggregate_count:
                self.errors.append(
                    f"Discard '{discard_type}' mismatch: "
                    f"sum of monthly ({monthly_sum}) != aggregate ({aggregate_count})"
                )
            else:
                print(f"  ✅ {discard_type}: {aggregate_count}")
        
        print()
    
    def _print_summary(self):
        """Print validation summary."""
        print("=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        
        if self.warnings:
            print(f"⚠️  {len(self.warnings)} WARNINGS:")
            for warning in self.warnings:
                print(f"  • {warning}")
            print()
        
        if self.errors:
            print(f"❌ {len(self.errors)} ERRORS FOUND:")
            for error in self.errors:
                print(f"  • {error}")
            print()
            print("VALIDATION FAILED ❌")
        else:
            print("✅ ALL PARITY CHECKS PASSED")
            print()
            print("Monthly data correctly aggregates to match overall totals.")
        
        print("=" * 80)


def load_pipeline_result(token: str) -> Dict[str, Any]:
    """Load pipeline_result.json from work directory or storage."""
    # Try local first
    local_path = os.path.join("work", token, "pipeline_result.json")
    
    if os.path.exists(local_path):
        with open(local_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    # Try storage
    storage_base = "/tmp/storage" if not os.environ.get('REPL_DEPLOYMENT') else ""
    storage_path = os.path.join(storage_base, "results", token, "pipeline_result.json")
    
    if os.path.exists(storage_path):
        with open(storage_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    raise FileNotFoundError(f"pipeline_result.json not found for token: {token}")


def validate_token(token: str) -> bool:
    """Run parity validation for a specific job token."""
    try:
        print(f"Loading pipeline result for token: {token}")
        pipeline_result = load_pipeline_result(token)
        
        validator = MonthlyParityValidator(pipeline_result)
        return validator.validate_all()
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_all_recent_jobs(limit: int = 5) -> Dict[str, bool]:
    """Validate parity for the N most recent completed jobs."""
    import psycopg2
    from urllib.parse import urlparse
    
    # Connect to database
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not set")
        return {}
    
    # Parse DATABASE_URL
    result = urlparse(database_url)
    conn = psycopg2.connect(
        host=result.hostname,
        port=result.port,
        user=result.username,
        password=result.password,
        database=result.path[1:]
    )
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT token, created_at 
        FROM processing_jobs 
        WHERE status = 'completed' 
        ORDER BY created_at DESC 
        LIMIT %s
    """, (limit,))
    
    results = {}
    for token, created_at in cursor.fetchall():
        print(f"\n{'=' * 80}")
        print(f"Testing token: {token} (created: {created_at})")
        print(f"{'=' * 80}\n")
        
        results[token] = validate_token(token)
    
    cursor.close()
    conn.close()
    
    return results


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Validate specific token
        token = sys.argv[1]
        success = validate_token(token)
        sys.exit(0 if success else 1)
    else:
        # Validate recent jobs
        print("Validating all recent completed jobs...")
        results = validate_all_recent_jobs(limit=3)
        
        print("\n" + "=" * 80)
        print("OVERALL RESULTS")
        print("=" * 80)
        
        for token, passed in results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{token}: {status}")
        
        all_passed = all(results.values())
        sys.exit(0 if all_passed else 1)
