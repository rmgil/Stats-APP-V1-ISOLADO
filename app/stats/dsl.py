"""
DSL (Domain Specific Language) parser for stats definitions.
"""
import os
import yaml
from typing import Dict, List, Any, Optional
from pathlib import Path


def load_catalog(catalog_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load the stats catalog from YAML file.
    
    Args:
        catalog_path: Optional path to catalog file. 
                     Defaults to app/stats/dsl/stats.yml
                     
    Returns:
        Parsed catalog dictionary
    """
    if catalog_path is None:
        # Default path relative to this file
        current_dir = Path(__file__).parent
        catalog_path = current_dir / "dsl" / "stats.yml"
    else:
        catalog_path = Path(catalog_path)
    
    if not catalog_path.exists():
        raise FileNotFoundError(f"Stats catalog not found: {catalog_path}")
    
    with open(catalog_path, 'r', encoding='utf-8') as f:
        catalog = yaml.safe_load(f)
    
    return catalog


def parse_stat_definition(stat_def: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single stat definition from the catalog.
    
    Args:
        stat_def: Raw stat definition from YAML
        
    Returns:
        Parsed and validated stat definition
    """
    required_fields = ['id', 'label', 'opportunity', 'attempt']
    for field in required_fields:
        if field not in stat_def:
            raise ValueError(f"Stat definition missing required field: {field}")
    
    # Parse filters
    filters = stat_def.get('filters', {})
    parsed_filters = {
        'heads_up_only': filters.get('heads_up_only', False),
        'pot_type': filters.get('pot_type', []),
        'eff_stack_min_bb': filters.get('eff_stack_min_bb'),
        'eff_stack_max_bb': filters.get('eff_stack_max_bb'),
        'exclude_allin_preflop': filters.get('exclude_allin_preflop', False)
    }
    
    # Parse opportunity conditions
    opportunity = stat_def['opportunity']
    if isinstance(opportunity, dict):
        if 'all' in opportunity:
            opp_conditions = {'type': 'all', 'conditions': opportunity['all']}
        elif 'any' in opportunity:
            opp_conditions = {'type': 'any', 'conditions': opportunity['any']}
        else:
            opp_conditions = opportunity
    else:
        opp_conditions = opportunity
    
    # Parse attempt conditions
    attempt = stat_def['attempt']
    
    return {
        'id': stat_def['id'],
        'label': stat_def['label'],
        'family': stat_def.get('family'),
        'scope': stat_def.get('scope', 'preflop'),
        'applies_to_groups': stat_def.get('applies_to_groups', []),
        'filters': parsed_filters,
        'opportunity': opp_conditions,
        'attempt': attempt
    }


def get_stats_by_group(catalog: Dict[str, Any], group: str) -> List[Dict[str, Any]]:
    """
    Get all stats that apply to a specific group.
    
    Args:
        catalog: Loaded stats catalog
        group: Group name (e.g., 'nonko_9max_pref')
        
    Returns:
        List of stat definitions for this group
    """
    stats = []
    for stat_def in catalog.get('stats', []):
        if group in stat_def.get('applies_to_groups', []):
            stats.append(parse_stat_definition(stat_def))
    return stats


def get_stats_by_family(catalog: Dict[str, Any], family: str) -> List[Dict[str, Any]]:
    """
    Get all stats belonging to a specific family.
    
    Args:
        catalog: Loaded stats catalog
        family: Family name (e.g., 'RFI')
        
    Returns:
        List of stat definitions in this family
    """
    stats = []
    for stat_def in catalog.get('stats', []):
        if stat_def.get('family') == family:
            stats.append(parse_stat_definition(stat_def))
    return stats


def validate_catalog(catalog: Dict[str, Any]) -> bool:
    """
    Validate the stats catalog structure.
    
    Args:
        catalog: Loaded catalog to validate
        
    Returns:
        True if valid
        
    Raises:
        ValueError: If catalog is invalid
    """
    if 'version' not in catalog:
        raise ValueError("Catalog missing 'version' field")
    
    if 'stats' not in catalog:
        raise ValueError("Catalog missing 'stats' field")
    
    if not isinstance(catalog['stats'], list):
        raise ValueError("'stats' must be a list")
    
    # Validate each stat
    stat_ids = set()
    for stat_def in catalog['stats']:
        try:
            parsed = parse_stat_definition(stat_def)
            
            # Check for duplicate IDs
            if parsed['id'] in stat_ids:
                raise ValueError(f"Duplicate stat ID: {parsed['id']}")
            stat_ids.add(parsed['id'])
            
        except Exception as e:
            raise ValueError(f"Invalid stat definition: {e}")
    
    return True


if __name__ == "__main__":
    # Test loading and validation
    catalog = load_catalog()
    print(f"Loaded catalog version {catalog['version']}")
    
    if validate_catalog(catalog):
        print(f"✓ Catalog valid with {len(catalog['stats'])} stats")
    
    # Show RFI stats
    rfi_stats = get_stats_by_family(catalog, 'RFI')
    print(f"\nRFI family has {len(rfi_stats)} stats:")
    for stat in rfi_stats:
        print(f"  - {stat['id']}: {stat['label']}")