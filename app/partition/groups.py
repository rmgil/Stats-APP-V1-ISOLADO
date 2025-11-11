"""
Group-based partitioning for poker hands.
Splits hands by configurable grouping criteria.
"""
import json
import os
from collections import defaultdict
from typing import Dict, List, Any, Optional

# Group constants
NONKO_9MAX_PREF = "nonko_9max_pref"
NONKO_6MAX_PREF = "nonko_6max_pref"
PKO_PREF = "pko_pref"
MYSTERY_PREF = "mystery_pref"
POSTFLOP_ALL = "postflop_all"


def infer_tourney_class(hand: dict) -> str:
    """
    Detecta 'pko'|'mystery'|'non-ko' com múltiplos fallbacks.
    1) Campo explícito (se existir no parse); 2) Caminho (file_id);
    3) palavras-chave no file_id; default 'non-ko'.
    
    Args:
        hand: Hand dictionary
        
    Returns:
        'pko', 'mystery', or 'non-ko'
    """
    tc = (hand.get("tourney_class") or "").lower()
    if tc in ("pko", "mystery", "non-ko"):
        return tc
    
    fid = (hand.get("file_id") or "").lower()
    if "/pko/" in fid or "\\pko\\" in fid:
        return "pko"
    if "/myst" in fid or "\\myst" in fid:
        return "mystery"
    if "/non-ko/" in fid or "\\non-ko\\" in fid:
        return "non-ko"
    if any(k in fid for k in ["bounty", "knockout", " ko "]):  # espaço para evitar 'poker'
        return "pko"
    return "non-ko"


def is_table_6max(table_max_resolved: int) -> bool:
    """Check if table is 6-max format."""
    return 3 <= table_max_resolved <= 6


def is_table_9max(table_max_resolved: int) -> bool:
    """Check if table is 9-max format."""
    return 7 <= table_max_resolved <= 10


def saw_flop(hand: dict) -> bool:
    """Check if hand saw the flop (2+ players)."""
    ip = hand.get("derived", {}).get("ip", {})
    return (ip.get("players_to_flop", 0) >= 2)


def groups_for_hand(hand: dict) -> List[str]:
    """
    Determine which groups a hand belongs to.
    
    Args:
        hand: Hand dictionary with derived data
        
    Returns:
        List of group names the hand belongs to
    """
    groups = []
    derived = hand.get("derived", {})
    table_max = int(derived.get("positions", {}).get("table_max_resolved")
                    or hand.get("table_max") or 0)
    tclass = infer_tourney_class(hand)
    
    # --- grupos preflop (já existentes) ---
    if tclass == "non-ko" and is_table_9max(table_max):
        groups.append(NONKO_9MAX_PREF)
    if tclass == "non-ko" and is_table_6max(table_max):
        groups.append(NONKO_6MAX_PREF)
    if tclass == "pko":
        groups.append(PKO_PREF)
    if tclass == "mystery":
        groups.append(MYSTERY_PREF)
    
    # --- SEMPRE adicionar postflop_all se a mão tem flop/turn/river ---
    streets = hand.get("streets") or {}
    saw_post = any((streets.get(st) or {}).get("actions") for st in ("flop", "turn", "river"))
    if saw_post:
        groups.append(POSTFLOP_ALL)
    
    return groups


def get_group_key(hand: dict, group_by: str) -> str:
    """
    Extract grouping key from hand.
    
    Args:
        hand: Hand dictionary
        group_by: Grouping field (e.g., 'hero', 'site', 'tournament_id', 'pot_type')
        
    Returns:
        Group key as string
    """
    if group_by == 'hero':
        return hand.get('hero') or 'no_hero'
    
    elif group_by == 'site':
        return hand.get('site') or 'unknown_site'
    
    elif group_by == 'tournament_id':
        return str(hand.get('tournament_id') or 'cash_game')
    
    elif group_by == 'pot_type':
        # Use derived data if available
        derived = hand.get('derived', {})
        preflop = derived.get('preflop', {})
        return preflop.get('pot_type') or 'none'
    
    elif group_by == 'stake_level':
        # Extract stake from blinds
        blinds = hand.get('blinds', {})
        bb = blinds.get('bb', 0)
        if bb <= 0:
            return 'unknown'
        elif bb <= 0.10:
            return 'micro'
        elif bb <= 1.00:
            return 'low'
        elif bb <= 5.00:
            return 'mid'
        else:
            return 'high'
    
    elif group_by == 'table_size':
        # Group by table format
        table_max = hand.get('table_max')
        if table_max == 6:
            return '6max'
        elif table_max == 9:
            return '9max'
        elif table_max == 2:
            return 'headsup'
        else:
            return f'{table_max}max' if table_max else 'unknown'
    
    elif group_by == 'position':
        # Hero's position
        derived = hand.get('derived', {})
        preflop = derived.get('preflop', {})
        pos = preflop.get('hero_position')
        if not pos:
            return 'no_position'
        # Group positions into categories
        if pos in ['EP', 'EP2', 'EP3', 'UTG']:
            return 'early'
        elif pos in ['MP', 'MP1', 'MP2']:
            return 'middle'
        elif pos in ['CO', 'BTN']:
            return 'late'
        elif pos in ['SB', 'BB']:
            return 'blinds'
        else:
            return pos.lower()
    
    else:
        # Direct field access
        value = hand.get(group_by)
        return str(value) if value is not None else 'null'


def partition_by_group(hands_jsonl: str, output_dir: str, group_by: str = 'hero') -> Dict[str, str]:
    """
    Partition hands by specified grouping.
    
    Args:
        hands_jsonl: Path to input JSONL file
        output_dir: Directory to write partitioned files
        group_by: Field to group by
        
    Returns:
        Dict mapping group keys to output file paths
    """
    # Group hands
    groups_data = defaultdict(list)
    
    with open(hands_jsonl, 'r', encoding='utf-8') as f:
        for line in f:
            hand = json.loads(line.strip())
            group_key = get_group_key(hand, group_by)
            # Sanitize key for filename
            safe_key = group_key.replace('/', '_').replace('\\', '_').replace(' ', '_')
            groups_data[safe_key].append(hand)
    
    # Create output directory
    group_dir = os.path.join(output_dir, f"by_{group_by}")
    os.makedirs(group_dir, exist_ok=True)
    
    # Write partitioned files
    output_files = {}
    for group_key, hands in groups_data.items():
        output_file = os.path.join(group_dir, f"{group_key}.jsonl")
        with open(output_file, 'w', encoding='utf-8') as f:
            for hand in hands:
                f.write(json.dumps(hand, ensure_ascii=False) + '\n')
        output_files[group_key] = output_file
        print(f"  {group_key}: {len(hands)} hands → {output_file}")
    
    return output_files


def multi_partition(hands_jsonl: str, output_dir: str, group_fields: List[str]) -> dict:
    """
    Partition by multiple fields simultaneously.
    
    Args:
        hands_jsonl: Input JSONL file
        output_dir: Output directory
        group_fields: List of fields to partition by
        
    Returns:
        Summary of all partitions created
    """
    results = {}
    
    for field in group_fields:
        print(f"\nPartitioning by {field}...")
        output_files = partition_by_group(hands_jsonl, output_dir, field)
        results[field] = {
            'groups': len(output_files),
            'files': output_files
        }
    
    return results