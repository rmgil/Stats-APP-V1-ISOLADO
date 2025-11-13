"""
Position assignment logic for poker hands (Phase 3)
"""
from typing import Dict, List, Optional
import logging
from app.parse.schemas import Hand

logger = logging.getLogger(__name__)

# Position orders for different table sizes
POS_ORDER_6MAX = ["BTN", "SB", "BB", "EP", "MP", "CO"]
POS_ORDER_9MAX = ["BTN", "SB", "BB", "EP", "EP2", "MP1", "MP2", "MP3", "CO"]
KEEP_ALWAYS = {"CO", "BTN", "SB", "BB"}  # Positions to always preserve when short-handed

# Removal priority when short-handed (removed in this order)
# For 9-max: Remove MP3 first (8 players), then MP2 (7 players), etc.
REMOVAL_PRIORITY_9MAX = ["MP3", "MP2", "MP1", "EP2", "EP"]
REMOVAL_PRIORITY_6MAX = ["EP", "MP"]

# Position buckets for grouping (SB/BB are special positions, not in buckets)
BUCKETS_6MAX = {
    "EP": ["EP"], 
    "MP": ["MP"], 
    "LP": ["CO", "BTN"],
    "BLINDS": ["SB", "BB"]  # Add blinds as special category
}
BUCKETS_9MAX_FULL = {
    "EP": ["EP", "EP2"], 
    "MP": ["MP1", "MP2", "MP3"], 
    "LP": ["CO", "BTN"],
    "BLINDS": ["SB", "BB"]  # Add blinds as special category
}


def assign_positions(hand: Hand) -> Dict[str, str]:
    """
    Assign absolute positions to players based on button and table size.
    
    Handles short-handed by removing positions from left (EP first) 
    while preserving CO/BTN/SB/BB.
    
    Returns:
        Dict mapping player name to position (EP/MP/CO/BTN/SB/BB)
    """
    try:
        if not hand.button_seat or not hand.players:
            logger.warning(
                f"[assign_positions] button={hand.button_seat}, "
                f"players={len(hand.players) if hand.players else 0}"
            )
            return {}

        seats = [p.seat for p in hand.players]
        seat_to_name = {p.seat: p.name for p in hand.players}

        # Order seats from button
        order = _order_from_button(seats, hand.button_seat)
        n = len(order)
        
        # Determine table configuration based on active players
        # 6 or fewer players are always treated as 6-max
        if n <= 6:
            base = POS_ORDER_6MAX.copy()
            need = n
            assigned = _shrink_with_priority(base, need, KEEP_ALWAYS, REMOVAL_PRIORITY_6MAX)
        else:
            # 7+ players use 9-max configuration
            base = POS_ORDER_9MAX.copy()
            need = min(n, 9)
            assigned = _shrink_with_priority(base, need, KEEP_ALWAYS, REMOVAL_PRIORITY_9MAX)

        # Map seat -> position -> player name
        abs_positions: Dict[str, str] = {}
        for seat, pos in zip(order, assigned):
            name = seat_to_name.get(seat)
            if name:
                abs_positions[name] = pos
                
        logger.debug(f"[assign_positions] Assigned {len(abs_positions)} positions for {n} players")
        return abs_positions
        
    except Exception as e:
        logger.error(f"Error assigning positions for hand {getattr(hand, 'tournament_id', 'unknown')}: {e}")
        return {}


def group_buckets(abs_positions: Dict[str, str], n_active_players: int) -> Dict[str, str]:
    """
    Group absolute positions into buckets (EP/MP/LP).
    
    Args:
        abs_positions: Dict of player -> absolute position
        n_active_players: Number of active players in the hand
        
    Returns:
        Dict of player -> bucket (EP/MP/LP)
    """
    # Use 6-max buckets for 6 or fewer players
    if n_active_players <= 6:
        buckets = BUCKETS_6MAX
    else:
        buckets = BUCKETS_9MAX_FULL
        
    pos_group: Dict[str, str] = {}
    for name, pos in abs_positions.items():
        for g, lst in buckets.items():
            if pos in lst:
                pos_group[name] = g
                break
                
    return pos_group


def get_position_bucket(position: str, table_size: int = 6) -> Optional[str]:
    """
    Get position bucket (EP/MP/LP) for a given position.
    
    Args:
        position: Specific position (e.g., "EP", "MP", "CO", "BTN")
        table_size: Number of players at table
        
    Returns:
        Position bucket: "EP", "MP", or "LP"
    """
    if table_size <= 6:
        buckets = BUCKETS_6MAX
    else:
        buckets = BUCKETS_9MAX_FULL
    
    for bucket, positions in buckets.items():
        if position in positions:
            return bucket
    
    return None


def _order_from_button(seats: List[int], btn: int) -> List[int]:
    """
    Order seats starting from button (BTN, SB, BB, UTG...).
    
    Args:
        seats: List of seat numbers
        btn: Button seat number
        
    Returns:
        List of seats ordered from button
    """
    if btn not in seats:
        btn = min(seats)  # Fallback to smallest seat
        
    # Rotate by button (BTN first, then SB, BB, UTG...)
    ordered = sorted(seats, key=lambda s: ((s - btn) % 100))
    return ordered


def _shrink_with_priority(base: List[str], need: int, keep: set, priority: List[str]) -> List[str]:
    """
    Shrink position list by removing positions according to priority order.
    
    Example for 9-max:
    - 9-handed: All positions (EP, EP2, MP1, MP2, MP3, CO, BTN, SB, BB)
    - 8-handed: Remove MP3 → (EP, EP2, MP1, MP2, CO, BTN, SB, BB)
    - 7-handed: Remove MP2 → (EP, EP2, MP1, CO, BTN, SB, BB)
    
    Args:
        base: Base position list
        need: Number of positions needed
        keep: Set of positions to always preserve
        priority: List of positions in removal priority order
        
    Returns:
        Shrinked position list
    """
    if len(base) <= need:
        return base[:]
    
    to_remove = len(base) - need
    positions_to_remove = set()
    
    # Build set of positions to remove based on priority
    removed = 0
    for pos in priority:
        if removed >= to_remove:
            break
        if pos in base and pos not in keep:
            positions_to_remove.add(pos)
            removed += 1
    
    # Return base list minus removed positions, preserving original order
    result = [pos for pos in base if pos not in positions_to_remove]
    return result