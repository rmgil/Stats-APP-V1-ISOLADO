"""
Effective stack calculations (Phase 3)
"""
from typing import Optional, List, Dict
import logging
from app.parse.schemas import Hand

logger = logging.getLogger(__name__)

# Position orders for calculating who acts after
POS_ORDER_6MAX = ["EP", "MP", "CO", "BTN", "SB", "BB"]
POS_ORDER_9MAX = ["EP", "EP2", "MP1", "MP2", "MP3", "CO", "BTN", "SB", "BB"]

# Minimum stack size in BB for valid opportunities
MIN_STACK_BB = 16.0


def _bb(hand: Hand) -> float:
    """Get big blind value from hand."""
    return float(hand.blinds.get("bb") or 0.0)


def _stack_of(hand: Hand, name: str) -> Optional[float]:
    """Get stack size of a specific player."""
    for p in hand.players:
        if p.name == name:
            return p.stack_chips
    return None


def eff_stack_bb_srp(hand: Hand, hero: str) -> Optional[float]:
    """
    Calculate effective stack in BBs for single raised pot (SRP).
    
    Only calculates when heads-up to flop.
    
    Args:
        hand: Hand object
        hero: Hero's name
        
    Returns:
        Effective stack in BBs if HU to flop, None otherwise
    """
    if not hero:
        return None
        
    # Check if heads-up to flop
    preflop = hand.streets.get("preflop")
    if not preflop or not preflop.actions:
        return None
        
    folded = {a.actor for a in preflop.actions if a.type == "FOLD"}
    active = [p.name for p in hand.players if p.name not in folded]
    
    # Only calculate for heads-up
    if len(active) != 2 or hero not in active:
        return None
        
    # Get villain name
    vil = active[0] if active[1] == hero else active[1]
    
    # Get stacks and BB
    sh = _stack_of(hand, hero)
    sv = _stack_of(hand, vil)
    bb = _bb(hand)
    
    if sh is None or sv is None or bb <= 0:
        return None
        
    # Effective stack is the smaller of the two stacks
    eff_stack = min(sh, sv) / bb
    
    logger.debug(f"Effective stack SRP: {eff_stack:.2f} BB (Hero: {sh/bb:.2f}, Villain: {sv/bb:.2f})")
    return round(eff_stack, 2)


def eff_stack_bb_vs_3bettor(hand: Hand, hero: str, three_bettor: Optional[str]) -> Optional[float]:
    """
    Calculate effective stack in BBs vs 3-bettor.
    
    Args:
        hand: Hand object
        hero: Hero's name
        three_bettor: Name of player who 3-bet
        
    Returns:
        Effective stack in BBs vs 3-bettor, None if not applicable
    """
    if not hero or not three_bettor:
        return None
        
    # Get stacks and BB
    sh = _stack_of(hand, hero)
    sv = _stack_of(hand, three_bettor)
    bb = _bb(hand)
    
    if sh is None or sv is None or bb <= 0:
        return None
        
    # Effective stack is the smaller of the two stacks
    eff_stack = min(sh, sv) / bb
    
    logger.debug(f"Effective stack vs 3-bettor: {eff_stack:.2f} BB (Hero: {sh/bb:.2f}, 3-bettor: {sv/bb:.2f})")
    return round(eff_stack, 2)


def get_positions_acting_after(hero_position: str, n_players: int) -> List[str]:
    """
    Get list of positions that act after the hero's position.
    
    Args:
        hero_position: Hero's absolute position (EP, MP, CO, BTN, SB, BB, etc.)
        n_players: Number of active players at the table
        
    Returns:
        List of positions that act after hero (in action order)
    """
    # Choose the appropriate position order based on table size
    if n_players <= 6:
        pos_order = POS_ORDER_6MAX
    else:
        pos_order = POS_ORDER_9MAX
    
    # Find hero's index
    try:
        hero_idx = pos_order.index(hero_position)
    except ValueError:
        logger.warning(f"Position {hero_position} not found in order for {n_players} players")
        return []
    
    # Return positions after hero in action order
    return pos_order[hero_idx + 1:]


def get_player_stack_bb(hand: Hand, player_name: str) -> Optional[float]:
    """
    Get a player's stack in big blinds.
    
    Args:
        hand: Hand object
        player_name: Player's name
        
    Returns:
        Stack in BB, or None if not found
    """
    stack_chips = _stack_of(hand, player_name)
    bb = _bb(hand)
    
    if stack_chips is None or bb <= 0:
        return None
        
    return round(stack_chips / bb, 2)


def validate_stack_for_opportunity(
    hand: Hand,
    hero_name: str,
    opportunity_type: str,
    context: Optional[Dict] = None
) -> bool:
    """
    Validate if an opportunity should count based on stack sizes.
    
    Rules:
    1. Hero's stack must always be >= 16bb
    2. Effective stack calculation depends on opportunity type:
       - 'rfi' / 'steal': Average of stacks acting after hero >= 16bb
       - 'response' (3bet/cc/defend): Raiser's stack >= 16bb  
       - 'fold': Average of stacks acting after hero >= 16bb
       - 'squeeze': Average of (raiser + caller) >= 16bb
       - 'fold_to_3bet': 3-bettor's stack >= 16bb (hero already validated in RFI)
       - 'bvb': Villain's stack >= 16bb
    
    Args:
        hand: Hand object
        hero_name: Hero's player name
        opportunity_type: Type of opportunity ('rfi', 'response', 'fold', 'squeeze', 'fold_to_3bet', 'bvb')
        context: Additional context dict with keys like:
            - 'hero_position': Hero's absolute position
            - 'raiser_name': Name of the raiser (for 'response')
            - 'three_bettor_name': Name of 3-bettor (for 'fold_to_3bet')
            - 'villain_name': Name of villain (for 'bvb')
            - 'raiser_name' + 'caller_name': For squeeze
            - 'positions_active_after': List of positions still to act (for rfi/fold)
            - 'n_players': Number of players at start of hand
            
    Returns:
        True if opportunity is valid (stacks meet requirements), False otherwise
    """
    if context is None:
        context = {}
    
    # First filter: Hero's stack must be >= 16bb
    hero_stack_bb = get_player_stack_bb(hand, hero_name)
    if hero_stack_bb is None or hero_stack_bb < MIN_STACK_BB:
        logger.debug(f"[STACK FILTER] Hero {hero_name} stack {hero_stack_bb}bb < {MIN_STACK_BB}bb - EXCLUDED")
        return False
    
    # Second filter: Effective stack based on opportunity type
    if opportunity_type in ['rfi', 'steal', 'fold']:
        # Need average of stacks acting after hero
        positions_after = context.get('positions_active_after', [])
        
        if not positions_after:
            # No one acts after (e.g., BB), always valid if hero stack is OK
            return True
        
        # Get stacks of players in those positions
        # We need to map positions to player names
        abs_positions = context.get('abs_positions', {})  # {player_name: position}
        position_to_player = {pos: name for name, pos in abs_positions.items()}
        
        stacks_after = []
        for pos in positions_after:
            player = position_to_player.get(pos)
            if player:
                stack_bb = get_player_stack_bb(hand, player)
                if stack_bb is not None:
                    stacks_after.append(stack_bb)
        
        if not stacks_after:
            # No players found in those positions
            return True
            
        avg_stack = sum(stacks_after) / len(stacks_after)
        if avg_stack < MIN_STACK_BB:
            logger.debug(f"[STACK FILTER] Average stack after hero {avg_stack:.1f}bb < {MIN_STACK_BB}bb - EXCLUDED")
            return False
    
    elif opportunity_type == 'response':
        # Need raiser's stack >= 16bb
        raiser_name = context.get('raiser_name')
        if not raiser_name:
            logger.warning(f"[STACK FILTER] 'response' type but no raiser_name in context")
            return False
            
        raiser_stack_bb = get_player_stack_bb(hand, raiser_name)
        if raiser_stack_bb is None or raiser_stack_bb < MIN_STACK_BB:
            logger.debug(f"[STACK FILTER] Raiser {raiser_name} stack {raiser_stack_bb}bb < {MIN_STACK_BB}bb - EXCLUDED")
            return False
    
    elif opportunity_type == 'squeeze':
        # Need average of (raiser + caller) >= 16bb
        raiser_name = context.get('raiser_name')
        caller_name = context.get('caller_name')
        
        if not raiser_name or not caller_name:
            logger.warning(f"[STACK FILTER] 'squeeze' type but missing raiser or caller in context")
            return False
        
        raiser_stack = get_player_stack_bb(hand, raiser_name)
        caller_stack = get_player_stack_bb(hand, caller_name)
        
        if raiser_stack is None or caller_stack is None:
            return False
            
        avg_stack = (raiser_stack + caller_stack) / 2
        if avg_stack < MIN_STACK_BB:
            logger.debug(f"[STACK FILTER] Squeeze avg(raiser+caller) {avg_stack:.1f}bb < {MIN_STACK_BB}bb - EXCLUDED")
            return False
    
    elif opportunity_type == 'fold_to_3bet':
        # Need 3-bettor's stack >= 16bb
        three_bettor_name = context.get('three_bettor_name')
        if not three_bettor_name:
            logger.warning(f"[STACK FILTER] 'fold_to_3bet' type but no three_bettor_name in context")
            return False
            
        three_bettor_stack = get_player_stack_bb(hand, three_bettor_name)
        if three_bettor_stack is None or three_bettor_stack < MIN_STACK_BB:
            logger.debug(f"[STACK FILTER] 3-bettor {three_bettor_name} stack {three_bettor_stack}bb < {MIN_STACK_BB}bb - EXCLUDED")
            return False
    
    elif opportunity_type == 'bvb':
        # Need villain's stack >= 16bb
        villain_name = context.get('villain_name')
        if not villain_name:
            logger.warning(f"[STACK FILTER] 'bvb' type but no villain_name in context")
            return False
            
        villain_stack = get_player_stack_bb(hand, villain_name)
        if villain_stack is None or villain_stack < MIN_STACK_BB:
            logger.debug(f"[STACK FILTER] BvB villain {villain_name} stack {villain_stack}bb < {MIN_STACK_BB}bb - EXCLUDED")
            return False
    
    # All checks passed
    return True


def compute_hero_stack_bb(hand: Hand) -> Optional[float]:
    """
    Calculate hero's stack in BB.
    
    Args:
        hand: Hand object
        
    Returns:
        Hero's stack in BB, or None if hero not found or BB = 0
    """
    hero = hand.hero
    if not hero:
        return None
    
    stack_chips = _stack_of(hand, hero)
    bb = _bb(hand)
    
    if stack_chips is None or bb <= 0:
        return None
    
    return round(stack_chips / bb, 2)


def compute_avg_stacks_after_hero_bb(hand: Hand) -> Optional[float]:
    """
    Calculate average stack (in BB) of players acting AFTER hero.
    
    Uses position orders from POS_ORDER_6MAX / POS_ORDER_9MAX.
    
    Args:
        hand: Hand object
        
    Returns:
        Average stack in BB of players after hero, or None if no players after
    """
    hero = hand.hero
    if not hero:
        return None
    
    bb = _bb(hand)
    if bb <= 0:
        return None
    
    # Get hero's position from derived data
    if not hasattr(hand, 'derived') or not hand.derived:
        return None
    
    derived = hand.derived
    if not hasattr(derived, 'positions') or not derived.positions:
        return None
    
    abs_positions = derived.positions.abs_positions
    if not abs_positions or hero not in abs_positions:
        return None
    
    hero_position = abs_positions[hero]
    n_players = len(hand.players)
    
    # Get positions acting after hero
    positions_after = get_positions_acting_after(hero_position, n_players)
    
    if not positions_after:
        return None
    
    # Map positions to player names
    position_to_player = {pos: name for name, pos in abs_positions.items()}
    
    # Collect stacks of players in those positions
    stacks_bb = []
    for pos in positions_after:
        player = position_to_player.get(pos)
        if player:
            stack_chips = _stack_of(hand, player)
            if stack_chips is not None:
                stacks_bb.append(stack_chips / bb)
    
    if not stacks_bb:
        return None
    
    return round(sum(stacks_bb) / len(stacks_bb), 2)


def compute_raiser_stack_bb(hand: Hand) -> Optional[float]:
    """
    Find open_raiser from derived.preflop and return their stack in BB.
    
    Args:
        hand: Hand object
        
    Returns:
        Raiser's stack in BB, or None if not found
    """
    if not hasattr(hand, 'derived') or not hand.derived:
        return None
    
    derived = hand.derived
    if not hasattr(derived, 'preflop') or not derived.preflop:
        return None
    
    open_raiser = derived.preflop.open_raiser
    if not open_raiser:
        return None
    
    stack_chips = _stack_of(hand, open_raiser)
    bb = _bb(hand)
    
    if stack_chips is None or bb <= 0:
        return None
    
    return round(stack_chips / bb, 2)


def compute_three_bettor_stack_bb(hand: Hand) -> Optional[float]:
    """
    Find three_bettor from derived.preflop and return their stack in BB.
    
    Args:
        hand: Hand object
        
    Returns:
        Three-bettor's stack in BB, or None if not found
    """
    if not hasattr(hand, 'derived') or not hand.derived:
        return None
    
    derived = hand.derived
    if not hasattr(derived, 'preflop') or not derived.preflop:
        return None
    
    three_bettor = derived.preflop.three_bettor
    if not three_bettor:
        return None
    
    stack_chips = _stack_of(hand, three_bettor)
    bb = _bb(hand)
    
    if stack_chips is None or bb <= 0:
        return None
    
    return round(stack_chips / bb, 2)


def compute_squeeze_avg_stack_bb(hand: Hand) -> Optional[float]:
    """
    Find open_raiser + first caller before hero from preflop actions.
    Return average of their stacks in BB.
    
    Args:
        hand: Hand object
        
    Returns:
        Average stack in BB of (raiser + first caller), or None if not found
    """
    hero = hand.hero
    if not hero:
        return None
    
    # Get open_raiser from derived data
    if not hasattr(hand, 'derived') or not hand.derived:
        return None
    
    derived = hand.derived
    if not hasattr(derived, 'preflop') or not derived.preflop:
        return None
    
    open_raiser = derived.preflop.open_raiser
    if not open_raiser:
        return None
    
    # Find first caller before hero from preflop actions
    preflop = hand.streets.get("preflop")
    if not preflop or not preflop.actions:
        return None
    
    first_caller = None
    hero_acted = False
    
    for action in preflop.actions:
        if action.actor == hero:
            hero_acted = True
            break
        if action.type == "CALL" and not first_caller:
            first_caller = action.actor
    
    if not first_caller:
        return None
    
    # Get stacks
    raiser_stack = _stack_of(hand, open_raiser)
    caller_stack = _stack_of(hand, first_caller)
    bb = _bb(hand)
    
    if raiser_stack is None or caller_stack is None or bb <= 0:
        return None
    
    avg_stack = (raiser_stack + caller_stack) / 2 / bb
    return round(avg_stack, 2)


def compute_bvb_villain_stack_bb(hand: Hand) -> Optional[float]:
    """
    BvB villain stack calculation.
    
    If hero is SB, return BB's stack.
    If hero is BB, return SB's stack.
    Otherwise None.
    
    Args:
        hand: Hand object
        
    Returns:
        Villain's stack in BB for BvB situations, or None otherwise
    """
    hero = hand.hero
    if not hero:
        return None
    
    # Get hero's position from derived data
    if not hasattr(hand, 'derived') or not hand.derived:
        return None
    
    derived = hand.derived
    if not hasattr(derived, 'positions') or not derived.positions:
        return None
    
    abs_positions = derived.positions.abs_positions
    if not abs_positions or hero not in abs_positions:
        return None
    
    hero_position = abs_positions[hero]
    
    # Check if hero is SB or BB
    if hero_position not in ["SB", "BB"]:
        return None
    
    # Find the villain (opposite blind)
    villain_position = "BB" if hero_position == "SB" else "SB"
    
    # Map positions to player names
    position_to_player = {pos: name for name, pos in abs_positions.items()}
    villain = position_to_player.get(villain_position)
    
    if not villain:
        return None
    
    # Get villain's stack
    villain_stack = _stack_of(hand, villain)
    bb = _bb(hand)
    
    if villain_stack is None or bb <= 0:
        return None
    
    return round(villain_stack / bb, 2)


def derive_stacks(hand: Hand) -> dict:
    """
    Derive all stack-related statistics.
    
    Returns:
        Dict with all stack-related fields
    """
    try:
        hero = hand.hero
        
        # Get 3-bettor if exists (would come from preflop analysis)
        from app.derive.preflop import who_3bet_4bet
        preflop = hand.streets.get("preflop")
        acts = preflop.actions if preflop else []
        three_bettor, _ = who_3bet_4bet(acts)
        
        return {
            "eff_stack_bb_srp": eff_stack_bb_srp(hand, hero) if hero else None,
            "eff_stack_bb_vs_3bettor": eff_stack_bb_vs_3bettor(hand, hero, three_bettor) if hero else None,
            "hero_stack_bb": compute_hero_stack_bb(hand),
            "avg_stacks_after_hero_bb": compute_avg_stacks_after_hero_bb(hand),
            "raiser_stack_bb": compute_raiser_stack_bb(hand),
            "three_bettor_stack_bb": compute_three_bettor_stack_bb(hand),
            "squeeze_avg_stack_bb": compute_squeeze_avg_stack_bb(hand),
            "bvb_villain_stack_bb": compute_bvb_villain_stack_bb(hand)
        }
        
    except Exception as e:
        logger.error(f"Error deriving stacks: {e}")
        return {
            "eff_stack_bb_srp": None,
            "eff_stack_bb_vs_3bettor": None,
            "hero_stack_bb": None,
            "avg_stacks_after_hero_bb": None,
            "raiser_stack_bb": None,
            "three_bettor_stack_bb": None,
            "squeeze_avg_stack_bb": None,
            "bvb_villain_stack_bb": None
        }