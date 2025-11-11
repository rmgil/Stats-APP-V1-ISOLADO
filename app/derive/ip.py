"""
IP/OOP and player count analysis per street (Phase 3)
"""
from typing import List, Optional
import logging
from app.parse.schemas import Hand, Action

logger = logging.getLogger(__name__)


def players_remaining_after_preflop(hand: Hand) -> List[str]:
    """
    Get list of players who didn't fold preflop.
    
    Args:
        hand: Hand object
        
    Returns:
        List of player names still active after preflop
    """
    preflop = hand.streets.get("preflop")
    if not preflop or not preflop.actions:
        # No preflop actions, use players_dealt_in as they all remain
        return hand.players_dealt_in[:]
    
    folded = {a.actor for a in preflop.actions if a.type == "FOLD"}
    
    # Use players_dealt_in instead of hand.players (which may be empty/malformed)
    remaining = [p for p in hand.players_dealt_in if p not in folded]
    
    logger.debug(f"Players to flop: {len(remaining)} of {len(hand.players_dealt_in)}")
    return remaining


def players_remaining_after_street(hand: Hand, street: str, prev_players: List[str]) -> List[str]:
    """
    Get list of players remaining after a specific street.
    
    Args:
        hand: Hand object
        street: Street name ("flop", "turn", "river")
        prev_players: Players who were active before this street
        
    Returns:
        List of player names still active after the street
    """
    street_info = hand.streets.get(street)
    if not street_info or not street_info.actions:
        # No actions on this street, all previous players remain
        return prev_players[:]
    
    folded = {a.actor for a in street_info.actions if a.type == "FOLD"}
    remaining = [n for n in prev_players if n not in folded]
    
    logger.debug(f"Players after {street}: {len(remaining)} of {len(prev_players)}")
    return remaining


def determine_ip(hand: Hand, street: str, two_players: List[str]) -> Optional[bool]:
    """
    Determine if hero is in position (IP) when heads-up.
    
    In position means acting last. The first player to act is out of position (OOP).
    
    Args:
        hand: Hand object
        street: Street to check ("flop", "turn", "river")
        two_players: List of exactly 2 players
        
    Returns:
        True if hero is IP, False if OOP, None if not heads-up or hero not involved
    """
    if len(two_players) != 2:
        return None
        
    hero = hand.hero
    if not hero or hero not in two_players:
        return None
        
    villain = two_players[0] if two_players[1] == hero else two_players[1]
    
    street_info = hand.streets.get(street)
    if not street_info or not street_info.actions:
        return None
    
    # Find who acts first
    first_actor = None
    for a in street_info.actions:
        if a.actor in (hero, villain):
            first_actor = a.actor
            break
    
    if first_actor is None:
        return None
    
    # First to act is OOP, so hero is IP if hero is NOT first
    return first_actor != hero


def derive_ip(hand: Hand) -> dict:
    """
    Derive IP/OOP and player count statistics for all streets.
    
    Returns:
        Dict with heads_up flags, hero_ip status, and player counts per street
    """
    try:
        # Flop analysis
        flop_players = players_remaining_after_preflop(hand)
        heads_up_flop = len(flop_players) == 2
        hero_ip_flop = determine_ip(hand, "flop", flop_players) if heads_up_flop else None
        
        # Turn analysis
        turn_players = players_remaining_after_street(hand, "flop", flop_players)
        heads_up_turn = len(turn_players) == 2
        hero_ip_turn = determine_ip(hand, "turn", turn_players) if heads_up_turn else None
        
        # River analysis
        river_players = players_remaining_after_street(hand, "turn", turn_players)
        heads_up_river = len(river_players) == 2
        hero_ip_river = determine_ip(hand, "river", river_players) if heads_up_river else None
        
        return {
            "heads_up_flop": heads_up_flop,
            "heads_up_turn": heads_up_turn,
            "heads_up_river": heads_up_river,
            "hero_ip_flop": hero_ip_flop,
            "hero_ip_turn": hero_ip_turn,
            "hero_ip_river": hero_ip_river,
            "players_to_flop": len(flop_players),
            "players_to_turn": len(turn_players),
            "players_to_river": len(river_players)
        }
        
    except Exception as e:
        logger.error(f"Error deriving IP data: {e}")
        # Return safe defaults
        return {
            "heads_up_flop": False,
            "heads_up_turn": False,
            "heads_up_river": False,
            "hero_ip_flop": None,
            "hero_ip_turn": None,
            "hero_ip_river": None,
            "players_to_flop": 0,
            "players_to_turn": 0,
            "players_to_river": 0
        }