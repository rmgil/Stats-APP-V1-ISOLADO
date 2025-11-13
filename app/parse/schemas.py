"""
Pydantic schemas for poker hand history parsing.
Defines data structures for hands, players, actions, and streets.
"""

from typing import Literal, List, Optional, Dict
from pydantic import BaseModel

# Type definitions
ActionType = Literal[
    "POST_SB", "POST_BB", "POST_ANTE", 
    "FOLD", "CHECK", "CALL", 
    "BET", "RAISE", "RERAISE", "ALLIN"
]

Street = Literal["preflop", "flop", "turn", "river"]

Site = Literal["pokerstars", "gg", "wpn", "winamax", "888", "other"]


class Action(BaseModel):
    """Represents a single player action in a hand."""
    actor: str
    type: ActionType
    amount: Optional[float] = None       # Amount for bet/raise/call
    to_amount: Optional[float] = None    # Total amount after raise
    allin: bool = False
    raw_offset: Optional[int] = None     # Position in original text (for click-through)


class StreetInfo(BaseModel):
    """Information about a specific betting street."""
    actions: List[Action] = []
    board: Optional[List[str]] = None    # Cards shown (flop/turn/river)


class Player(BaseModel):
    """Represents a player at the table."""
    seat: int
    name: str
    stack_chips: Optional[float] = None
    is_hero: bool = False


class Hand(BaseModel):
    """Complete hand history data structure."""
    # Metadata
    site: Site
    tournament_id: Optional[str] = None
    tournament_name: Optional[str] = None
    file_id: str
    timestamp_utc: Optional[str] = None
    
    # Table info
    button_seat: Optional[int] = None
    table_max: Optional[int] = None      # 6-max, 9-max, etc.
    blinds: Dict[str, float] = {}        # {"sb": ..., "bb": ..., "ante": ...}
    
    # Players
    players: List[Player] = []           # Initialize as empty list
    players_dealt_in: List[str] = []     # Players who received cards
    hero: Optional[str] = None           # Hero's name if identified
    
    # Action
    streets: Dict[Street, StreetInfo]
    
    # Derived stats
    any_allin_preflop: bool = False
    players_to_flop: int = 0
    heads_up_flop: bool = False
    
    # Text offsets for UI click-through
    raw_offsets: Dict[str, int] = {}     # {"hand_start": i, "hand_end": j, "flop": k, ...}