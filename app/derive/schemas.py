"""
Schemas for derived data (Phase 3)
"""
from typing import Optional, Literal, Dict, List
from pydantic import BaseModel, Field

Position6 = Literal["EP", "MP", "CO", "BTN", "SB", "BB"]
Position9 = Literal["EP", "EP2", "MP1", "MP2", "MP3", "CO", "BTN", "SB", "BB"]
PosGroup = Literal["EP", "MP", "LP"]


class DerivedPositions(BaseModel):
    """Position assignments and groupings for players"""
    table_max_resolved: int
    abs_positions: Dict[str, str] = {}      # player -> (Position6|Position9)
    pos_group: Dict[str, PosGroup] = {}     # player -> EP/MP/LP
    button_seat: int


class DerivedPreflop(BaseModel):
    """Derived preflop data calculated from raw hand parsing"""
    unopened_pot: bool = False
    has_limper_before_hero: bool = False
    open_raiser: Optional[str] = None
    hero_raised_first_in: bool = False
    is_isoraiser: bool = False
    three_bettor: Optional[str] = None
    four_bettor: Optional[str] = None
    faced_3bet: bool = False
    folded_to_3bet: bool = False
    is_squeeze: bool = False
    is_resteal_vs_btn: bool = False
    pot_type: Literal["SRP", "3bet", "4bet", "none"] = "none"
    freeplay_bb: bool = False

    # já implementados na base (mantém):
    hero_vpip: bool = False              # hero fez CALL/RAISE/ALLIN preflop
    hero_position: Optional[str] = None  # posição absoluta do hero (ex.: CO/BTN/SB/BB/EP/MP...)
    pot_size_bb: Optional[float] = None  # tamanho do pote no INÍCIO do FLOP em BB


class DerivedIP(BaseModel):
    """In-position and player count statistics per street"""
    heads_up_flop: bool = False
    heads_up_turn: bool = False
    heads_up_river: bool = False
    hero_ip_flop: Optional[bool] = None
    hero_ip_turn: Optional[bool] = None
    hero_ip_river: Optional[bool] = None
    players_to_flop: int = 0
    players_to_turn: int = 0
    players_to_river: int = 0


class DerivedStacks(BaseModel):
    """Effective stack calculations"""
    eff_stack_bb_srp: Optional[float] = None
    eff_stack_bb_vs_3bettor: Optional[float] = None


class DerivedFlags(BaseModel):
    """Boolean flags for special situations"""
    any_allin_preflop: bool = False


class DerivedPostflop(BaseModel):
    """Derived postflop data"""
    pfr_player: Optional[str] = None

    # Cbet (opportunity/attempt) por street e posição relativa
    cbet_flop_opp_ip: bool = False
    cbet_flop_att_ip: bool = False
    cbet_flop_opp_oop: bool = False
    cbet_flop_att_oop: bool = False

    cbet_turn_opp_ip: bool = False
    cbet_turn_att_ip: bool = False
    cbet_turn_opp_oop: bool = False
    cbet_turn_att_oop: bool = False

    # Donk
    donk_flop: bool = False
    donk_turn: bool = False

    # vs Cbet (fold/raise) IP/OOP
    vs_cbet_flop_fold_ip: bool = False
    vs_cbet_flop_raise_ip: bool = False
    vs_cbet_flop_fold_oop: bool = False
    vs_cbet_flop_raise_oop: bool = False

    # bet vs missed cbet (SRP)
    flop_bet_vs_missed_cbet_srp: bool = False
    turn_bet_vs_missed_cbet_srp_oop: bool = False

    # Showdown / W$WSF
    saw_flop: bool = False
    saw_showdown: bool = False
    won_showdown: bool = False
    won_when_saw_flop: bool = False

    # River Agg %
    river_agg_pct: Optional[float] = None


class Derived(BaseModel):
    """Complete derived data structure"""
    positions: DerivedPositions
    preflop: DerivedPreflop
    ip: DerivedIP
    stacks: DerivedStacks
    flags: DerivedFlags
    postflop: DerivedPostflop