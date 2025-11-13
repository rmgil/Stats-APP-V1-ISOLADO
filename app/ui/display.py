"""
Display helpers for user-friendly stat names and labels
"""

# Display names for statistics
STAT_DISPLAY_NAMES = {
    # Preflop RFI
    "RFI_EARLY": "RFI Early Position",
    "RFI_MIDDLE": "RFI Middle Position",
    "RFI_CO": "RFI Cutoff",
    "RFI_BTN": "RFI Button",
    "RFI_SB": "RFI Small Blind",
    
    # Steals
    "CO_STEAL": "CO Steal",
    "BTN_STEAL": "BTN Steal",
    "SB_STEAL": "SB Steal",
    
    # 3-Bet
    "3BET_VS_EARLY": "3-Bet vs Early",
    "3BET_VS_MIDDLE": "3-Bet vs Middle",
    "3BET_VS_CO": "3-Bet vs CO",
    "3BET_VS_BTN": "3-Bet vs BTN",
    "3BET_VS_SB": "3-Bet vs SB",
    "3BET_TOTAL": "3-Bet Total",
    
    # 4-Bet
    "4BET_VS_3BET": "4-Bet vs 3-Bet",
    "4BET_TOTAL": "4-Bet Total",
    
    # Folds
    "FOLD_TO_3BET": "Fold to 3-Bet",
    "FOLD_TO_4BET": "Fold to 4-Bet",
    "FOLD_TO_STEAL": "Fold to Steal",
    
    # BB Defense
    "BB_DEFENSE_VS_CO": "BB Defense vs CO",
    "BB_DEFENSE_VS_BTN": "BB Defense vs BTN",
    "BB_DEFENSE_VS_SB": "BB Defense vs SB",
    "BB_FREEPLAY": "BB Freeplay",
    
    # Isolate & Squeeze
    "ISO_VS_LIMPERS": "Isolate Limpers",
    "SQUEEZE": "Squeeze",
    "RESTEAL_VS_BTN": "Resteal vs BTN",
    
    # Postflop Aggression
    "CBET_FLOP": "C-Bet Flop",
    "CBET_TURN": "C-Bet Turn",
    "CBET_RIVER": "C-Bet River",
    "CHECK_RAISE_FLOP": "Check-Raise Flop",
    "CHECK_RAISE_TURN": "Check-Raise Turn",
    "CHECK_RAISE_RIVER": "Check-Raise River",
    "DONK_BET": "Donk Bet",
    
    # Postflop Defense
    "FOLD_TO_CBET_FLOP": "Fold to C-Bet Flop",
    "FOLD_TO_CBET_TURN": "Fold to C-Bet Turn",
    "FOLD_TO_CBET_RIVER": "Fold to C-Bet River",
    "CALL_CBET_FLOP": "Call C-Bet Flop",
    "RAISE_CBET_FLOP": "Raise C-Bet Flop",
    
    # General
    "VPIP": "VPIP",
    "PFR": "PFR",
    "AF": "Aggression Factor",
    "WTSD": "Went to Showdown",
    "WSD": "Won at Showdown",
    "WWSF": "Won When Saw Flop",
    
    # Additional Preflop Stats
    "RFI_CO": "RFI Cutoff",
    "RFI_BTN": "RFI Button",
    "RFI_SB": "RFI Small Blind",
    "RFI_EP_SHORT": "RFI EP Short-handed",
    "RFI_MP_SHORT": "RFI MP Short-handed",
    "RFI_CUTOFF": "RFI Cutoff",
    "RFI_BUTTON": "RFI Button",
    "RFI_SMALL_BLIND": "RFI Small Blind",
    "3BET_VS_CUTOFF": "3-Bet vs Cutoff",
    "3BET_VS_BUTTON": "3-Bet vs Button",
    "ISO_VS_LIMP": "Iso vs Limp",
    "COLD_4BET": "Cold 4-Bet",
    "4BET_RANGE": "4-Bet Range",
    "FREEPLAY_BB": "Freeplay BB",
    
    # Additional Postflop Stats  
    "POST_CBET_FLOP_IP": "C-Bet Flop IP",
    "POST_CBET_FLOP_OOP": "C-Bet Flop OOP",
    "POST_BET_VS_MISSED_CBET_IP": "Bet vs Missed C-Bet IP",
    "POST_BET_VS_MISSED_CBET_SRP_FLOP": "Bet vs Missed C-Bet SRP",
    "POST_CBET_TURN_IP": "C-Bet Turn IP",
    "POST_CBET_TURN_OOP": "C-Bet Turn OOP",
    "POST_DONK_TURN": "Donk Turn",
    "POST_DONK_TURN_SRP_VS_PFR": "Donk Turn SRP vs PFR",
    "POST_BET_TURN_VS_MISSED_FLOP_CBET_OOP_SRP": "Bet Turn vs Missed Flop C-Bet OOP",
    "POST_TURN_FOLD_VS_CBET_OOP": "Fold Turn vs C-Bet OOP",
    "POST_WTSD": "Went to Showdown",
    "POST_W$SD": "Won $ at Showdown",
    "POST_W$WSF": "Won $ When Saw Flop",
    "POST_RIVER_AGG_PCT": "River Aggression %",
    "POST_RIVER_BET_SINGLE_RAISED_POT": "River Bet SRP",
    "POST_W$SD_B_RIVER": "Won $ SD after River Bet",
    "POST_DONK_FLOP": "Donk Flop",
    "POST_VS_CBET_FLOP_FOLD_OOP": "Fold Flop vs C-Bet OOP",
    "POST_VS_CBET_FLOP_CALL_IP": "Call Flop vs C-Bet IP",
    "POST_VS_CBET_FLOP_CALL_OOP": "Call Flop vs C-Bet OOP",
    "POST_PROBE_TURN_ATT_IP": "Probe Turn IP",
    "POST_PROBE_TURN_ATT_OOP": "Probe Turn OOP",
    "POST_DELAYED_CBET_TURN_ATT_IP": "Delayed C-Bet Turn IP",
    "POST_DELAYED_CBET_TURN_ATT_OOP": "Delayed C-Bet Turn OOP",
    "POST_XR_FLOP": "Check-Raise Flop",
    "POST_BET_VS_MISS_TURN": "Bet Turn vs Missed C-Bet",
    "POST_WWSF": "Won When Saw Flop",
    "POST_AGG_PCT_FLOP": "Aggression % Flop",
    "POST_AGG_PCT_TURN": "Aggression % Turn",
    "POST_AGG_PCT_RIVER": "Aggression % River",
}

# Display names for groups
GROUP_DISPLAY_NAMES = {
    "nonko_9max": "NON-KO 9-Max",
    "nonko_6max": "NON-KO 6-Max",
    "pko_9max": "PKO 9-Max",
    "pko_6max": "PKO 6-Max",
    "mystery_9max": "Mystery Bounty 9-Max",
    "mystery_6max": "Mystery Bounty 6-Max",
}

# Display names for subgroups
SUBGROUP_DISPLAY_NAMES = {
    # Preflop
    "PREFLOP_RFI": "Pre-Flop RFI",
    "PREFLOP_3BET": "Pre-Flop 3-Bet",
    "PREFLOP_4BET": "Pre-Flop 4-Bet",
    "PREFLOP_STEAL": "Pre-Flop Steal",
    "PREFLOP_DEFENSE": "Pre-Flop Defense",
    "PREFLOP_ISO": "Pre-Flop Isolate",
    "PREFLOP_SQUEEZE": "Pre-Flop Squeeze",
    "PREFLOP_FOLD": "Pre-Flop Folds",
    
    # Postflop
    "POSTFLOP_CBET": "Post-Flop C-Bet",
    "POSTFLOP_DEFENSE": "Post-Flop Defense",
    "POSTFLOP_AGGRESSION": "Post-Flop Aggression",
    "POSTFLOP_CHECKRAISE": "Post-Flop Check-Raise",
    "POSTFLOP_DONK": "Post-Flop Donk",
    
    # General
    "GENERAL_VPIP": "VPIP/PFR",
    "GENERAL_AGGRESSION": "Aggression Stats",
    "GENERAL_SHOWDOWN": "Showdown Stats",
}

def stat_label(key: str) -> str:
    """
    Get user-friendly label for a stat code
    
    Args:
        key: Stat code (e.g., 'RFI_EARLY')
    
    Returns:
        User-friendly label or original key if not found
    """
    return STAT_DISPLAY_NAMES.get(key, key)

def group_label(key: str) -> str:
    """
    Get user-friendly label for a group code
    
    Args:
        key: Group code (e.g., 'nonko_9max')
    
    Returns:
        User-friendly label or original key if not found
    """
    return GROUP_DISPLAY_NAMES.get(key, key)

def subgroup_label(key: str) -> str:
    """
    Get user-friendly label for a subgroup code
    
    Args:
        key: Subgroup code (e.g., 'PREFLOP_RFI')
    
    Returns:
        User-friendly label or original key if not found
    """
    return SUBGROUP_DISPLAY_NAMES.get(key, key)

def format_percentage(value: float, decimals: int = 2) -> str:
    """
    Format a percentage value for display
    
    Args:
        value: Percentage value (e.g., 15.5)
        decimals: Number of decimal places
    
    Returns:
        Formatted string (e.g., '15.50%')
    """
    if value is None:
        return "-"
    return f"{value:.{decimals}f}%"

def format_ratio(attempts: int, opportunities: int) -> str:
    """
    Format attempts/opportunities ratio for display
    
    Args:
        attempts: Number of attempts
        opportunities: Number of opportunities
    
    Returns:
        Formatted string (e.g., '123/456')
    """
    if opportunities == 0:
        return "0/0"
    return f"{attempts}/{opportunities}"

def format_score(score: float) -> str:
    """
    Format score value for display
    
    Args:
        score: Score value (0-100)
    
    Returns:
        Formatted string with one decimal
    """
    if score is None:
        return "-"
    return f"{score:.1f}"

def get_color_class(pct: float, ideal_min: float, ideal_max: float) -> str:
    """
    Determine color class based on percentage and ideal range
    
    Args:
        pct: Actual percentage
        ideal_min: Minimum ideal percentage
        ideal_max: Maximum ideal percentage
    
    Returns:
        CSS class name: 'stat-green', 'stat-yellow', or 'stat-red'
    """
    if pct is None or ideal_min is None or ideal_max is None:
        return ""
    
    # Within ideal range = green
    if ideal_min <= pct <= ideal_max:
        return "stat-green"
    
    # Within 2pp tolerance = yellow
    if (abs(pct - ideal_min) <= 2.0) or (abs(pct - ideal_max) <= 2.0):
        return "stat-yellow"
    
    # Outside tolerance = red
    return "stat-red"