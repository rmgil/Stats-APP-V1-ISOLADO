"""
Centralized position mapping following GG Poker standard.
All position assignments must use this module for consistency.
"""

def get_position_map(num_players: int) -> dict:
    """
    Get position mapping for a given number of players.
    Follows GG Poker standard exactly.
    
    Returns mapping of relative_position (0=BTN, 1=SB, 2=BB, etc.) to position name.
    
    Position removal order as players decrease:
    9->8: Remove UTG+2 (Early position)
    8->7: Remove HJ (Middle position) - CUSTOM: Keep 2 EP + 1 MP for 7-max
    7->6: Remove HJ again (Middle position)
    6->5: Remove UTG (Early position)
    5->4: Remove MP (Middle position)
    """
    if num_players == 9:
        # Full ring: 3 Early (UTG, UTG+1, UTG+2), 2 Middle (MP, HJ), 2 Late (CO, BTN)
        return {
            0: "BTN",    # Button
            1: "SB",     # Small Blind
            2: "BB",     # Big Blind
            3: "UTG",    # Under the Gun (Early)
            4: "UTG+1",  # UTG+1 (Early)
            5: "UTG+2",  # UTG+2 (Early) - THIS WAS MISSING IN PREFLOP_STATS!
            6: "MP",     # Middle Position
            7: "HJ",     # Hijack (Middle)
            8: "CO"      # Cutoff (Late)
        }
    elif num_players == 8:
        # Remove UTG+2: 2 Early (UTG, UTG+1), 2 Middle (MP, HJ), 2 Late (CO, BTN)
        return {
            0: "BTN",
            1: "SB",
            2: "BB",
            3: "UTG",    # Early
            4: "UTG+1",  # Early
            5: "MP",     # Middle
            6: "HJ",     # Middle
            7: "CO"      # Late
        }
    elif num_players == 7:
        # Remove HJ: 2 Early (UTG, UTG+1), 1 Middle (MP), 2 Late (CO, BTN)
        return {
            0: "BTN",
            1: "SB",
            2: "BB",
            3: "UTG",    # Early
            4: "UTG+1",  # Early
            5: "MP",     # Middle (only one)
            6: "CO"      # Late
        }
    elif num_players == 6:
        # 6-max standard: Remove HJ: 1 Early (UTG), 1 Middle (MP), 2 Late (CO, BTN)
        return {
            0: "BTN",
            1: "SB",
            2: "BB",
            3: "UTG",    # Early
            4: "MP",     # Middle
            5: "CO"      # Late
        }
    elif num_players == 5:
        # Remove UTG: 0 Early, 1 Middle (MP), 2 Late (CO, BTN)
        return {
            0: "BTN",
            1: "SB",
            2: "BB",
            3: "MP",     # Middle (no Early positions)
            4: "CO"      # Late
        }
    elif num_players == 4:
        # Remove MP: 0 Early, 0 Middle, 2 Late (CO, BTN)
        return {
            0: "BTN",
            1: "SB",
            2: "BB",
            3: "CO"      # Late (no Early or Middle)
        }
    elif num_players == 3:
        # Heads-up + 1: Only BTN, SB, BB
        return {
            0: "BTN",
            1: "SB",
            2: "BB"
        }
    elif num_players == 2:
        # Heads-up: BTN = SB
        return {
            0: "BTN/SB",
            1: "BB"
        }
    else:
        # Unsupported player count - return empty
        return {}


def get_position_category(position: str) -> str:
    """
    Categorize position for RFI and other statistics.
    
    Returns: "Early", "Middle", "Late", or None
    
    GG Poker categories:
    - Early: UTG, UTG+1, UTG+2
    - Middle: MP, MP+1, MP+2, HJ (but we simplify to MP, HJ)
    - Late: CO, BTN
    - Blinds: SB, BB (not categorized for RFI)
    """
    # Early positions
    if position in ["UTG", "UTG+1", "UTG+2"]:
        return "Early"
    
    # Middle positions
    # Note: MP+1, MP+2 are alternative names for the same positions
    # In our standardized mapping, we use MP and HJ
    elif position in ["MP", "MP+1", "MP+2", "HJ"]:
        return "Middle"
    
    # Late positions
    elif position in ["CO", "BTN"]:
        return "Late"
    
    # Blinds (not used for RFI categories)
    elif position in ["SB", "BB", "BTN/SB"]:
        return None
    
    # Unknown position
    return None


def get_rfi_stat_name(position_category: str) -> str:
    """
    Get the RFI statistic name for a position category.
    
    Args:
        position_category: "Early", "Middle", "Late"
    
    Returns:
        Statistic name like "Early RFI", "CO Steal", etc.
    """
    if position_category == "Early":
        return "Early RFI"
    elif position_category == "Middle":
        return "Middle RFI"
    elif position_category == "Late":
        # Need actual position for Late
        return None  # Caller should handle CO vs BTN
    return None


def get_rfi_stat_for_position(position: str) -> str:
    """
    Get the specific RFI statistic name for a position.
    
    Returns the exact stat name like "Early RFI", "Middle RFI", "CO Steal", "BTN Steal"
    """
    category = get_position_category(position)
    
    if category == "Early":
        return "Early RFI"
    elif category == "Middle":
        return "Middle RFI"
    elif position == "CO":
        return "CO Steal"
    elif position == "BTN":
        return "BTN Steal"
    
    return None