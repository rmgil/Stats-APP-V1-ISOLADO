"""
Centralized configuration for derive module
"""
import logging

logger = logging.getLogger(__name__)

# Position configurations
POSITION_CONFIGS = {
    "6max": {
        "order": ["BTN", "SB", "BB", "EP", "MP", "CO"],
        "buckets": {
            "EP": ["EP"], 
            "MP": ["MP"], 
            "LP": ["CO", "BTN"]
        }
    },
    "9max": {
        "order": ["BTN", "SB", "BB", "EP", "EP2", "MP1", "MP2", "MP3", "CO"],
        "buckets": {
            "EP": ["EP", "EP2"], 
            "MP": ["MP1", "MP2", "MP3"], 
            "LP": ["CO", "BTN"]
        }
    },
    "preserve": ["CO", "BTN", "SB", "BB"]  # Positions to preserve when short-handed
}

# Default values for error scenarios
DEFAULT_PREFLOP_VALUES = {
    "hero_vpip": False,
    "hero_position": None,
    "pot_size_bb": None
}

DEFAULT_POSITION_VALUES = {}

DEFAULT_DERIVED_VALUES = {
    "preflop": DEFAULT_PREFLOP_VALUES,
    "positions": DEFAULT_POSITION_VALUES,
    "ip": {
        "heads_up_flop": False
    },
    "stacks": {
        "eff_stack_bb_srp": None,
        "eff_stack_bb_vs_3bettor": None
    }
}

def get_default_preflop_values():
    """Return default preflop values for error cases"""
    return DEFAULT_PREFLOP_VALUES.copy()

def get_default_position_values():
    """Return default position values for error cases"""
    return DEFAULT_POSITION_VALUES.copy()

def get_default_derived_values():
    """Return default derived values for error cases"""
    import copy
    return copy.deepcopy(DEFAULT_DERIVED_VALUES)