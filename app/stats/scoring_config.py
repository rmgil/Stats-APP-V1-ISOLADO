"""
Configuration for ideal statistics and scoring parameters.
Each stat has an ideal percentage and oscillation factors for deviations.
"""

SCORING_CONFIG = {
    'nonko_9max': {
        'Early RFI': {
            'ideal': 19.0,  # Ideal percentage
            'oscillation_down': 2.0,  # Each 2% of ideal below reduces score
            'oscillation_up': 4.0,     # Each 4% of ideal above reduces score
            'weight': 0.1  # 10% weight for overall RFI score
        },
        'Middle RFI': {
            'ideal': 23.0,
            'oscillation_down': 2.0,  # Each 2% of ideal below reduces score
            'oscillation_up': 3.0,     # Each 3% of ideal above reduces score
            'weight': 0.2  # 20% weight for overall RFI score
        },
        'CO Steal': {
            'ideal': 36.0,
            'oscillation_down': 2.0,  # Each 2% of ideal below reduces score
            'oscillation_up': 6.0,     # Each 6% of ideal above reduces score
            'weight': 0.3  # 30% weight for overall RFI score
        },
        'BTN Steal': {
            'ideal': 48.0,
            'oscillation_down': 1.0,  # Each 1% of ideal below reduces score
            'oscillation_up': 8.0,     # Each 8% of ideal above reduces score
            'weight': 0.4  # 40% weight for overall RFI score
        },
        
        # BvB Stats (Battle of the Blinds)
        'SB UO VPIP': {
            'ideal': 83.0,
            'oscillation_down': 1.0,  # 1% para baixo
            'oscillation_up': 999.0,   # Effectively no upper limit (above ideal = max score)
            'weight': 0.25  # 25% weight for BvB score
        },
        'BB fold vs SB steal': {
            'ideal': 30.0,
            'oscillation_down': 12.0,  # 12% para baixo
            'oscillation_up': 3.0,     # 3% para cima
            'weight': 0.25  # 25% weight for BvB score
        },
        'BB raise vs SB limp UOP': {
            'ideal': 57.0,
            'oscillation_down': 3.0,   # 3% para baixo
            'oscillation_up': 18.0,    # 18% para cima
            'weight': 0.25  # 25% weight for BvB score
        },
        'SB Steal': {
            'ideal': 28.0,
            'oscillation_down': 2.0,   # 2% para baixo
            'oscillation_up': 100.0,   # 100% para cima
            'weight': 0.25  # 25% weight for BvB score
        },
        
        # 3bet & Cold Call Stats (no ideal values for 3bet and Cold Call - only VPIP has scores)
        'EP 3bet': None,  # No scoring, only percentage display
        'EP Cold Call': None,  # No scoring, only percentage display
        'EP VPIP': {'ideal': 8.0, 'oscillation_down': 4.0, 'oscillation_up': 4.0, 'weight': 0.25},
        'MP 3bet': None,  # No scoring, only percentage display
        'MP Cold Call': None,  # No scoring, only percentage display
        'MP VPIP': {'ideal': 11.0, 'oscillation_down': 3.0, 'oscillation_up': 3.0, 'weight': 0.25},
        'CO 3bet': None,  # No scoring, only percentage display
        'CO Cold Call': None,  # No scoring, only percentage display
        'CO VPIP': {'ideal': 15.0, 'oscillation_down': 3.0, 'oscillation_up': 3.0, 'weight': 0.25},
        'BTN 3bet': None,  # No scoring, only percentage display
        'BTN Cold Call': None,  # No scoring, only percentage display
        'BTN VPIP': {'ideal': 21.0, 'oscillation_down': 3.0, 'oscillation_up': 3.0, 'weight': 0.20},
        'BTN fold to CO steal': {'ideal': 74.0, 'oscillation_down': 6.0, 'oscillation_up': 2.0, 'weight': 0.05},
        
        # vs 3bet IP/OOP
        'Fold to 3bet IP': {'ideal': 55.0, 'oscillation_down': 7.0, 'oscillation_up': 7.0, 'weight': 0.50},
        'Fold to 3bet OOP': {'ideal': 50.0, 'oscillation_down': 7.0, 'oscillation_up': 7.0, 'weight': 0.50},
        'Fold to 3bet': None,  # No scoring, only percentage display
        
        # Squeeze - 9max
        'Squeeze': {'ideal': 7.0, 'oscillation_down': 4.0, 'oscillation_up': 9.0, 'weight': 0.70},
        'Squeeze vs BTN Raiser': {'ideal': 12.0, 'oscillation_down': 4.0, 'oscillation_up': 9.0, 'weight': 0.30},
        
        # Defesa da BB - 9max
        'BB fold vs CO steal': {'ideal': 23.0, 'oscillation_down': 8.0, 'oscillation_up': 4.0, 'weight': 0.30},
        'BB fold vs BTN steal': {'ideal': 19.0, 'oscillation_down': 8.0, 'oscillation_up': 4.0, 'weight': 0.35},
        'BB resteal vs BTN steal': {'ideal': 16.0, 'oscillation_down': 3.0, 'oscillation_up': 12.0, 'weight': 0.20},
        
        # Defesa da SB (placeholders)
        'SB fold to CO Steal': {'ideal': 80.0, 'oscillation_down': 5.0, 'oscillation_up': 10.0, 'weight': 0.33},
        'SB fold to BTN Steal': {'ideal': 75.0, 'oscillation_down': 5.0, 'oscillation_up': 10.0, 'weight': 0.33},
        'SB resteal vs BTN': {'ideal': 10.0, 'oscillation_down': 2.0, 'oscillation_up': 3.0, 'weight': 0.34}
    },
    'nonko_6max': {
        'Early RFI': {
            'ideal': 23.0,  # Higher in 6-max
            'oscillation_down': 2.0,  # Each 2% of ideal below reduces score
            'oscillation_up': 4.0,     # Each 4% of ideal above reduces score
            'weight': 0.1  # 10% weight for overall RFI score
        },
        'Middle RFI': {
            'ideal': 28.0,
            'oscillation_down': 2.0,  # Each 2% of ideal below reduces score
            'oscillation_up': 3.0,     # Each 3% of ideal above reduces score
            'weight': 0.2  # 20% weight for overall RFI score
        },
        'CO Steal': {
            'ideal': 34.0,
            'oscillation_down': 2.0,  # Each 2% of ideal below reduces score
            'oscillation_up': 6.0,     # Each 6% of ideal above reduces score
            'weight': 0.3  # 30% weight for overall RFI score
        },
        'BTN Steal': {
            'ideal': 47.0,
            'oscillation_down': 1.0,  # Each 1% of ideal below reduces score
            'oscillation_up': 8.0,     # Each 8% of ideal above reduces score
            'weight': 0.4  # 40% weight for overall RFI score
        },
        
        # BvB Stats (Battle of the Blinds) - 6max
        'SB UO VPIP': {
            'ideal': 82.0,
            'oscillation_down': 1.0,  # 1% para baixo
            'oscillation_up': 999.0,   # Effectively no upper limit (above ideal = max score)
            'weight': 0.25  # 25% weight for BvB score
        },
        'BB fold vs SB steal': {
            'ideal': 30.0,
            'oscillation_down': 12.0,  # 12% para baixo
            'oscillation_up': 3.0,     # 3% para cima
            'weight': 0.25  # 25% weight for BvB score
        },
        'BB raise vs SB limp UOP': {
            'ideal': 55.0,
            'oscillation_down': 3.0,   # 3% para baixo
            'oscillation_up': 18.0,    # 18% para cima
            'weight': 0.25  # 25% weight for BvB score
        },
        'SB Steal': {
            'ideal': 28.0,
            'oscillation_down': 2.0,   # 2% para baixo
            'oscillation_up': 100.0,   # 100% para cima
            'weight': 0.25  # 25% weight for BvB score
        },
        
        # 3bet & Cold Call Stats - 6max (EP doesn't exist in 6max)
        'EP 3bet': None,  # EP doesn't exist in 6max
        'EP Cold Call': None,  # EP doesn't exist in 6max
        'EP VPIP': None,  # EP doesn't exist in 6max
        'MP 3bet': None,  # No scoring, only percentage display
        'MP Cold Call': None,  # No scoring, only percentage display
        'MP VPIP': {'ideal': 13.0, 'oscillation_down': 4.0, 'oscillation_up': 3.0, 'weight': 0.25},
        'CO 3bet': None,  # No scoring, only percentage display
        'CO Cold Call': None,  # No scoring, only percentage display
        'CO VPIP': {'ideal': 18.0, 'oscillation_down': 3.5, 'oscillation_up': 3.0, 'weight': 0.25},
        'BTN 3bet': None,  # No scoring, only percentage display
        'BTN Cold Call': None,  # No scoring, only percentage display
        'BTN VPIP': {'ideal': 23.0, 'oscillation_down': 3.0, 'oscillation_up': 3.0, 'weight': 0.25},
        'BTN fold to CO steal': {'ideal': 73.0, 'oscillation_down': 6.0, 'oscillation_up': 2.0, 'weight': 0.25},
        
        # vs 3bet IP/OOP - 6max
        'Fold to 3bet IP': {'ideal': 55.0, 'oscillation_down': 7.0, 'oscillation_up': 7.0, 'weight': 0.50},
        'Fold to 3bet OOP': {'ideal': 50.0, 'oscillation_down': 7.0, 'oscillation_up': 7.0, 'weight': 0.50},
        'Fold to 3bet': None,  # No scoring, only percentage display
        
        # Squeeze - 6max
        'Squeeze': {'ideal': 7.0, 'oscillation_down': 4.0, 'oscillation_up': 9.0, 'weight': 0.70},
        'Squeeze vs BTN Raiser': {'ideal': 12.0, 'oscillation_down': 4.0, 'oscillation_up': 9.0, 'weight': 0.30},
        
        # Defesa da BB - 6max
        'BB fold vs CO steal': {'ideal': 24.0, 'oscillation_down': 8.0, 'oscillation_up': 4.0, 'weight': 0.30},
        'BB fold vs BTN steal': {'ideal': 20.0, 'oscillation_down': 8.0, 'oscillation_up': 4.0, 'weight': 0.35},
        'BB resteal vs BTN steal': {'ideal': 16.0, 'oscillation_down': 3.0, 'oscillation_up': 12.0, 'weight': 0.20},
        
        # Defesa da SB - 6max (placeholders)
        'SB fold to CO Steal': {'ideal': 78.0, 'oscillation_down': 5.0, 'oscillation_up': 10.0, 'weight': 0.33},
        'SB fold to BTN Steal': {'ideal': 73.0, 'oscillation_down': 5.0, 'oscillation_up': 10.0, 'weight': 0.33},
        'SB resteal vs BTN': {'ideal': 11.0, 'oscillation_down': 2.0, 'oscillation_up': 3.0, 'weight': 0.34}
    },
    'pko': {
        'Early RFI': {
            'ideal': 22.0,  # More aggressive in PKO
            'oscillation_down': 2.0,  # Each 2% of ideal below reduces score
            'oscillation_up': 4.0,     # Each 4% of ideal above reduces score
            'weight': 0.1  # 10% weight for overall RFI score
        },
        'Middle RFI': {
            'ideal': 27.0,
            'oscillation_down': 2.0,  # Each 2% of ideal below reduces score
            'oscillation_up': 3.0,     # Each 3% of ideal above reduces score
            'weight': 0.2  # 20% weight for overall RFI score
        },
        'CO Steal': {
            'ideal': 38.0,
            'oscillation_down': 2.0,  # Each 2% of ideal below reduces score
            'oscillation_up': 6.0,     # Each 6% of ideal above reduces score
            'weight': 0.3  # 30% weight for overall RFI score
        },
        'BTN Steal': {
            'ideal': 53.0,
            'oscillation_down': 1.0,  # Each 1% of ideal below reduces score
            'oscillation_up': 8.0,     # Each 8% of ideal above reduces score
            'weight': 0.4  # 40% weight for overall RFI score
        },
        
        # BvB Stats (Battle of the Blinds) - PKO
        'SB UO VPIP': {
            'ideal': 83.0,
            'oscillation_down': 1.0,  # 1% para baixo
            'oscillation_up': 999.0,   # Effectively no upper limit (above ideal = max score)
            'weight': 0.25  # 25% weight for BvB score
        },
        'BB fold vs SB steal': {
            'ideal': 24.0,  # Lower in PKO (defend more due to bounties)
            'oscillation_down': 12.0,  # 12% para baixo
            'oscillation_up': 3.0,     # 3% para cima
            'weight': 0.25  # 25% weight for BvB score
        },
        'BB raise vs SB limp UOP': {
            'ideal': 57.0,
            'oscillation_down': 3.0,   # 3% para baixo
            'oscillation_up': 18.0,    # 18% para cima
            'weight': 0.25  # 25% weight for BvB score
        },
        'SB Steal': {
            'ideal': 28.0,
            'oscillation_down': 2.0,   # 2% para baixo
            'oscillation_up': 100.0,   # 100% para cima
            'weight': 0.25  # 25% weight for BvB score
        },
        
        # 3bet & Cold Call Stats - PKO
        'EP 3bet': None,  # No scoring, only percentage display
        'EP Cold Call': None,  # No scoring, only percentage display
        'EP VPIP': {'ideal': 13.0, 'oscillation_down': 4.0, 'oscillation_up': 4.0, 'weight': 0.20},
        'MP 3bet': None,  # No scoring, only percentage display
        'MP Cold Call': None,  # No scoring, only percentage display
        'MP VPIP': {'ideal': 15.5, 'oscillation_down': 3.0, 'oscillation_up': 3.0, 'weight': 0.20},
        'CO 3bet': None,  # No scoring, only percentage display
        'CO Cold Call': None,  # No scoring, only percentage display
        'CO VPIP': {'ideal': 22.0, 'oscillation_down': 3.0, 'oscillation_up': 3.0, 'weight': 0.20},
        'BTN 3bet': None,  # No scoring, only percentage display
        'BTN Cold Call': None,  # No scoring, only percentage display
        'BTN VPIP': {'ideal': 26.5, 'oscillation_down': 3.0, 'oscillation_up': 3.0, 'weight': 0.20},
        'BTN fold to CO steal': {'ideal': 68.0, 'oscillation_down': 6.0, 'oscillation_up': 2.0, 'weight': 0.20},
        
        # vs 3bet IP/OOP - PKO
        'Fold to 3bet IP': {'ideal': 48.0, 'oscillation_down': 7.0, 'oscillation_up': 7.0, 'weight': 0.50},
        'Fold to 3bet OOP': {'ideal': 44.0, 'oscillation_down': 7.0, 'oscillation_up': 7.0, 'weight': 0.50},
        'Fold to 3bet': None,  # No scoring, only percentage display
        
        # Squeeze - PKO
        'Squeeze': {'ideal': 9.0, 'oscillation_down': 4.0, 'oscillation_up': 9.0, 'weight': 0.70},
        'Squeeze vs BTN Raiser': {'ideal': 13.0, 'oscillation_down': 4.0, 'oscillation_up': 9.0, 'weight': 0.30},
        
        # Defesa da BB - PKO
        'BB fold vs CO steal': {'ideal': 17.5, 'oscillation_down': 8.0, 'oscillation_up': 4.0, 'weight': 0.30},
        'BB fold vs BTN steal': {'ideal': 15.0, 'oscillation_down': 8.0, 'oscillation_up': 4.0, 'weight': 0.35},
        'BB resteal vs BTN steal': {'ideal': 18.0, 'oscillation_down': 3.0, 'oscillation_up': 12.0, 'weight': 0.20},
        
        # Defesa da SB - PKO (handled by SBDefenseScorer)
        'SB fold to CO Steal': None,  # Handled by SBDefenseScorer
        'SB fold to BTN Steal': None,  # Handled by SBDefenseScorer
        'SB resteal vs BTN': None  # Handled by SBDefenseScorer
    },
    'postflop_all': {
        # Flop Cbet Group
        # Flop CBet IP % - ideal 90, down 2% (1.8), up 6% (5.4)
        'Flop CBet IP %': {'ideal': 90.0, 'oscillation_down': 1.8, 'oscillation_up': 5.4, 'weight': 0.40, 'group': 'Flop Cbet'},
        # Flop CBet 3BetPot IP - ideal 90, down 2% (1.8), up 6% (5.4)
        'Flop CBet 3BetPot IP': {'ideal': 90.0, 'oscillation_down': 1.8, 'oscillation_up': 5.4, 'weight': 0.20, 'group': 'Flop Cbet'},
        # Flop CBet OOP% - ideal 37, down 2% (0.74), up 4% (1.48)
        'Flop CBet OOP%': {'ideal': 37.0, 'oscillation_down': 0.74, 'oscillation_up': 1.48, 'weight': 0.40, 'group': 'Flop Cbet'},
        
        # Vs Cbet Group
        # Flop fold vs Cbet IP - ideal 31, down 6% (1.86), up 4% (1.24)
        'Flop fold vs Cbet IP': {'ideal': 31.0, 'oscillation_down': 1.86, 'oscillation_up': 1.24, 'weight': 0.30, 'group': 'Vs Cbet'},
        # Flop raise Cbet IP - ideal 12.5, down 2% (0.25), up 50% (6.25)
        'Flop raise Cbet IP': {'ideal': 12.5, 'oscillation_down': 0.25, 'oscillation_up': 6.25, 'weight': 0.10, 'group': 'Vs Cbet'},
        # Flop raise Cbet OOP - ideal 20, down 2% (0.4), up 20% (4.0)
        'Flop raise Cbet OOP': {'ideal': 20.0, 'oscillation_down': 0.4, 'oscillation_up': 4.0, 'weight': 0.40, 'group': 'Vs Cbet'},
        # Fold vs Check Raise - ideal 32, down 2% (0.64), up 40% (12.8)
        'Fold vs Check Raise': {'ideal': 32.0, 'oscillation_down': 0.64, 'oscillation_up': 12.8, 'weight': 0.20, 'group': 'Vs Cbet'},
        
        # vs Skipped Cbet Group
        # Flop bet vs missed Cbet SRP - ideal 60, down 3% (1.8), up 12% (7.2)
        'Flop bet vs missed Cbet SRP': {'ideal': 60.0, 'oscillation_down': 1.8, 'oscillation_up': 7.2, 'weight': 1.0, 'group': 'vs Skipped Cbet'},
        
        # Turn Play Group
        # Turn CBet IP% - ideal 60, down 2% (1.2), up 20% (12.0)
        'Turn CBet IP%': {'ideal': 60.0, 'oscillation_down': 1.2, 'oscillation_up': 12.0, 'weight': 0.50, 'group': 'Turn Play'},
        # Turn Cbet OOP% - ideal 50, down 2% (1.0), up 10% (5.0)
        'Turn Cbet OOP%': {'ideal': 50.0, 'oscillation_down': 1.0, 'oscillation_up': 5.0, 'weight': 0.10, 'group': 'Turn Play'},
        # Turn donk bet - ideal 8, down 3% (0.24), up 25% (2.0)
        'Turn donk bet': {'ideal': 8.0, 'oscillation_down': 0.24, 'oscillation_up': 2.0, 'weight': 0.05, 'group': 'Turn Play'},
        # Turn donk bet SRP vs PFR - ideal 12, down 3% (0.36), up 25% (3.0)
        'Turn donk bet SRP vs PFR': {'ideal': 12.0, 'oscillation_down': 0.36, 'oscillation_up': 3.0, 'weight': 0.05, 'group': 'Turn Play'},
        # Bet turn vs Missed Flop Cbet OOP SRP - ideal 45, down 2% (0.9), up 10% (4.5)
        'Bet turn vs Missed Flop Cbet OOP SRP': {'ideal': 45.0, 'oscillation_down': 0.9, 'oscillation_up': 4.5, 'weight': 0.20, 'group': 'Turn Play'},
        # Turn Fold vs CBet OOP - ideal 43, down 5% (2.15), up 5% (2.15)
        'Turn Fold vs CBet OOP': {'ideal': 43.0, 'oscillation_down': 2.15, 'oscillation_up': 2.15, 'weight': 0.10, 'group': 'Turn Play'},
        
        # River play Group
        # WTSD% - ideal 30, down 3% (0.9), up 3% (0.9)
        'WTSD%': {'ideal': 30.0, 'oscillation_down': 0.9, 'oscillation_up': 0.9, 'weight': 0.15, 'group': 'River play'},
        # W$SD% - ideal 50, down 2% (1.0), up 2% (1.0)
        'W$SD%': {'ideal': 50.0, 'oscillation_down': 1.0, 'oscillation_up': 1.0, 'weight': 0.15, 'group': 'River play'},
        # W$WSF Rating - Sem ideal, sem scoring (apenas display de ratio)
        'W$WSF Rating': {'ideal': None, 'oscillation_down': 0.0, 'oscillation_up': 0.0, 'weight': 0.0, 'group': 'River play'},
        # River Agg % - ideal 2.5, down 2% (0.05), up 6% (0.15)
        'River Agg %': {'ideal': 2.5, 'oscillation_down': 0.05, 'oscillation_up': 0.15, 'weight': 0.15, 'group': 'River play'},
        # River bet - Single Rsd Pot - ideal 45, down 3% (1.35), up 4% (1.8)
        'River bet - Single Rsd Pot': {'ideal': 45.0, 'oscillation_down': 1.35, 'oscillation_up': 1.8, 'weight': 0.40, 'group': 'River play'},
        # W$SD% B River - ideal 57, down 1.5% (0.855), up 1.5% (0.855)
        'W$SD% B River': {'ideal': 57.0, 'oscillation_down': 0.855, 'oscillation_up': 0.855, 'weight': 0.15, 'group': 'River play'}
    }
}

def get_stat_config(group_key: str, stat_name: str) -> dict:
    """Get configuration for a specific stat in a group."""
    return SCORING_CONFIG.get(group_key, {}).get(stat_name, None)

def update_ideal_value(group_key: str, stat_name: str, new_ideal: float):
    """Update the ideal value for a specific stat."""
    if group_key in SCORING_CONFIG and stat_name in SCORING_CONFIG[group_key]:
        SCORING_CONFIG[group_key][stat_name]['ideal'] = new_ideal

def update_oscillation_factors(group_key: str, stat_name: str, down: float = None, up: float = None):
    """Update oscillation factors for a specific stat."""
    if group_key in SCORING_CONFIG and stat_name in SCORING_CONFIG[group_key]:
        if down is not None:
            SCORING_CONFIG[group_key][stat_name]['oscillation_down'] = down
        if up is not None:
            SCORING_CONFIG[group_key][stat_name]['oscillation_up'] = up