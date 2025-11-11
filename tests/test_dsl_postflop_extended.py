"""
Phase 8.2.F - Test DSL postflop extended stats
Verifies opportunity/attempt counts for each new stat in synthetic dataset
"""
import json
import tempfile
import pytest
from pathlib import Path
from app.stats.engine import run_stats


def create_synthetic_hands():
    """Create synthetic hands with various postflop scenarios"""
    hands = []
    
    # Hand 1: CBet turn IP
    hands.append({
        "hand_id": "hand_001",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T10:00:00",
        "players": [
            {"name": "Hero", "position": "BTN", "stack_bb": 100},
            {"name": "Villain1", "position": "BB", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Hero",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 100,
                "is_heads_up": True
            },
            "postflop": {
                "cbet_flop_opp_ip": True,
                "cbet_flop_att_ip": True,
                "cbet_turn_opp_ip": True,
                "cbet_turn_att_ip": True,
                "saw_flop": True,
                "saw_turn": True
            }
        }
    })
    
    # Hand 2: vs CBet flop call IP
    hands.append({
        "hand_id": "hand_002",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T11:00:00",
        "players": [
            {"name": "Hero", "position": "CO", "stack_bb": 80},
            {"name": "Villain1", "position": "BTN", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Villain1",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 80,
                "is_heads_up": True
            },
            "postflop": {
                "vs_cbet_flop_fold_ip": False,
                "vs_cbet_flop_call_ip": True,
                "vs_cbet_flop_raise_ip": False,
                "saw_flop": True,
                "saw_turn": True
            }
        }
    })
    
    # Hand 3: Probe turn OOP
    hands.append({
        "hand_id": "hand_003",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T12:00:00",
        "players": [
            {"name": "Hero", "position": "BB", "stack_bb": 90},
            {"name": "Villain1", "position": "BTN", "stack_bb": 110}
        ],
        "derived": {
            "preflop": {
                "pfr": "Villain1",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 90,
                "is_heads_up": True
            },
            "postflop": {
                "probe_turn_opp_oop": True,
                "probe_turn_att_oop": True,
                "saw_flop": True,
                "saw_turn": True
            }
        }
    })
    
    # Hand 4: Delayed CBet turn IP
    hands.append({
        "hand_id": "hand_004",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T13:00:00",
        "players": [
            {"name": "Hero", "position": "CO", "stack_bb": 75},
            {"name": "Villain1", "position": "BB", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Hero",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 75,
                "is_heads_up": True
            },
            "postflop": {
                "delayed_cbet_turn_opp_ip": True,
                "delayed_cbet_turn_att_ip": True,
                "saw_flop": True,
                "saw_turn": True
            }
        }
    })
    
    # Hand 5: Donk flop and turn
    hands.append({
        "hand_id": "hand_005",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T14:00:00",
        "players": [
            {"name": "Hero", "position": "BB", "stack_bb": 100},
            {"name": "Villain1", "position": "BTN", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Villain1",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 100,
                "is_heads_up": True
            },
            "postflop": {
                "donk_flop": True,
                "donk_turn": False,
                "saw_flop": True,
                "saw_turn": True
            }
        }
    })
    
    # Hand 6: Check-raise flop
    hands.append({
        "hand_id": "hand_006",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T15:00:00",
        "players": [
            {"name": "Hero", "position": "BB", "stack_bb": 100},
            {"name": "Villain1", "position": "BTN", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Villain1",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 100,
                "is_heads_up": True
            },
            "postflop": {
                "xr_flop_opp": True,
                "xr_flop_att": True,
                "saw_flop": True
            }
        }
    })
    
    # Hand 7: Bet vs missed CBet turn
    hands.append({
        "hand_id": "hand_007",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T16:00:00",
        "players": [
            {"name": "Hero", "position": "CO", "stack_bb": 100},
            {"name": "Villain1", "position": "BTN", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Villain1",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 100,
                "is_heads_up": True
            },
            "postflop": {
                "bet_vs_miss_turn_opp": True,
                "bet_vs_miss_turn_att": True,
                "saw_flop": True,
                "saw_turn": True
            }
        }
    })
    
    # Hand 8: Showdown stats
    hands.append({
        "hand_id": "hand_008",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T17:00:00",
        "players": [
            {"name": "Hero", "position": "BTN", "stack_bb": 100},
            {"name": "Villain1", "position": "BB", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Hero",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 100,
                "is_heads_up": True
            },
            "postflop": {
                "saw_flop": True,
                "saw_showdown": True,
                "won_showdown": True,
                "wwsf": True
            }
        }
    })
    
    # Hand 9: Aggression frequency
    hands.append({
        "hand_id": "hand_009",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T18:00:00",
        "players": [
            {"name": "Hero", "position": "BTN", "stack_bb": 100},
            {"name": "Villain1", "position": "BB", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Hero",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 100,
                "is_heads_up": True
            },
            "postflop": {
                "agg_pct_flop": 0.65,
                "agg_pct_turn": 0.55,
                "agg_pct_river": 0.45,
                "saw_flop": True,
                "saw_turn": True,
                "saw_river": True
            }
        }
    })
    
    # Hand 10: Multiple stats in one hand
    hands.append({
        "hand_id": "hand_010",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T19:00:00",
        "players": [
            {"name": "Hero", "position": "CO", "stack_bb": 100},
            {"name": "Villain1", "position": "BTN", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Villain1",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 100,
                "is_heads_up": True
            },
            "postflop": {
                "vs_cbet_flop_fold_ip": False,
                "vs_cbet_flop_call_ip": True,
                "vs_cbet_flop_raise_ip": False,
                "vs_cbet_turn_fold_oop": False,
                "vs_cbet_turn_call_oop": True,
                "saw_flop": True,
                "saw_turn": True,
                "saw_showdown": True,
                "won_showdown": False,
                "wwsf": False
            }
        }
    })
    
    return hands


def test_postflop_extended_stats():
    """Test that all new postflop stats are correctly computed"""
    
    # Create synthetic dataset
    hands = create_synthetic_hands()
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for hand in hands:
            f.write(json.dumps(hand) + '\n')
        temp_input = f.name
    
    # Create temporary output directory
    with tempfile.TemporaryDirectory() as temp_output:
        try:
            # Run stats engine
            result = run_stats(temp_input, 'app/stats/dsl/stats.yml', temp_output)
            
            # Load results
            counts_path = Path(temp_output) / 'stat_counts.json'
            with open(counts_path) as f:
                stats_data = json.load(f)
            
            counts = stats_data['counts']['2024-07']['postflop_all']
            
            # Verify CBet turn stats
            assert 'POST_CBET_TURN_IP' in counts
            assert counts['POST_CBET_TURN_IP']['opportunities'] >= 1
            assert counts['POST_CBET_TURN_IP']['attempts'] >= 1
            
            # Verify vs CBet call stats
            assert 'POST_VS_CBET_FLOP_CALL_IP' in counts
            assert counts['POST_VS_CBET_FLOP_CALL_IP']['opportunities'] >= 1
            assert counts['POST_VS_CBET_FLOP_CALL_IP']['attempts'] >= 1
            
            # Verify probe turn stats
            assert 'POST_PROBE_TURN_ATT_OOP' in counts
            assert counts['POST_PROBE_TURN_ATT_OOP']['opportunities'] >= 1
            assert counts['POST_PROBE_TURN_ATT_OOP']['attempts'] >= 1
            
            # Verify delayed CBet stats
            assert 'POST_DELAYED_CBET_TURN_ATT_IP' in counts
            assert counts['POST_DELAYED_CBET_TURN_ATT_IP']['opportunities'] >= 1
            assert counts['POST_DELAYED_CBET_TURN_ATT_IP']['attempts'] >= 1
            
            # Verify donk stats
            assert 'POST_DONK_FLOP' in counts
            assert counts['POST_DONK_FLOP']['opportunities'] >= 1
            assert counts['POST_DONK_FLOP']['attempts'] >= 1
            
            # Verify check-raise stats
            assert 'POST_XR_FLOP' in counts
            assert counts['POST_XR_FLOP']['opportunities'] >= 1
            assert counts['POST_XR_FLOP']['attempts'] >= 1
            
            # Verify bet vs missed CBet stats
            assert 'POST_BET_VS_MISS_TURN' in counts
            assert counts['POST_BET_VS_MISS_TURN']['opportunities'] >= 1
            assert counts['POST_BET_VS_MISS_TURN']['attempts'] >= 1
            
            # Verify showdown stats
            assert 'POST_WTSD' in counts
            assert counts['POST_WTSD']['opportunities'] >= 1
            assert counts['POST_WTSD']['attempts'] >= 1
            
            assert 'POST_W$SD' in counts
            assert counts['POST_W$SD']['opportunities'] >= 1
            
            assert 'POST_WWSF' in counts
            assert counts['POST_WWSF']['opportunities'] >= 1
            
            # Verify aggression frequency stats
            assert 'POST_AGG_PCT_FLOP' in counts
            assert counts['POST_AGG_PCT_FLOP']['opportunities'] >= 1
            assert counts['POST_AGG_PCT_FLOP']['percentage'] > 0
            
            assert 'POST_AGG_PCT_TURN' in counts
            assert counts['POST_AGG_PCT_TURN']['opportunities'] >= 1
            
            assert 'POST_AGG_PCT_RIVER' in counts
            assert counts['POST_AGG_PCT_RIVER']['opportunities'] >= 1
            
            # Verify total stats computed
            assert result['stats_computed'] >= 30  # Should have at least 30 postflop stats
            
        finally:
            # Clean up
            Path(temp_input).unlink()


def test_multiway_filtering():
    """Test that multiway hands are properly filtered"""
    
    # Create hand with multiway flag
    multiway_hand = {
        "hand_id": "hand_mw_001",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T20:00:00",
        "players": [
            {"name": "Hero", "position": "BTN", "stack_bb": 100},
            {"name": "Villain1", "position": "BB", "stack_bb": 100},
            {"name": "Villain2", "position": "CO", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Hero",
                "pot_type": "SRP",
                "players_to_flop": ["Hero", "Villain1", "Villain2"],
                "eff_stack_srp": 100,
                "is_heads_up": False
            },
            "postflop": {
                "is_multiway_flop": True,
                "cbet_flop_opp_ip": True,
                "cbet_flop_att_ip": True,
                "saw_flop": True
            }
        }
    }
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write(json.dumps(multiway_hand) + '\n')
        temp_input = f.name
    
    # Create temporary output directory
    with tempfile.TemporaryDirectory() as temp_output:
        try:
            # Run stats engine
            result = run_stats(temp_input, 'app/stats/dsl/stats.yml', temp_output)
            
            # Load results
            counts_path = Path(temp_output) / 'stat_counts.json'
            with open(counts_path) as f:
                stats_data = json.load(f)
            
            counts = stats_data['counts']['2024-07']['postflop_all']
            
            # Verify that multiway-gated stats are filtered
            # These stats should be filtered when is_multiway_flop is True
            multiway_gated_stats = [
                'POST_VS_CBET_FLOP_FOLD_IP',
                'POST_VS_CBET_FLOP_CALL_IP',
                'POST_VS_CBET_FLOP_RAISE_IP'
            ]
            
            for stat in multiway_gated_stats:
                if stat in counts:
                    # If the stat exists, it should have 0 opportunities due to multiway filtering
                    assert counts[stat]['opportunities'] == 0, f"{stat} should be filtered in multiway"
            
        finally:
            # Clean up
            Path(temp_input).unlink()


def test_3bet_pot_filtering():
    """Test that 3bet pot specific stats work correctly"""
    
    # Create hand with 3bet pot
    threbet_hand = {
        "hand_id": "hand_3b_001",
        "hero": "Hero",
        "tournament_id": "t1",
        "datetime": "2024-07-15T21:00:00",
        "players": [
            {"name": "Hero", "position": "BTN", "stack_bb": 100},
            {"name": "Villain1", "position": "BB", "stack_bb": 100}
        ],
        "derived": {
            "preflop": {
                "pfr": "Hero",
                "pot_type": "3bet",
                "players_to_flop": ["Hero", "Villain1"],
                "eff_stack_srp": 100,
                "is_heads_up": True,
                "is_3bet": True
            },
            "postflop": {
                "cbet_flop_3betpot_opp_ip": True,
                "cbet_flop_3betpot_att_ip": True,
                "saw_flop": True
            }
        }
    }
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write(json.dumps(threbet_hand) + '\n')
        temp_input = f.name
    
    # Create temporary output directory
    with tempfile.TemporaryDirectory() as temp_output:
        try:
            # Run stats engine
            result = run_stats(temp_input, 'app/stats/dsl/stats.yml', temp_output)
            
            # Stats should be computed successfully
            assert result['stats_computed'] > 0
            
        finally:
            # Clean up
            Path(temp_input).unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])