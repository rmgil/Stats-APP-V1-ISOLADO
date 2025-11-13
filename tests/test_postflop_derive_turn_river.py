"""
Phase 8.2.F - Test postflop derivations for turn and river streets
Tests CBet turn, vs cbet responses, probe, delayed cbet, donk, xr
"""
import json
import pytest
from app.derive.postflop import derive_postflop

def create_test_hand(streets_actions, hero_name="Hero", pfr_name="Villain1", pot_type="SRP"):
    """Helper to create test hand with specified actions"""
    return {
        "hand_id": "test_hand_001",
        "hero": hero_name,
        "players": [
            {"name": hero_name, "position": "BTN"},
            {"name": pfr_name, "position": "BB"},
            {"name": "Villain2", "position": "CO"}
        ],
        "derived": {
            "preflop": {
                "pfr": pfr_name,
                "pot_type": pot_type,
                "players_to_flop": [hero_name, pfr_name],
                "is_3bet": pot_type == "3bet",
                "is_4bet": pot_type == "4bet"
            }
        },
        "streets": streets_actions
    }


class TestCBetTurn:
    """Test CBet turn scenarios IP and OOP"""
    
    def test_cbet_turn_ip_opportunity_and_attempt(self):
        """Test CBet turn IP when hero cbets flop and has opportunity on turn"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Villain1", "action": "check"},
                    {"player": "Hero", "action": "bet", "amount": 10}  # CBet flop
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Villain1", "action": "check"},  # Check to hero
                    {"player": "Hero", "action": "bet", "amount": 20}  # CBet turn
                ],
                "pot_before": 40
            }
        }, hero_name="Hero", pfr_name="Hero")
        
        result = derive_postflop(hand)
        
        assert result["cbet_turn_opp_ip"] == True  # Hero has opportunity
        assert result["cbet_turn_att_ip"] == True  # Hero attempts cbet turn
    
    def test_cbet_turn_oop_opportunity_no_attempt(self):
        """Test CBet turn OOP when hero checks"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "bet", "amount": 10},  # CBet flop OOP
                    {"player": "Villain1", "action": "call", "amount": 10}
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Hero", "action": "check"},  # No CBet turn
                    {"player": "Villain1", "action": "check"}
                ],
                "pot_before": 40
            }
        }, hero_name="Hero", pfr_name="Hero")
        
        result = derive_postflop(hand)
        
        assert result["cbet_turn_opp_oop"] == True  # Hero has opportunity
        assert result["cbet_turn_att_oop"] == False  # Hero doesn't attempt


class TestVsCBetResponses:
    """Test vs CBet responses on flop and turn"""
    
    def test_vs_cbet_flop_call_ip(self):
        """Test call vs CBet flop IP"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Villain1", "action": "bet", "amount": 10},  # CBet
                    {"player": "Hero", "action": "call", "amount": 10}  # Hero calls IP
                ],
                "pot_before": 20
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result["vs_cbet_flop_fold_ip"] == False
        assert result["vs_cbet_flop_call_ip"] == True
        assert result["vs_cbet_flop_raise_ip"] == False
    
    def test_vs_cbet_flop_raise_oop(self):
        """Test check-raise vs CBet flop OOP"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "check"},
                    {"player": "Villain1", "action": "bet", "amount": 10},  # CBet
                    {"player": "Hero", "action": "raise", "amount": 30}  # Check-raise
                ],
                "pot_before": 20
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result["vs_cbet_flop_fold_oop"] == False
        assert result["vs_cbet_flop_call_oop"] == False
        assert result["vs_cbet_flop_raise_oop"] == True
    
    def test_vs_cbet_turn_fold_oop(self):
        """Test fold vs CBet turn OOP"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "check"},
                    {"player": "Villain1", "action": "bet", "amount": 10},
                    {"player": "Hero", "action": "call", "amount": 10}
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Hero", "action": "check"},
                    {"player": "Villain1", "action": "bet", "amount": 20},  # CBet turn
                    {"player": "Hero", "action": "fold"}  # Fold vs CBet turn
                ],
                "pot_before": 40
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result.get("vs_cbet_turn_fold_oop") == True
        assert result.get("vs_cbet_turn_call_oop") == False
        assert result.get("vs_cbet_turn_raise_oop") == False


class TestProbeTurn:
    """Test probe betting on turn"""
    
    def test_probe_turn_ip_attempt(self):
        """Test probe turn IP after PFR checks flop"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Villain1", "action": "check"},  # PFR checks
                    {"player": "Hero", "action": "check"}  # Hero checks back
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Villain1", "action": "check"},
                    {"player": "Hero", "action": "bet", "amount": 15}  # Probe bet
                ],
                "pot_before": 20
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result["probe_turn_opp_ip"] == True
        assert result["probe_turn_att_ip"] == True
    
    def test_probe_turn_oop_no_attempt(self):
        """Test probe turn OOP opportunity but no attempt"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "check"},
                    {"player": "Villain1", "action": "check"}  # PFR checks
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Hero", "action": "check"},  # No probe
                    {"player": "Villain1", "action": "check"}
                ],
                "pot_before": 20
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result["probe_turn_opp_oop"] == True
        assert result["probe_turn_att_oop"] == False


class TestDelayedCBet:
    """Test delayed CBet on turn"""
    
    def test_delayed_cbet_turn_ip_attempt(self):
        """Test delayed CBet turn IP"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Villain1", "action": "check"},
                    {"player": "Hero", "action": "check"}  # Hero checks back flop
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Villain1", "action": "check"},
                    {"player": "Hero", "action": "bet", "amount": 15}  # Delayed CBet
                ],
                "pot_before": 20
            }
        }, hero_name="Hero", pfr_name="Hero")
        
        result = derive_postflop(hand)
        
        assert result["delayed_cbet_turn_opp_ip"] == True
        assert result["delayed_cbet_turn_att_ip"] == True
    
    def test_delayed_cbet_turn_oop_attempt(self):
        """Test delayed CBet turn OOP"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "check"},  # Hero checks
                    {"player": "Villain1", "action": "check"}  # Villain checks back
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Hero", "action": "bet", "amount": 15},  # Delayed CBet OOP
                    {"player": "Villain1", "action": "fold"}
                ],
                "pot_before": 20
            }
        }, hero_name="Hero", pfr_name="Hero")
        
        result = derive_postflop(hand)
        
        assert result["delayed_cbet_turn_opp_oop"] == True
        assert result["delayed_cbet_turn_att_oop"] == True


class TestDonkBetting:
    """Test donk betting"""
    
    def test_donk_flop(self):
        """Test donk bet on flop"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "bet", "amount": 10},  # Donk bet
                    {"player": "Villain1", "action": "fold"}
                ],
                "pot_before": 20
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result["donk_flop"] == True
    
    def test_donk_turn(self):
        """Test donk bet on turn"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "check"},
                    {"player": "Villain1", "action": "bet", "amount": 10},
                    {"player": "Hero", "action": "call", "amount": 10}
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Hero", "action": "bet", "amount": 20},  # Donk turn
                    {"player": "Villain1", "action": "fold"}
                ],
                "pot_before": 40
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result["donk_turn"] == True


class TestCheckRaise:
    """Test check-raise scenarios"""
    
    def test_xr_flop_opportunity_and_attempt(self):
        """Test check-raise flop"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "check"},
                    {"player": "Villain1", "action": "bet", "amount": 10},
                    {"player": "Hero", "action": "raise", "amount": 30}  # Check-raise
                ],
                "pot_before": 20
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result["xr_flop_opp"] == True
        assert result["xr_flop_att"] == True
    
    def test_xr_turn_opportunity_no_attempt(self):
        """Test check-raise turn opportunity but no attempt"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "check"},
                    {"player": "Villain1", "action": "bet", "amount": 10},
                    {"player": "Hero", "action": "call", "amount": 10}
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Hero", "action": "check"},
                    {"player": "Villain1", "action": "bet", "amount": 20},
                    {"player": "Hero", "action": "call", "amount": 20}  # Just call, no raise
                ],
                "pot_before": 40
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result.get("xr_turn_opp") == True
        assert result.get("xr_turn_att") == False


class TestMultiwayScenarios:
    """Test multiway postflop scenarios"""
    
    def test_multiway_flags_disable_stats(self):
        """Test that multiway flags properly disable certain stats"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Villain2", "action": "check"},
                    {"player": "Hero", "action": "bet", "amount": 10},
                    {"player": "Villain1", "action": "call", "amount": 10},
                    {"player": "Villain2", "action": "fold"}
                ],
                "pot_before": 30
            }
        }, hero_name="Hero", pfr_name="Hero")
        
        # Add third player to flop
        hand["derived"]["preflop"]["players_to_flop"] = ["Hero", "Villain1", "Villain2"]
        
        result = derive_postflop(hand)
        
        # CBet stats should still work in multiway
        assert "cbet_flop_opp_oop" in result
        
        # Check multiway flag
        assert result.get("is_multiway_flop") == True


class TestBetVsMissedCBet:
    """Test bet vs missed CBet scenarios"""
    
    def test_bet_vs_miss_turn_attempt(self):
        """Test bet vs missed CBet on turn"""
        hand = create_test_hand({
            "flop": {
                "actions": [
                    {"player": "Villain1", "action": "check"},  # PFR checks
                    {"player": "Hero", "action": "check"}
                ],
                "pot_before": 20
            },
            "turn": {
                "actions": [
                    {"player": "Villain1", "action": "check"},  # PFR checks again
                    {"player": "Hero", "action": "bet", "amount": 15}  # Bet vs missed
                ],
                "pot_before": 20
            }
        }, hero_name="Hero", pfr_name="Villain1")
        
        result = derive_postflop(hand)
        
        assert result.get("bet_vs_miss_turn_opp") == True
        assert result.get("bet_vs_miss_turn_att") == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])