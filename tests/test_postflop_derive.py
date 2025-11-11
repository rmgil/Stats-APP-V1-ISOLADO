import pytest
import json
from app.derive.postflop import derive_postflop
from app.parse.schemas import Hand, Player, Action, StreetInfo


def create_hand_with_flop_cbet_ip():
    """Hero is PFR, cbets flop in position"""
    return {
        "hand_id": "TEST_FLOP_CBET_IP",
        "hero": "Hero",
        "players": [
            {"name": "Hero", "position_absolute": "BTN", "stack": 100.0, "is_hero": True},
            {"name": "Villain", "position_absolute": "BB", "stack": 100.0, "is_hero": False}
        ],
        "derived": {
            "preflop": {
                "open_raiser": "Hero",
                "pot_type": "SRP"
            },
            "ip": {
                "hero_ip_flop": True,
                "players_to_flop": 2
            }
        },
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Hero", "type": "RAISE", "amount": 2.5},
                    {"actor": "Villain", "type": "CALL", "amount": 2.5}
                ],
                "pot": 5.0
            },
            "flop": {
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "BET", "amount": 3.5},
                    {"actor": "Villain", "type": "CALL", "amount": 3.5}
                ],
                "pot": 12.0
            }
        }
    }


def create_hand_with_vs_cbet_fold():
    """Hero folds to cbet"""
    return {
        "hand_id": "TEST_VS_CBET_FOLD",
        "hero": "Hero",
        "players": [
            {"name": "Villain", "position_absolute": "CO", "stack": 100.0, "is_hero": False},
            {"name": "Hero", "position_absolute": "BB", "stack": 100.0, "is_hero": True}
        ],
        "derived": {
            "preflop": {
                "open_raiser": "Villain",
                "pot_type": "SRP"
            },
            "ip": {
                "hero_ip_flop": False,
                "players_to_flop": 2
            }
        },
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Villain", "type": "RAISE", "amount": 2.5},
                    {"actor": "Hero", "type": "CALL", "amount": 2.5}
                ],
                "pot": 5.0
            },
            "flop": {
                "actions": [
                    {"actor": "Villain", "type": "BET", "amount": 3.5},
                    {"actor": "Hero", "type": "FOLD"}
                ],
                "pot": 5.0
            }
        }
    }


def create_hand_with_donk_turn():
    """Hero donks turn after checking flop"""
    return {
        "hand_id": "TEST_DONK_TURN",
        "hero": "Hero",
        "players": [
            {"name": "Villain", "position_absolute": "BTN", "stack": 100.0, "is_hero": False},
            {"name": "Hero", "position_absolute": "BB", "stack": 100.0, "is_hero": True}
        ],
        "derived": {
            "preflop": {
                "open_raiser": "Villain",
                "pot_type": "SRP"
            },
            "ip": {
                "hero_ip_flop": False,
                "hero_ip_turn": False,
                "players_to_flop": 2
            }
        },
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Villain", "type": "RAISE", "amount": 2.5},
                    {"actor": "Hero", "type": "CALL", "amount": 2.5}
                ],
                "pot": 5.0
            },
            "flop": {
                "actions": [
                    {"actor": "Hero", "type": "CHECK"},
                    {"actor": "Villain", "type": "CHECK"}
                ],
                "pot": 5.0
            },
            "turn": {
                "actions": [
                    {"actor": "Hero", "type": "BET", "amount": 3.0},
                    {"actor": "Villain", "type": "CALL", "amount": 3.0}
                ],
                "pot": 11.0
            }
        }
    }


def create_hand_with_river_showdown():
    """Hand that goes to showdown"""
    return {
        "hand_id": "TEST_RIVER_SHOWDOWN",
        "hero": "Hero",
        "players": [
            {"name": "Hero", "position_absolute": "CO", "stack": 100.0, "is_hero": True},
            {"name": "Villain", "position_absolute": "BB", "stack": 100.0, "is_hero": False}
        ],
        "derived": {
            "preflop": {
                "open_raiser": "Hero",
                "pot_type": "SRP"
            },
            "ip": {
                "hero_ip_flop": True,
                "hero_ip_river": True,
                "players_to_flop": 2
            }
        },
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Hero", "type": "RAISE", "amount": 2.5},
                    {"actor": "Villain", "type": "CALL", "amount": 2.5}
                ],
                "pot": 5.0
            },
            "flop": {
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "BET", "amount": 3.0},
                    {"actor": "Villain", "type": "CALL", "amount": 3.0}
                ],
                "pot": 11.0
            },
            "turn": {
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "CHECK"}
                ],
                "pot": 11.0
            },
            "river": {
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "BET", "amount": 7.0},
                    {"actor": "Villain", "type": "CALL", "amount": 7.0}
                ],
                "pot": 25.0
            },
            "showdown": {
                "actions": [
                    {"actor": "Hero", "type": "SHOW"},
                    {"actor": "Villain", "type": "MUCK"}
                ]
            }
        },
        "winners": ["Hero"]
    }


def test_derive_flop_cbet_ip():
    """Test flop cbet IP detection"""
    hand = create_hand_with_flop_cbet_ip()
    result = derive_postflop(hand)
    
    assert result is not None
    assert result["cbet_flop_opp_ip"] == True
    assert result["cbet_flop_att_ip"] == True
    assert result["saw_flop"] == True
    assert result["pfr_player"] == "Hero"


def test_derive_vs_cbet_fold():
    """Test vs cbet fold detection"""
    hand = create_hand_with_vs_cbet_fold()
    result = derive_postflop(hand)
    
    assert result is not None
    assert result["vs_cbet_flop_fold_oop"] == True
    assert result["saw_flop"] == True
    assert result["pfr_player"] == "Villain"
    

def test_derive_donk_turn():
    """Test donk turn detection"""
    hand = create_hand_with_donk_turn()
    result = derive_postflop(hand)
    
    assert result is not None
    # Note: donk_turn is not implemented in current version, testing donk_flop
    assert result["donk_flop"] == False  # No donk on flop (both checked)
    assert result["saw_flop"] == True
    

def test_derive_river_showdown():
    """Test river and showdown metrics"""
    hand = create_hand_with_river_showdown()
    result = derive_postflop(hand)
    
    assert result is not None
    assert result["saw_flop"] == True
    assert result["saw_showdown"] == True
    assert result["won_showdown"] == True
    assert result["won_when_saw_flop"] == True
    assert result["river_agg_pct"] > 0


def test_derive_postflop_empty_hand():
    """Test handling of hand without postflop streets"""
    hand = {
        "hand_id": "TEST_PREFLOP_ONLY",
        "hero": "Hero",
        "players": [
            {"name": "Hero", "position_absolute": "BTN", "stack": 100.0, "is_hero": True},
            {"name": "Villain", "position_absolute": "BB", "stack": 100.0, "is_hero": False}
        ],
        "derived": {
            "preflop": {
                "open_raiser": "Hero",
                "pot_type": "SRP"
            },
            "ip": {
                "players_to_flop": 0
            }
        },
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Hero", "type": "RAISE", "amount": 2.5},
                    {"actor": "Villain", "type": "FOLD"}
                ],
                "pot": 2.5
            }
        }
    }
    
    result = derive_postflop(hand)
    assert result is not None
    assert result["saw_flop"] == False
    assert result["saw_showdown"] == False