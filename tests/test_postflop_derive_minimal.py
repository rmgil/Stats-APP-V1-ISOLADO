import json
from app.derive.postflop import derive_postflop

def make_hand_flop_cbet_ip():
    # Hero é PFR, flop heads-up, villain check, hero bet ⇒ CBet IP
    return {
        "hero": "Hero",
        "players": [{"name":"Hero"},{"name":"Villain"}],
        "derived": {
            "preflop": {"open_raiser":"Hero","pot_type":"SRP"},
            "ip": {"hero_ip_flop": True, "players_to_flop": 2}
        },
        "streets": {
            "preflop": {"actions":[{"actor":"Hero","type":"RAISE"},{"actor":"Villain","type":"CALL"}]},
            "flop":    {"actions":[{"actor":"Villain","type":"CHECK"},{"actor":"Hero","type":"BET","amount":2.0}]}
        }
    }

def make_hand_vs_cbet_fold_ip():
    # Villain é PFR e aposta primeiro no flop; Hero IP e faz FOLD
    return {
        "hero": "Hero",
        "players": [{"name":"Hero"},{"name":"Villain"}],
        "derived": {
            "preflop": {"open_raiser":"Villain","pot_type":"SRP"},
            "ip": {"hero_ip_flop": True, "players_to_flop": 2}
        },
        "streets": {
            "preflop": {"actions":[{"actor":"Villain","type":"RAISE"},{"actor":"Hero","type":"CALL"}]},
            "flop":    {"actions":[{"actor":"Villain","type":"BET","amount":2.0},{"actor":"Hero","type":"FOLD"}]}
        }
    }

def test_cbet_flop_ip():
    hand = make_hand_flop_cbet_ip()
    d = derive_postflop(hand)
    assert d["cbet_flop_opp_ip"] is True
    assert d["cbet_flop_att_ip"] is True
    assert d["donk_flop"] is False

def test_vs_cbet_fold_ip():
    hand = make_hand_vs_cbet_fold_ip()
    d = derive_postflop(hand)
    assert d["vs_cbet_flop_fold_ip"] is True
    assert d["vs_cbet_flop_raise_ip"] is False
    assert d["cbet_flop_opp_ip"] is False

def test_wtsd_flag():
    hand = make_hand_flop_cbet_ip()
    # adiciona showdown 'SHOW' para disparar WTSD
    hand["streets"]["showdown"] = {"actions":[{"actor":"Hero","type":"SHOW"}]}
    d = derive_postflop(hand)
    assert d["saw_flop"] is True
    assert d["saw_showdown"] is True