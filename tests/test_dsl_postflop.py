import pytest
import json
from pathlib import Path
from app.stats.engine import run_stats
from app.derive.runner import enrich_hands


def create_test_hands():
    """Create test hands with various postflop scenarios"""
    hands = [
        # Hand 1: Hero cbets flop IP, turn cbet, river aggression
        {
            "hand_id": "DSL_TEST_1",
            "hero": "Hero",
            "site": "pokerstars",
            "tournament_id": "TEST",
            "table_size": 6,
            "players": [
                {"name": "Hero", "position_absolute": "BTN", "stack": 100.0, "is_hero": True},
                {"name": "Villain", "position_absolute": "BB", "stack": 100.0, "is_hero": False}
            ],
            "preflop": {
                "actions": [
                    {"player": "Hero", "action": "R", "amount": 2.5},
                    {"player": "Villain", "action": "C", "amount": 2.5}
                ],
                "pfr": "Hero",
                "pot": 5.0,
                "effective_stack": 97.5
            },
            "flop": {
                "actions": [
                    {"player": "Villain", "action": "X"},
                    {"player": "Hero", "action": "B", "amount": 3.5},
                    {"player": "Villain", "action": "C", "amount": 3.5}
                ],
                "pot": 12.0
            },
            "turn": {
                "actions": [
                    {"player": "Villain", "action": "X"},
                    {"player": "Hero", "action": "B", "amount": 8.0},
                    {"player": "Villain", "action": "F"}
                ],
                "pot": 12.0
            },
            "winners": ["Hero"]
        },
        # Hand 2: Hero vs cbet - raise flop IP
        {
            "hand_id": "DSL_TEST_2",
            "hero": "Hero",
            "site": "pokerstars",
            "tournament_id": "TEST",
            "table_size": 6,
            "players": [
                {"name": "Villain", "position_absolute": "CO", "stack": 100.0, "is_hero": False},
                {"name": "Hero", "position_absolute": "BTN", "stack": 100.0, "is_hero": True}
            ],
            "preflop": {
                "actions": [
                    {"player": "Villain", "action": "R", "amount": 2.5},
                    {"player": "Hero", "action": "C", "amount": 2.5}
                ],
                "pfr": "Villain",
                "pot": 5.5,
                "effective_stack": 97.5
            },
            "flop": {
                "actions": [
                    {"player": "Villain", "action": "B", "amount": 3.5},
                    {"player": "Hero", "action": "R", "amount": 10.0},
                    {"player": "Villain", "action": "F"}
                ],
                "pot": 5.5
            },
            "winners": ["Hero"]
        },
        # Hand 3: Bet vs missed cbet flop
        {
            "hand_id": "DSL_TEST_3",
            "hero": "Hero",
            "site": "pokerstars",
            "tournament_id": "TEST",
            "table_size": 6,
            "players": [
                {"name": "Villain", "position_absolute": "BTN", "stack": 100.0, "is_hero": False},
                {"name": "Hero", "position_absolute": "BB", "stack": 100.0, "is_hero": True}
            ],
            "preflop": {
                "actions": [
                    {"player": "Villain", "action": "R", "amount": 2.5},
                    {"player": "Hero", "action": "C", "amount": 2.5}
                ],
                "pfr": "Villain",
                "pot": 5.0,
                "effective_stack": 97.5
            },
            "flop": {
                "actions": [
                    {"player": "Hero", "action": "X"},
                    {"player": "Villain", "action": "X"}
                ],
                "pot": 5.0
            },
            "turn": {
                "actions": [
                    {"player": "Hero", "action": "B", "amount": 3.0},
                    {"player": "Villain", "action": "F"}
                ],
                "pot": 5.0
            },
            "winners": ["Hero"]
        },
        # Hand 4: Goes to showdown
        {
            "hand_id": "DSL_TEST_4",
            "hero": "Hero",
            "site": "pokerstars",
            "tournament_id": "TEST",
            "table_size": 6,
            "players": [
                {"name": "Hero", "position_absolute": "CO", "stack": 100.0, "is_hero": True},
                {"name": "Villain", "position_absolute": "BB", "stack": 100.0, "is_hero": False}
            ],
            "preflop": {
                "actions": [
                    {"player": "Hero", "action": "R", "amount": 2.5},
                    {"player": "Villain", "action": "C", "amount": 2.5}
                ],
                "pfr": "Hero",
                "pot": 5.0,
                "effective_stack": 97.5
            },
            "flop": {
                "actions": [
                    {"player": "Villain", "action": "X"},
                    {"player": "Hero", "action": "X"}
                ],
                "pot": 5.0
            },
            "turn": {
                "actions": [
                    {"player": "Villain", "action": "X"},
                    {"player": "Hero", "action": "X"}
                ],
                "pot": 5.0
            },
            "river": {
                "actions": [
                    {"player": "Villain", "action": "X"},
                    {"player": "Hero", "action": "X"}
                ],
                "pot": 5.0
            },
            "winners": ["Hero"],
            "showdown": True
        }
    ]
    
    return hands


def test_postflop_stats_generation(tmp_path):
    """Test that postflop stats are correctly generated from DSL"""
    
    # Create test hands
    hands = create_test_hands()
    
    # Write hands to temp file
    hands_file = tmp_path / "test_hands.jsonl"
    with open(hands_file, 'w') as f:
        for hand in hands:
            f.write(json.dumps(hand) + '\n')
    
    # Run derive to enrich hands
    enriched_file = tmp_path / "test_hands_enriched.jsonl"
    enrich_hands(str(hands_file), str(enriched_file))
    
    # Run stats engine
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir(exist_ok=True)
    
    result = run_stats(
        str(enriched_file),
        "app/stats/dsl/stats.yml", 
        str(stats_dir)
    )
    
    # Verify stats were generated
    assert result["success"] == True
    assert result["hands_processed"] == 4
    
    # Load stat counts
    stat_counts_file = stats_dir / "stat_counts.json"
    assert stat_counts_file.exists()
    
    with open(stat_counts_file) as f:
        counts = json.load(f)
    
    # Check specific postflop stats exist
    assert "POST_CBET_FLOP_IP" in counts["stats"]["postflop"]
    assert "POST_VS_CBET_FLOP_RAISE_IP" in counts["stats"]["postflop"]
    assert "POST_BET_TURN_VS_MISSED_FLOP_CBET_OOP_SRP" in counts["stats"]["postflop"]
    assert "POST_WTSD" in counts["stats"]["postflop"]
    assert "POST_W$SD" in counts["stats"]["postflop"]
    

def test_postflop_stat_opportunities_attempts(tmp_path):
    """Test specific opportunity and attempt counts for key stats"""
    
    # Create test hands
    hands = create_test_hands()
    
    # Write hands to temp file
    hands_file = tmp_path / "test_hands.jsonl"
    with open(hands_file, 'w') as f:
        for hand in hands:
            f.write(json.dumps(hand) + '\n')
    
    # Run derive to enrich hands
    enriched_file = tmp_path / "test_hands_enriched.jsonl"
    enrich_hands(str(hands_file), str(enriched_file))
    
    # Run stats engine
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir(exist_ok=True)
    
    run_stats(
        str(enriched_file),
        "app/stats/dsl/stats.yml", 
        str(stats_dir)
    )
    
    # Load stat counts
    with open(stats_dir / "stat_counts.json") as f:
        counts = json.load(f)
    
    postflop_stats = counts["stats"]["postflop"]["all"]
    
    # Verify POST_CBET_FLOP_IP (Hand 1 has opportunity and attempt)
    if "POST_CBET_FLOP_IP" in postflop_stats:
        stat = postflop_stats["POST_CBET_FLOP_IP"]
        assert stat["opportunities"] >= 1
        assert stat["attempts"] >= 1
    
    # Verify POST_VS_CBET_FLOP_RAISE_IP (Hand 2 has opportunity and attempt)
    if "POST_VS_CBET_FLOP_RAISE_IP" in postflop_stats:
        stat = postflop_stats["POST_VS_CBET_FLOP_RAISE_IP"]
        assert stat["opportunities"] >= 1
        assert stat["attempts"] >= 1
    
    # Verify POST_WTSD (Hands that saw flop)
    if "POST_WTSD" in postflop_stats:
        stat = postflop_stats["POST_WTSD"]
        assert stat["opportunities"] >= 3  # At least 3 hands saw flop
        assert stat["attempts"] >= 1       # At least 1 went to showdown
    

def test_postflop_filters_effective_stack(tmp_path):
    """Test that effective stack filters work correctly"""
    
    # Create a hand with low effective stack
    low_stack_hand = {
        "hand_id": "LOW_STACK_TEST",
        "hero": "Hero",
        "site": "pokerstars",
        "tournament_id": "TEST",
        "table_size": 6,
        "players": [
            {"name": "Hero", "position_absolute": "BTN", "stack": 10.0, "is_hero": True},
            {"name": "Villain", "position_absolute": "BB", "stack": 100.0, "is_hero": False}
        ],
        "preflop": {
            "actions": [
                {"player": "Hero", "action": "R", "amount": 2.5},
                {"player": "Villain", "action": "C", "amount": 2.5}
            ],
            "pfr": "Hero",
            "pot": 5.0,
            "effective_stack": 7.5  # Less than 16bb minimum
        },
        "flop": {
            "actions": [
                {"player": "Villain", "action": "X"},
                {"player": "Hero", "action": "B", "amount": 3.5},
                {"player": "Villain", "action": "F"}
            ],
            "pot": 5.0
        },
        "winners": ["Hero"]
    }
    
    # Write hand to temp file
    hands_file = tmp_path / "test_low_stack.jsonl"
    with open(hands_file, 'w') as f:
        f.write(json.dumps(low_stack_hand) + '\n')
    
    # Run derive to enrich hands
    enriched_file = tmp_path / "test_low_stack_enriched.jsonl"
    enrich_hands(str(hands_file), str(enriched_file))
    
    # Run stats engine
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir(exist_ok=True)
    
    run_stats(
        str(enriched_file),
        "app/stats/dsl/stats.yml", 
        str(stats_dir)
    )
    
    # Load stat counts
    with open(stats_dir / "stat_counts.json") as f:
        counts = json.load(f)
    
    # Postflop stats should be filtered out due to low effective stack
    postflop_stats = counts["stats"].get("postflop", {}).get("all", {})
    
    # Stats should not have opportunities due to eff_stack_min_bb filter
    if "POST_CBET_FLOP_IP" in postflop_stats:
        stat = postflop_stats["POST_CBET_FLOP_IP"]
        assert stat.get("opportunities", 0) == 0