"""
Tests for partition module groups, months, and hand ID generation.
"""
from app.partition.groups import groups_for_hand
from app.partition.months import month_bucket, make_hand_id


def test_groups_basic():
    """Test basic group assignment for non-KO 9max without flop."""
    hand = {
        "file_id": "/runs/CLASSIFIED/non-KO/t1.txt",
        "timestamp_utc": "2025-06-20T12:00:00Z",
        "derived": {"positions": {"table_max_resolved": 9}, "ip": {"players_to_flop": 0}}
    }
    assert "nonko_9max_pref" in groups_for_hand(hand)
    assert "postflop_all" not in groups_for_hand(hand)


def test_groups_pko_postflop():
    """Test PKO 6max hand that saw flop."""
    hand = {
        "file_id": "/runs/CLASSIFIED/PKO/t2.txt",
        "timestamp_utc": "2025-06-20T12:00:00Z",
        "derived": {"positions": {"table_max_resolved": 6}, "ip": {"players_to_flop": 3}}
    }
    g = groups_for_hand(hand)
    assert "pko_pref" in g
    assert "postflop_all" in g


def test_edge_no_derived():
    """Test edge case with no derived data."""
    hand = {"timestamp_utc": "2025-01-01T00:00:00Z"}
    assert groups_for_hand(hand) == []


def test_month_bucket_dst():
    """Test month bucket with DST transition."""
    # 23:30Z em 30/06 vira 00:30 em PT (muda para Julho)
    assert month_bucket("2025-06-30T23:30:00Z") == "2025-07"


def test_hand_id_deterministic():
    """Test that hand ID is deterministic regardless of player order."""
    h1 = {
        "site": "pokerstars",
        "tournament_id": "123",
        "file_id": "a",
        "button_seat": 4,
        "timestamp_utc": "2025-01-01T00:00:00Z",
        "players": [{"name": "A"}, {"name": "B"}]
    }
    h2 = {
        "site": "pokerstars",
        "tournament_id": "123",
        "file_id": "a",
        "button_seat": 4,
        "timestamp_utc": "2025-01-01T00:00:00Z",
        "players": [{"name": "B"}, {"name": "A"}]
    }
    assert make_hand_id(h1) == make_hand_id(h2)


# Additional edge cases
def test_month_bucket_edge_cases():
    """Test various edge cases for month_bucket."""
    # Empty string
    assert month_bucket("") == "unknown"
    # Invalid date
    assert month_bucket("invalid-date") == "unknown"
    # None (simulated as empty)
    assert month_bucket("") == "unknown"
    # New Year's Eve crossing
    assert month_bucket("2024-12-31T23:30:00Z") == "2024-12"  # Still December in PT


def test_groups_edge_cases():
    """Test edge cases for groups_for_hand."""
    # Missing table_max
    hand = {
        "file_id": "data/pko/test.txt",
        "derived": {"positions": {}, "ip": {"players_to_flop": 2}}
    }
    groups = groups_for_hand(hand)
    assert "pko_pref" in groups  # PKO detected from file_id
    assert "postflop_all" in groups  # Saw flop
    
    # Mystery tournament (defaults to non-ko behavior)
    hand = {
        "file_id": "data/mystery/test.txt",
        "table_max": 6,
        "derived": {"positions": {"table_max_resolved": 6}, "ip": {"players_to_flop": 0}}
    }
    groups = groups_for_hand(hand)
    # Mystery defaults to non-ko
    assert "nonko_6max_pref" not in groups  # Mystery is not non-ko
    
    # Explicit tourney_class overrides file path
    hand = {
        "tourney_class": "PKO",
        "file_id": "regular.txt",  # Non-PKO path
        "table_max": 9,
        "derived": {"positions": {"table_max_resolved": 9}, "ip": {"players_to_flop": 2}}
    }
    groups = groups_for_hand(hand)
    assert "pko_pref" in groups  # Explicit class wins
    assert "postflop_all" in groups


def test_hand_id_edge_cases():
    """Test edge cases for make_hand_id."""
    # Missing fields
    h1 = {"site": "pokerstars"}
    h2 = {"site": "pokerstars"}
    assert make_hand_id(h1) == make_hand_id(h2)
    assert len(make_hand_id(h1)) == 16
    
    # Different raw_offsets
    h1 = {
        "site": "ps",
        "raw_offsets": {"hand_start": 100},
        "players": [{"name": "A"}]
    }
    h2 = {
        "site": "ps",
        "raw_offsets": {"hand_start": 200},
        "players": [{"name": "A"}]
    }
    assert make_hand_id(h1) != make_hand_id(h2)
    
    # Empty players list
    h1 = {"site": "ps", "players": []}
    h2 = {"site": "ps", "players": None}
    # Both should handle gracefully
    id1 = make_hand_id(h1)
    id2 = make_hand_id(h2)
    assert len(id1) == 16
    assert len(id2) == 16


if __name__ == "__main__":
    # Run all tests
    test_functions = [
        test_groups_basic,
        test_groups_pko_postflop,
        test_edge_no_derived,
        test_month_bucket_dst,
        test_hand_id_deterministic,
        test_month_bucket_edge_cases,
        test_groups_edge_cases,
        test_hand_id_edge_cases
    ]
    
    for test_func in test_functions:
        try:
            test_func()
            print(f"✓ {test_func.__name__}")
        except AssertionError as e:
            print(f"✗ {test_func.__name__}: {e}")
    
    print("\n✅ All tests completed!")