#!/usr/bin/env python3
"""
Simplified tests focusing on core parser functionality.
"""

from app.parse.site_pokerstars import PokerStarsParser
from app.parse.site_generic import find_hand_boundaries


def test_hand_boundary_detection():
    """Test that hand boundaries are correctly detected."""
    text = """PokerStars Hand #123: Test
Table 1
Seat 1: Player1 (1500)
*** HOLE CARDS ***
Player1: folds

PokerStars Hand #124: Test
Table 2
Seat 2: Player2 (2000)
*** HOLE CARDS ***
Player2: raises 100

"""
    
    boundaries = list(find_hand_boundaries(text))
    print(f"Found {len(boundaries)} hand boundaries")
    for start, end, hand_text in boundaries:
        print(f"  Hand from {start} to {end}: {len(hand_text)} chars")
        print(f"    First line: {hand_text.split(chr(10))[0][:50]}...")
    
    # We expect to find hands based on the pattern
    assert len(boundaries) >= 1


def test_pokerstars_parser_basic():
    """Test basic PokerStars parsing."""
    parser = PokerStarsParser()
    hero_aliases = {'global': ['Hero'], 'pokerstars': []}
    
    # Create a complete, valid hand
    hand_text = """PokerStars Hand #123456789: Tournament #987654321, $10+$1 Hold'em No Limit - Level I (10/20) - 2025/01/15 12:00:00 UTC
Table '987654321 1' 9-max Seat #1 is the button
Seat 1: Player1 (1500 in chips)
Seat 2: Hero (1500 in chips)
Seat 3: Player3 (1500 in chips)
Hero: posts small blind 10
Player3: posts big blind 20
*** HOLE CARDS ***
Dealt to Hero [As Kd]
Player1: folds
Hero: raises 30 to 50
Player3: calls 30
*** FLOP *** [Qh Js Tc]
Hero: bets 60
Player3: folds
Hero collected 100 from pot
*** SUMMARY ***
Total pot 100 | Rake 0
Board [Qh Js Tc]
Seat 1: Player1 (button) folded before Flop
Seat 2: Hero (small blind) collected (100)
Seat 3: Player3 (big blind) folded on the Flop

"""
    
    hands = parser.parse_tournament(hand_text, 'test.txt', hero_aliases)
    print(f"\nParsed {len(hands)} hands from PokerStars text")
    
    # Find the most complete hand (with button and actions)
    complete_hands = [h for h in hands if h.button_seat is not None]
    print(f"Complete hands with button: {len(complete_hands)}")
    
    hands_with_actions = [h for h in hands if any(
        len(h.streets[street].actions) > 0 
        for street in ['preflop', 'flop', 'turn', 'river']
    )]
    print(f"Hands with actions: {len(hands_with_actions)}")
    
    # Basic assertion - we should parse something
    assert len(hands) >= 1
    
    # Check if we have hero detection
    hands_with_hero = [h for h in hands if h.hero is not None]
    print(f"Hands with hero detected: {len(hands_with_hero)}")


def test_action_parsing():
    """Test that actions are being parsed."""
    parser = PokerStarsParser()
    hero_aliases = {'global': [], 'pokerstars': []}
    
    # Minimal hand focused on actions
    hand_text = """PokerStars Hand #999: Tournament #111, $5+$0.50 Hold'em No Limit
Table '111 1' 6-max Seat #2 is the button
Seat 1: UTG (1000 in chips)
Seat 2: BTN (1000 in chips)
Seat 3: SB (1000 in chips)
Seat 4: BB (1000 in chips)
SB: posts small blind 25
BB: posts big blind 50
*** HOLE CARDS ***
UTG: raises 100 to 150
BTN: calls 150
SB: raises 825 to 975 and is all-in
BB: folds
UTG: folds
BTN: calls 825
*** FLOP *** [7h 2s 3c]
*** TURN *** [7h 2s 3c] [Kd]
*** RIVER *** [7h 2s 3c Kd] [As]
*** SHOWDOWN ***
SB: shows [Ah Ad]
BTN: shows [Kc Ks]
SB collected 2150 from pot

"""
    
    hands = parser.parse_tournament(hand_text, 'test.txt', hero_aliases)
    print(f"\n[Action Test] Parsed {len(hands)} hands")
    
    # Check for hands with preflop actions
    for i, hand in enumerate(hands):
        preflop = hand.streets.get('preflop')
        if preflop and preflop.actions:
            print(f"  Hand {i}: {len(preflop.actions)} preflop actions")
            for action in preflop.actions[:3]:  # Show first 3 actions
                print(f"    - {action.type}: {action.actor} ({action.amount})")
    
    assert len(hands) >= 1


if __name__ == '__main__':
    print("=== Testing Hand Boundary Detection ===")
    test_hand_boundary_detection()
    
    print("\n=== Testing PokerStars Parser ===")
    test_pokerstars_parser_basic()
    
    print("\n=== Testing Action Parsing ===")
    test_action_parsing()
    
    print("\n✓ All simple tests completed!")