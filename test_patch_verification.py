#!/usr/bin/env python3
"""
Verify that the patch is actually being applied correctly
"""
import tempfile
from app.stats.preflop_stats import PreflopStats
from app.stats.hand_collector import HandCollector
from app.stats.preflop_stats_multisite import patch_preflop_stats, MultiSitePreflopExtractor

pokerstars_pko_hand = """PokerStars Hand #256955611604: Tournament #3908028211, $10+$10+$2 USD Hold'em No Limit - Level I (25/50) - 2025/07/15 10:35:55 BRT [2025/07/15 9:35:55 ET]
Table '3908028211 28' 8-max Seat #1 is the button
Seat 1: chanse04 (5000 in chips, $10 bounty) 
Seat 2: gleb4ik13 (5209 in chips, $10 bounty) 
Seat 3: euphoria rivera (4899 in chips, $10 bounty) 
Seat 5: Gabrielfs13 (5000 in chips, $10 bounty) 
Seat 7: bianchettibh (5000 in chips, $10 bounty) 
chanse04: posts the ante 8
gleb4ik13: posts the ante 8
euphoria rivera: posts the ante 8
Gabrielfs13: posts the ante 8
bianchettibh: posts the ante 8
gleb4ik13: posts small blind 25
euphoria rivera: posts big blind 50
*** HOLE CARDS ***
Dealt to Gabrielfs13 [Kd 7c]
Gabrielfs13: folds 
bianchettibh: raises 60 to 110
chanse04: raises 385 to 495
gleb4ik13: folds 
euphoria rivera: folds 
bianchettibh: calls 385
*** FLOP *** [8h Js 9c]
bianchettibh: checks 
chanse04: bets 855
bianchettibh: folds 
Uncalled bet (855) returned to chanse04
chanse04 collected 1105 from pot
*** SUMMARY ***
Total pot 1105 | Rake 0 
Board [8h Js 9c]
Seat 1: chanse04 (button) collected (1105)
Seat 2: gleb4ik13 (small blind) folded before Flop
Seat 3: euphoria rivera (big blind) folded before Flop
Seat 5: Gabrielfs13 folded before Flop (didn't bet)
Seat 7: bianchettibh folded on the Flop"""

print("=" * 80)
print("VERIFYING PATCH APPLICATION")
print("=" * 80)

# Create PreflopStats instance
with tempfile.TemporaryDirectory() as tmpdir:
    hand_collector = HandCollector(tmpdir)
    calc = PreflopStats(hand_collector=hand_collector)
    
    print("\n1. BEFORE PATCH:")
    print("-" * 80)
    print(f"   _extract_hand_id method: {calc._extract_hand_id}")
    print(f"   _extract_positions method: {calc._extract_positions}")
    print(f"   _extract_preflop_actions method: {calc._extract_preflop_actions}")
    
    # Test methods before patch
    hand_id_before = calc._extract_hand_id(pokerstars_pko_hand)
    positions_before = calc._extract_positions(pokerstars_pko_hand)
    actions_before = calc._extract_preflop_actions(pokerstars_pko_hand)
    
    print(f"\n   Testing on PokerStars PKO hand:")
    print(f"   - Hand ID: {hand_id_before}")
    print(f"   - Positions: {len(positions_before)} found: {positions_before}")
    print(f"   - Actions: {len(actions_before)} found")
    
    # Apply patch
    print("\n\n2. APPLYING PATCH...")
    print("-" * 80)
    patch_preflop_stats()
    
    # Create new instance AFTER patch
    calc2 = PreflopStats(hand_collector=hand_collector)
    
    print("\n3. AFTER PATCH:")
    print("-" * 80)
    print(f"   _extract_hand_id method: {calc2._extract_hand_id}")
    print(f"   _extract_positions method: {calc2._extract_positions}")
    print(f"   _extract_preflop_actions method: {calc2._extract_preflop_actions}")
    
    # Test methods after patch
    hand_id_after = calc2._extract_hand_id(pokerstars_pko_hand)
    positions_after = calc2._extract_positions(pokerstars_pko_hand)
    actions_after = calc2._extract_preflop_actions(pokerstars_pko_hand)
    
    print(f"\n   Testing on PokerStars PKO hand:")
    print(f"   - Hand ID: {hand_id_after}")
    print(f"   - Positions: {len(positions_after)} found: {positions_after}")
    print(f"   - Actions: {len(actions_after)} found")
    for i, action in enumerate(actions_after[:3], 1):
        print(f"      {i}. {action}")
    
    print("\n\n4. COMPARISON:")
    print("-" * 80)
    print(f"   Hand ID changed: {hand_id_before != hand_id_after}")
    print(f"   Positions changed: {positions_before != positions_after}")
    print(f"   Actions changed: {len(actions_before) != len(actions_after)}")
    
    if len(positions_after) > 0 and len(actions_after) > 0:
        print("\n   ✅ PATCH APPLIED SUCCESSFULLY!")
        print("   Now testing analyze_hand()...")
        
        calc2.analyze_hand(pokerstars_pko_hand)
        stats = calc2.get_stats_summary()
        
        stats_with_opps = {k: v for k, v in stats.items() if v.get('opportunities', 0) > 0}
        print(f"\n   analyze_hand() generated {len(stats_with_opps)} stats with opportunities")
        
        if len(stats_with_opps) == 0:
            print("   ❌ PROBLEM: Patch works but analyze_hand() still generates 0 stats!")
            print("   The issue must be in analyze_hand() logic, not the extractor!")
        else:
            print("   ✅ SUCCESS: Stats being generated!")
    else:
        print("\n   ❌ PATCH FAILED - positions/actions still empty!")
