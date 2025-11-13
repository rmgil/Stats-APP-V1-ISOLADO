"""Test PKO detection in ZIP import for different networks."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from pathlib import Path
import tempfile
import zipfile
from app.mtt_import.detectors import detect_tourney_type

# PokerStars PKO fixture with X+Y+fee format
POKERSTARS_PKO_HH = """PokerStars Hand #123456789: Tournament #987654321, €3.37+€3.38+€0.75 EUR Hold'em No Limit - Level I (10/20) - 2024/01/15 20:00:00 CET
Table '987654321 1' 6-max Seat #1 is the button
Seat 1: Player1 (1500 in chips)
Seat 2: Player2 (1500 in chips)
Seat 3: Player3 (1500 in chips)
Seat 4: Player4 (1500 in chips)
Seat 5: Player5 (1500 in chips)
Seat 6: Hero (1500 in chips)
Player2: posts small blind 10
Player3: posts big blind 20
*** HOLE CARDS ***
Dealt to Hero [As Ks]
Player4: folds
Player5: raises 40 to 60
Hero: calls 60
Player1: folds
Player2: folds
Player3: folds
*** FLOP *** [Ah 7c 2s]
Player5: bets 80
Hero: calls 80
*** TURN *** [Ah 7c 2s] [9d]
Player5: checks
Hero: bets 160
Player5: folds
Uncalled bet (160) returned to Hero
Hero collected 310 from pot
*** SUMMARY ***
Total pot 310 | Rake 0
Board [Ah 7c 2s 9d]
Seat 6: Hero collected (310)
"""

# GGPoker Bounty Hunters fixture
GGPOKER_BOUNTY_HH = """Poker Hand #TM20240115123456: Bounty Hunters $32, Holdem No Limit - Level 3 (30/60) [2024/01/15 14:30:00]
Table 'Bounty Hunters 001' 9-max Seat #5 is the button
Seat 1: Player1 (5000 in chips) Bounty: $16
Seat 2: Player2 (4800 in chips) Bounty: $16
Seat 3: Player3 (5200 in chips) Bounty: $16
Seat 4: Hero (5100 in chips) Bounty: $16
Seat 5: Player5 (4900 in chips) Bounty: $16
Seat 6: Player6 (5000 in chips) Bounty: $16
Player6: posts small blind 30
Player1: posts big blind 60
*** HOLE CARDS ***
Dealt to Hero [Qh Qd]
Player2: folds
Player3: raises 120 to 180
Hero: raises 420 to 600
Player5: folds
Player6: folds
Player1: folds
Player3: calls 420
*** FLOP *** [9s 5c 2h]
Player3: checks
Hero: bets 720
Player3: folds
Uncalled bet (720) returned to Hero
Hero collected 1290 from pot
*** SUMMARY ***
Total pot 1290 | Rake 0
Board [9s 5c 2h]
Seat 4: Hero collected (1290)
"""

# NON-KO fixture for comparison
REGULAR_HH = """PokerStars Hand #234567890: Tournament #111222333, $10+$1 USD Hold'em No Limit - Level II (15/30) - 2024/01/15 21:00:00 ET
Table '111222333 5' 9-max Seat #2 is the button
Seat 1: Player1 (3000 in chips)
Seat 2: Player2 (2950 in chips)
Seat 3: Hero (3100 in chips)
Player3: posts small blind 15
Hero: posts big blind 30
*** HOLE CARDS ***
Dealt to Hero [Jh Js]
Player1: raises 60 to 90
Player2: folds
Hero: calls 60
*** FLOP *** [Tc 8d 4s]
Hero: checks
Player1: bets 120
Hero: calls 120
*** TURN *** [Tc 8d 4s] [2c]
Hero: checks
Player1: checks
*** RIVER *** [Tc 8d 4s 2c] [Kh]
Hero: checks
Player1: checks
*** SHOW DOWN ***
Hero: shows [Jh Js] (a pair of Jacks)
Player1: mucks hand
Hero collected 420 from pot
*** SUMMARY ***
Total pot 420 | Rake 0
Board [Tc 8d 4s 2c Kh]
Seat 3: Hero showed [Jh Js] and won (420) with a pair of Jacks
"""


class TestPKODetection:
    """Test tournament type detection for different networks."""
    
    def test_pokerstars_pko_format_detection(self):
        """Test PokerStars X+Y+fee format is detected as PKO."""
        tourney_type = detect_tourney_type(POKERSTARS_PKO_HH)
        assert tourney_type == "PKO", f"Expected PKO but got {tourney_type} for PokerStars X+Y+fee format"
        
    def test_ggpoker_bounty_hunters_detection(self):
        """Test GGPoker Bounty Hunters is detected as PKO."""
        tourney_type = detect_tourney_type(GGPOKER_BOUNTY_HH)
        assert tourney_type == "PKO", f"Expected PKO but got {tourney_type} for GGPoker Bounty Hunters"
        
    def test_regular_tournament_detection(self):
        """Test regular tournament is detected as NON_KO."""
        tourney_type = detect_tourney_type(REGULAR_HH)
        assert tourney_type == "NON_KO", f"Expected NON_KO but got {tourney_type} for regular tournament"
    
    def test_zip_import_with_pokerstars_pko(self):
        """Test ZIP import correctly classifies PokerStars PKO hands."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test ZIP with PokerStars PKO hand
            zip_path = Path(tmpdir) / "test_pokerstars_pko.zip"
            with zipfile.ZipFile(zip_path, 'w') as zf:
                zf.writestr("pokerstars_pko.txt", POKERSTARS_PKO_HH)
            
            # Test detection from ZIP
            with zipfile.ZipFile(zip_path, 'r') as zf:
                content = zf.read("pokerstars_pko.txt").decode('utf-8')
                tourney_type = detect_tourney_type(content)
                assert tourney_type == "PKO", "PokerStars PKO from ZIP not detected correctly"
    
    def test_zip_import_with_gg_bounty(self):
        """Test ZIP import correctly classifies GGPoker Bounty Hunters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test ZIP with GGPoker Bounty hand
            zip_path = Path(tmpdir) / "test_gg_bounty.zip"
            with zipfile.ZipFile(zip_path, 'w') as zf:
                zf.writestr("gg_bounty.txt", GGPOKER_BOUNTY_HH)
            
            # Test detection from ZIP
            with zipfile.ZipFile(zip_path, 'r') as zf:
                content = zf.read("gg_bounty.txt").decode('utf-8')
                tourney_type = detect_tourney_type(content)
                assert tourney_type == "PKO", "GGPoker Bounty Hunters from ZIP not detected correctly"
    
    def test_mixed_zip_classification(self):
        """Test ZIP with mixed tournament types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "test_mixed.zip"
            
            # Create ZIP with all types
            with zipfile.ZipFile(zip_path, 'w') as zf:
                zf.writestr("ps_pko.txt", POKERSTARS_PKO_HH)
                zf.writestr("gg_bounty.txt", GGPOKER_BOUNTY_HH)
                zf.writestr("regular.txt", REGULAR_HH)
            
            # Test each file
            results = {}
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for filename in zf.namelist():
                    content = zf.read(filename).decode('utf-8')
                    results[filename] = detect_tourney_type(content)
            
            assert results["ps_pko.txt"] == "PKO", "PokerStars PKO misclassified"
            assert results["gg_bounty.txt"] == "PKO", "GGPoker Bounty misclassified"
            assert results["regular.txt"] == "NON_KO", "Regular tournament misclassified"


if __name__ == "__main__":
    # Run tests
    test = TestPKODetection()
    test.test_pokerstars_pko_format_detection()
    print("✓ PokerStars PKO format detected correctly")
    
    test.test_ggpoker_bounty_hunters_detection()
    print("✓ GGPoker Bounty Hunters detected correctly")
    
    test.test_regular_tournament_detection()
    print("✓ Regular tournament detected correctly")
    
    test.test_zip_import_with_pokerstars_pko()
    print("✓ ZIP import PokerStars PKO works")
    
    test.test_zip_import_with_gg_bounty()
    print("✓ ZIP import GGPoker Bounty works")
    
    test.test_mixed_zip_classification()
    print("✓ Mixed ZIP classification works")
    
    print("\n✅ All PKO detection tests passed!")