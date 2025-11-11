"""
Test tournament classification for PKO/Mystery/NON-KO detection
"""
import pytest
from app.classify.run import classify_tournament, extract_tournament_info

# Test fixtures for different poker sites
POKERSTARS_FIXTURES = {
    "pko": """PokerStars Hand #123456: Tournament #999999999, $10+$10 USD Hold'em Progressive Knockout - Level I (10/20)
Table '999999999 1' 9-max Seat #1 is the button
Seat 1: Player1 (1500 in chips, $10 bounty)""",
    
    "mystery": """PokerStars Hand #123457: Tournament #999999998, $50+$50 USD Hold'em Mystery Bounty - Level II (15/30)
Table '999999998 1' 9-max Seat #2 is the button""",
    
    "nonko": """PokerStars Hand #123458: Tournament #999999997, $100+$9 USD Hold'em No Limit - Level III (20/40)
Table '999999997 1' 9-max Seat #3 is the button""",
    
    "bounty_hunters": """PokerStars Hand #123459: Tournament #999999996, $25 Bounty Hunters Special - Level I (10/20)
Table '999999996 1' 9-max"""
}

GGPOKER_FIXTURES = {
    "pko": """GGPoker Hand #123456: Tournament #999999 "Daily PKO $100"
Blinds 100/200 ante 25
Table 1 (9-max)""",
    
    "mystery": """GGPoker Hand #123457: Tournament #999998 "Mystery Bounty Main Event"
Blinds 150/300 ante 40
Table 2 (9-max)""",
    
    "nonko": """GGPoker Hand #123458: Tournament #999997 "Sunday Million"
Blinds 200/400 ante 50
Table 3 (9-max)""",
    
    "knockout": """GGPoker Hand #123459: Tournament #999996 "Knockout Series"
Blinds 50/100 ante 10"""
}

EIGHT88_FIXTURES = {
    "pko": """888poker Hand #123456
Tournament ID: 999999 - $50 Progressive Knockout
Level 1 Blinds: 10/20""",
    
    "mystery": """888poker Hand #123457
Tournament ID: 999998 - Mystery Bounty $100
Level 2 Blinds: 15/30""",
    
    "nonko": """888poker Hand #123458
Tournament ID: 999997 - Sunday Special $200
Level 3 Blinds: 20/40""",
    
    "bounty": """888poker Hand #123459
Tournament ID: 999996 - Bounty Builder $30
Level 1 Blinds: 10/20"""
}

class TestClassifyTournament:
    """Test tournament classification logic"""
    
    def test_pokerstars_pko(self):
        assert classify_tournament(POKERSTARS_FIXTURES["pko"]) == "PKO"
        
    def test_pokerstars_mystery(self):
        assert classify_tournament(POKERSTARS_FIXTURES["mystery"]) == "MYSTERIES"
        
    def test_pokerstars_nonko(self):
        assert classify_tournament(POKERSTARS_FIXTURES["nonko"]) == "NON-KO"
        
    def test_pokerstars_bounty_hunters(self):
        assert classify_tournament(POKERSTARS_FIXTURES["bounty_hunters"]) == "PKO"
    
    def test_ggpoker_pko(self):
        assert classify_tournament(GGPOKER_FIXTURES["pko"]) == "PKO"
        
    def test_ggpoker_mystery(self):
        assert classify_tournament(GGPOKER_FIXTURES["mystery"]) == "MYSTERIES"
        
    def test_ggpoker_nonko(self):
        assert classify_tournament(GGPOKER_FIXTURES["nonko"]) == "NON-KO"
        
    def test_ggpoker_knockout(self):
        assert classify_tournament(GGPOKER_FIXTURES["knockout"]) == "PKO"
    
    def test_888_pko(self):
        assert classify_tournament(EIGHT88_FIXTURES["pko"]) == "PKO"
        
    def test_888_mystery(self):
        assert classify_tournament(EIGHT88_FIXTURES["mystery"]) == "MYSTERIES"
        
    def test_888_nonko(self):
        assert classify_tournament(EIGHT88_FIXTURES["nonko"]) == "NON-KO"
        
    def test_888_bounty(self):
        assert classify_tournament(EIGHT88_FIXTURES["bounty"]) == "PKO"

class TestExtractTournamentInfo:
    """Test tournament name extraction"""
    
    def test_extract_pokerstars(self):
        name = extract_tournament_info(POKERSTARS_FIXTURES["pko"])
        assert "Progressive Knockout" in name
        
    def test_extract_ggpoker(self):
        name = extract_tournament_info(GGPOKER_FIXTURES["mystery"])
        assert name == "Mystery Bounty Main Event"
        
    def test_extract_888(self):
        name = extract_tournament_info(EIGHT88_FIXTURES["bounty"])
        assert name == "Bounty Builder $30"

class TestEdgeCases:
    """Test edge cases and special scenarios"""
    
    def test_case_insensitive(self):
        """Test case insensitive matching"""
        assert classify_tournament("PROGRESSIVE KNOCKOUT tournament") == "PKO"
        assert classify_tournament("mystery BOUNTY special") == "MYSTERIES"
        assert classify_tournament("regular tournament") == "NON-KO"
    
    def test_ko_as_whole_word(self):
        """Test that KO matches as whole word only"""
        assert classify_tournament("This is a KO tournament") == "PKO"
        assert classify_tournament("This is knockout event") == "PKO"
        assert classify_tournament("Look at this token") == "NON-KO"  # 'ko' in 'token' shouldn't match
    
    def test_mystery_priority(self):
        """Test that Mystery has priority over regular bounty"""
        assert classify_tournament("Mystery Bounty Progressive") == "MYSTERIES"
        assert classify_tournament("Bounty Mystery Special") == "MYSTERIES"
    
    def test_empty_content(self):
        """Test empty or minimal content"""
        assert classify_tournament("") == "NON-KO"
        assert classify_tournament("Random text") == "NON-KO"