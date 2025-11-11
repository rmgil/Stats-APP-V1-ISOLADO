"""Test tournament classification rules (PKO / Non-KO / Mystery)"""
import pytest
from app.mtt_import.detectors import detect_tourney_type


def test_mystery_bounty_detection():
    """Test Mystery Bounty detection"""
    # 888poker Mystery Bounty example
    header_888_mystery = """888poker Tournament #123456789 $8.80 Mystery Bounty Hold'em No Limit"""
    assert detect_tourney_type(header_888_mystery) == 'MYSTERY'
    
    # Generic Mystery Bounty
    header_mystery = """Tournament #987654321 Mystery Bounty $50+$5"""
    assert detect_tourney_type(header_mystery) == 'MYSTERY'
    

def test_pko_bounty_detection():
    """Test PKO/Bounty detection"""
    # GGPoker Bounty Hunters example
    header_gg_bounty = """Poker Hand #TM123456789
Bounty Hunters $32, Table #1
Hold'em No Limit - Level I"""
    assert detect_tourney_type(header_gg_bounty) == 'PKO'
    
    # PokerStars PKO with €X+€Y+fee format
    header_ps_pko = """PokerStars Tournament #123456789, €3.37+€3.38+€0.75 EUR
Hold'em No Limit - Level I (10/20)"""
    assert detect_tourney_type(header_ps_pko) == 'PKO'
    
    # Progressive KO variant
    header_progressive = """Tournament #123456789 Progressive KO $10+$10+$1"""
    assert detect_tourney_type(header_progressive) == 'PKO'
    
    # Simple Bounty tournament
    header_bounty = """Tournament #123456789 $5 Bounty Hold'em"""
    assert detect_tourney_type(header_bounty) == 'PKO'
    
    # KO tournament
    header_ko = """Tournament #123456789 $10 KO Hold'em No Limit"""
    assert detect_tourney_type(header_ko) == 'PKO'
    

def test_nonko_detection():
    """Test Non-KO detection (default case)"""
    # Regular tournament without bounty/KO keywords
    header_regular = """PokerStars Tournament #123456789, $10+$1 USD
Hold'em No Limit - Level I (10/20)"""
    assert detect_tourney_type(header_regular) == 'NON_KO'
    
    # Freezeout tournament
    header_freezeout = """Tournament #123456789 $50 Freezeout Hold'em"""
    assert detect_tourney_type(header_freezeout) == 'NON_KO'
    
    # Turbo tournament
    header_turbo = """Tournament #123456789, $20+$2 Turbo Hold'em"""
    assert detect_tourney_type(header_turbo) == 'NON_KO'
    

def test_priority_order():
    """Test that Mystery has priority over PKO"""
    # Mystery Bounty should be classified as MYSTERY, not PKO
    header_mystery_with_bounty = """Tournament #123456789 Mystery Bounty $100+$10"""
    assert detect_tourney_type(header_mystery_with_bounty) == 'MYSTERY'
    
    # Even with PKO keywords, Mystery takes priority
    header_mystery_pko = """Mystery Bounty Progressive KO Tournament"""
    assert detect_tourney_type(header_mystery_pko) == 'MYSTERY'
    

def test_case_insensitive():
    """Test case-insensitive detection"""
    assert detect_tourney_type("MYSTERY BOUNTY tournament") == 'MYSTERY'
    assert detect_tourney_type("mystery bounty tournament") == 'MYSTERY'
    assert detect_tourney_type("MysTeRy BoUnTy tournament") == 'MYSTERY'
    
    assert detect_tourney_type("BOUNTY HUNTERS $32") == 'PKO'
    assert detect_tourney_type("bounty hunters $32") == 'PKO'
    
    assert detect_tourney_type("Regular Tournament $10") == 'NON_KO'
    

def test_real_world_examples():
    """Test with real-world tournament headers"""
    # Real GGPoker Bounty Hunters
    gg_real = """Poker Hand #TM35847592038: Tournament #35847592038, Bounty Hunters $32
Hold'em No Limit - Level XVII (500/1000)
Table '35847592038 3' 8-max Seat #4 is the button"""
    assert detect_tourney_type(gg_real) == 'PKO'
    
    # Real 888poker Mystery Bounty
    eight88_real = """888poker Hand #HH20231015-183045
Tournament #T123456789, Mystery Bounty $8.80 No Limit Hold'em
Level 10 (100/200) - 2023/10/15 18:30:45 ET"""
    assert detect_tourney_type(eight88_real) == 'MYSTERY'
    
    # Real PokerStars PKO
    ps_real = """PokerStars Hand #236789012: Tournament #3456789012, €3.37+€3.38+€0.75 EUR 
Hold'em No Limit - Level VI (50/100) - 2023/10/15 20:15:30 CET [2023/10/15 14:15:30 ET]
Table '3456789012 45' 9-max Seat #3 is the button"""
    assert detect_tourney_type(ps_real) == 'PKO'
    
    # Real PokerStars Non-KO
    ps_nonko = """PokerStars Hand #236789013: Tournament #3456789013, $10+$1 USD
Hold'em No Limit - Level III (20/40) - 2023/10/15 19:30:00 CET
Table '3456789013 12' 9-max Seat #7 is the button"""
    assert detect_tourney_type(ps_nonko) == 'NON_KO'