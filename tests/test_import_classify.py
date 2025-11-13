"""
Test tournament classification for different poker sites and formats
"""
import pytest
from app.upload.ingest import detect_bucket

def test_ggpoker_bounty_hunters_is_pko():
    """GGPoker Bounty Hunters should be classified as PKO"""
    samples = [
        "Bounty Hunters $5.50",
        "Bounty Hunters Special $10",
        "Daily Bounty Hunters $25",
        "$5.50 Bounty Hunters",
    ]
    for sample in samples:
        result = detect_bucket(sample, sample)
        assert result == "PKO", f"'{sample}' should be PKO, got {result}"

def test_pokerstars_ko_format_is_pko():
    """PokerStars KO format with explicit KO/Bounty keywords"""
    samples = [
        "$5.10+$5.00+$0.90 KO",
        "$10.20+$10.00+$1.80 Bounty",
        "$2.55+$2.50+$0.45 Progressive KO",
        "$25.50+$25.00+$4.50 Knockout",
    ]
    for sample in samples:
        result = detect_bucket(sample, sample)
        assert result == "PKO", f"'{sample}' should be PKO, got {result}"

def test_mystery_bounty_is_mystery():
    """Mystery Bounty tournaments should be classified as MYSTERY"""
    samples = [
        "Mystery Bounty $100",
        "$50 Mystery Bounty Special",
        "Daily Mystery Bounty Tournament",
        "WSOP Mystery Bounty Event",
        "GGPoker Mystery Bounty Festival",
    ]
    for sample in samples:
        result = detect_bucket(sample, sample)
        assert result == "MYSTERY", f"'{sample}' should be MYSTERY, got {result}"

def test_regular_tournaments_are_nonko():
    """Regular tournaments should be classified as NON-KO"""
    samples = [
        "$5.50 NL Hold'em",
        "Daily Special $10+$1",
        "Sunday Million $215",
        "Big $22",
        "Hot $11 Turbo",
        "$50+$5 Deep Stack",
    ]
    for sample in samples:
        result = detect_bucket(sample, sample)
        assert result == "NON_KO", f"'{sample}' should be NON_KO, got {result}"

def test_pko_keywords_detection():
    """Test various PKO keywords are detected"""
    pko_keywords = [
        "Progressive KO",
        "Progressive Knockout",
        "PKO Special",
        "Knockout Tournament",
        "Bounty Builder",
        "KO Festival",
    ]
    for keyword in pko_keywords:
        result = detect_bucket(keyword, keyword)
        assert result == "PKO", f"'{keyword}' should be PKO, got {result}"

def test_case_insensitive_detection():
    """Classification should be case-insensitive"""
    samples = [
        ("MYSTERY BOUNTY", "MYSTERY"),
        ("mystery bounty", "MYSTERY"),
        ("Mystery Bounty", "MYSTERY"),
        ("BOUNTY HUNTERS", "PKO"),
        ("bounty hunters", "PKO"),
        ("Progressive KO", "PKO"),
        ("progressive ko", "PKO"),
    ]
    for sample, expected in samples:
        result = detect_bucket(sample, sample)
        assert result == expected, f"'{sample}' should be {expected}, got {result}"

def test_classification_with_file_content():
    """Test classification using both filename and content"""
    # Mystery in filename should take precedence
    result = detect_bucket("Mystery_Bounty_Event.txt", "Regular tournament content")
    assert result == "MYSTERY"
    
    # PKO in content should be detected
    result = detect_bucket("tournament.txt", "This is a Progressive KO event")
    assert result == "PKO"
    
    # Bounty Hunters in content
    result = detect_bucket("gg_tournament.txt", "Welcome to Bounty Hunters $10")
    assert result == "PKO"
    
    # Regular tournament should be NON_KO
    result = detect_bucket("regular.txt", "Regular $10+$1 tournament")
    assert result == "NON_KO"

def test_special_characters_handling():
    """Test handling of special characters in tournament names"""
    samples = [
        "€10+€10+€2 PKO",  # Has PKO keyword
        "£5.50 Bounty Hunters",  # Has Bounty keyword
        "$25 Knockout Special",  # Has Knockout keyword
    ]
    for sample in samples:
        result = detect_bucket(sample, sample)
        assert result == "PKO", f"'{sample}' should be PKO, got {result}"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])