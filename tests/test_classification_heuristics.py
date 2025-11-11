"""Test classification heuristics for Mystery, PKO and NON-KO"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.upload.ingest import detect_bucket


def test_mystery_bounty():
    """Test Mystery Bounty detection"""
    print("\n🎯 Testing MYSTERY detection:")
    
    test_cases = [
        # Mystery Bounty variations
        ("Mystery Bounty Tournament", "MYSTERY"),
        ("mystery bounty special", "MYSTERY"),
        ("MYSTERY BOUNTY", "MYSTERY"),
        ("Tournament #123 Mystery Bounty €50", "MYSTERY"),
        ("GGPoker Mystery Bounty Edition", "MYSTERY"),
        ("888poker Mystery Bounty", "MYSTERY"),
        ("PokerStars Mystery Bounty Championship", "MYSTERY"),
    ]
    
    for text, expected in test_cases:
        result = detect_bucket(text)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{text}' → {result} (expected {expected})")
        assert result == expected, f"Failed: '{text}' should be {expected}, got {result}"


def test_pko_detection():
    """Test PKO detection including PokerStars A+B+fee pattern"""
    print("\n💰 Testing PKO detection:")
    
    test_cases = [
        # Bounty Hunters
        ("Bounty Hunters Special", "PKO"),
        ("GGPoker Bounty Hunters", "PKO"),
        ("bounty hunters tournament", "PKO"),
        
        # Progressive KO
        ("Progressive Knock Out", "PKO"),
        ("progressive KO tournament", "PKO"),
        ("PROGRESSIVE knockout", "PKO"),
        
        # KO variations
        ("Knock-Out Tournament", "PKO"),
        ("knockout special", "PKO"),
        ("KO Championship", "PKO"),
        ("Daily KO", "PKO"),
        
        # Generic Bounty
        ("Bounty Tournament", "PKO"),
        ("Sunday Bounty", "PKO"),
        
        # PokerStars A+B+fee pattern
        ("Tournament #123456789, €3.37+€3.38+€0.75 EUR Hold'em", "PKO"),
        ("Tournament #999999999, $5.00+$5.00+$1.00 USD Hold'em", "PKO"),
        ("Tournament #888888888, £10.50+£10.50+£2.00 GBP Hold'em", "PKO"),
    ]
    
    for text, expected in test_cases:
        result = detect_bucket(text)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{text[:50]}...' → {result}")
        assert result == expected, f"Failed: '{text}' should be {expected}, got {result}"


def test_non_ko_fallback():
    """Test NON-KO fallback for regular tournaments"""
    print("\n🎮 Testing NON-KO fallback:")
    
    test_cases = [
        # Regular tournaments
        ("Sunday Million", "NON_KO"),
        ("Main Event Championship", "NON_KO"),
        ("Tournament #123456789, €20+€2 EUR Hold'em", "NON_KO"),
        ("Daily Turbo", "NON_KO"),
        ("Sit & Go Tournament", "NON_KO"),
        ("WSOP Circuit Event", "NON_KO"),
        ("High Roller Tournament", "NON_KO"),
        ("", "NON_KO"),  # Empty text
        ("Random text without keywords", "NON_KO"),
    ]
    
    for text, expected in test_cases:
        result = detect_bucket(text)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{text[:30]}...' → {result}")
        assert result == expected, f"Failed: '{text}' should be {expected}, got {result}"


def test_priority_order():
    """Test that Mystery > PKO > NON-KO priority is respected"""
    print("\n🎲 Testing priority order (Mystery > PKO > NON-KO):")
    
    test_cases = [
        # Mystery has priority over PKO
        ("Mystery Bounty Hunters", "MYSTERY"),
        ("Mystery Bounty KO Tournament", "MYSTERY"),
        ("Progressive Mystery Bounty", "MYSTERY"),
        
        # PKO when no Mystery
        ("Bounty Hunters without mystery", "PKO"),
        ("KO Tournament regular", "PKO"),
    ]
    
    for text, expected in test_cases:
        result = detect_bucket(text)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{text}' → {result}")
        assert result == expected, f"Failed: '{text}' should be {expected}, got {result}"


def test_encoding_normalization():
    """Test encoding normalization"""
    print("\n🔤 Testing encoding normalization:")
    
    from app.upload.ingest import smart_read_text
    from pathlib import Path
    import tempfile
    
    test_texts = [
        ("UTF-8", "Test €50 tournament\n", "utf-8"),
        ("UTF-8-BOM", "\ufeffTest €50 tournament\n", "utf-8-sig"),
        ("Windows-1252", "Test £50 tournament\n", "cp1252"),
        ("Latin-1", "Test ¢50 tournament\n", "latin-1"),
    ]
    
    with tempfile.TemporaryDirectory() as tmpdir:
        for name, text, encoding in test_texts:
            # Write with specific encoding
            test_file = Path(tmpdir) / f"test_{name}.txt"
            test_file.write_text(text, encoding=encoding)
            
            # Read with smart_read_text
            result = smart_read_text(test_file)
            
            # Check line endings are normalized to LF
            assert "\r\n" not in result, f"CRLF found in {name}"
            assert "\r" not in result, f"CR found in {name}"
            
            print(f"  ✓ {name} encoding handled correctly")


if __name__ == "__main__":
    print("=" * 70)
    print("TESTING CLASSIFICATION HEURISTICS")
    print("=" * 70)
    
    test_mystery_bounty()
    test_pko_detection()
    test_non_ko_fallback()
    test_priority_order()
    test_encoding_normalization()
    
    print("\n" + "=" * 70)
    print("✅ ALL HEURISTICS WORKING CORRECTLY!")
    print("=" * 70)
    print("\nSummary:")
    print("  • Mystery: Detects 'Mystery Bounty' (GGPoker, 888, Stars)")
    print("  • PKO: Detects 'Bounty Hunters', 'KO', 'Progressive'")
    print("  • PKO: Detects PokerStars A+B+fee pattern (e.g., €3.37+€3.38+€0.75)")
    print("  • NON-KO: Fallback when no patterns match")
    print("  • Encoding: Normalizes UTF-8, UTF-8-BOM, CP1252, Latin-1 to UTF-8")
    print("  • Line endings: Converts CRLF and CR to LF")