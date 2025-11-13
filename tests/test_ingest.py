"""Test the new ingest service"""
import sys
import os
import io
import json
import zipfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.upload.ingest import ingest_zip, detect_bucket, smart_read_text


def create_test_zip():
    """Create a test ZIP file with different types of hand histories"""
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        # Mystery Bounty file
        zf.writestr('mystery_tournament.txt', '''
PokerStars Hand #123456: Tournament #999999999, €10+€10+€2 EUR Hold'em No Limit - Level I (10/20)
Mystery Bounty Tournament - Big Prize Pool!
Table '999999999 1' 9-max Seat #1 is the button
Seat 1: Player1 (1500 in chips)
*** HOLE CARDS ***
Player1: raises 40 to 60
*** FLOP *** [Ac Kd Qh]
        ''')
        
        # PKO file (Bounty Hunters format)
        zf.writestr('pko_bounty.txt', '''
GGPoker Hand #123457: Bounty Hunters Special €5+€5+€1
Table '888888888 1' 6-max Seat #2 is the button  
Seat 1: Player2 (2000 in chips, $5 bounty)
*** HOLE CARDS ***
Player2: raises 50 to 100
*** FLOP *** [7d 8s 9h]
        ''')
        
        # Progressive KO file
        zf.writestr('progressive_ko.txt', '''
888poker Tournament #777777777, $10+$10+$2 Progressive Knock Out
Table '777777777 1' 9-max
Seat 3: Player3 (1500 in chips)
*** HOLE CARDS ***
Player3: calls 20
        ''')
        
        # Regular tournament (NON-KO)
        zf.writestr('regular_tournament.txt', '''
PokerStars Hand #123458: Tournament #666666666, €20+€2 EUR Hold'em No Limit
Table '666666666 1' 9-max Seat #4 is the button
Seat 4: Player4 (1500 in chips)
*** HOLE CARDS ***  
Player4: folds
        ''')
        
        # Another NON-KO
        zf.writestr('sunday_million.txt', '''
PokerStars Tournament #555555555, $100+$9 USD Hold'em No Limit
Sunday Million - Final Table
Seat 5: Player5 (100000 in chips)
*** HOLE CARDS ***
Player5: raises 2000 to 4000
        ''')
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


def test_ingest_service():
    """Test the ingest service functionality"""
    print("=" * 60)
    print("TESTING INGEST SERVICE")
    print("=" * 60)
    
    # Create test ZIP
    zip_bytes = create_test_zip()
    
    # Run ingest
    manifest = ingest_zip(zip_bytes)
    
    # Check results
    print("\n✓ Ingest completed successfully")
    print(f"Token: {manifest['token']}")
    print(f"Root: {manifest['root']}")
    print("\nClassification Results:")
    print(f"  • MYSTERY: {manifest['counts']['MYSTERY']} files")
    print(f"  • PKO: {manifest['counts']['PKO']} files") 
    print(f"  • NON_KO: {manifest['counts']['NON_KO']} files")
    
    # Verify counts
    assert manifest['counts']['MYSTERY'] == 1, "Should have 1 MYSTERY file"
    assert manifest['counts']['PKO'] == 2, "Should have 2 PKO files"
    assert manifest['counts']['NON_KO'] == 2, "Should have 2 NON_KO files"
    
    # Check files exist
    root = Path(manifest['root'])
    assert root.exists(), "Root directory should exist"
    
    raw_dir = root / "raw"
    assert raw_dir.exists(), "Raw directory should exist"
    
    # Count actual files
    txt_files = list(raw_dir.glob("*.txt"))
    assert len(txt_files) == 5, f"Should have 5 txt files, got {len(txt_files)}"
    
    # Test individual bucket detection
    print("\nTesting Bucket Detection:")
    test_cases = [
        ("Mystery Bounty tournament", "MYSTERY"),
        ("Bounty Hunters Special", "PKO"),
        ("Progressive Knock Out", "PKO"),
        ("Regular tournament", "NON_KO"),
        ("Sunday Million", "NON_KO"),
    ]
    
    for text, expected in test_cases:
        result = detect_bucket(text)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{text[:30]}...' → {result}")
    
    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)
    
    return manifest


if __name__ == "__main__":
    test_ingest_service()