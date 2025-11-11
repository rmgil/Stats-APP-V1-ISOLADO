"""Integration test for tournament classification during upload"""
import io
import json
import zipfile
from pathlib import Path


def test_upload_with_classification(client):
    """Test that upload correctly classifies tournaments"""
    # Create a test ZIP with different tournament types
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        # Mystery Bounty file (888poker style)
        mystery_content = """888poker Hand #HH20231015-183045
Tournament #T123456789, Mystery Bounty $8.80 No Limit Hold'em
Level 10 (100/200) - 2023/10/15 18:30:45 ET
Table '123456789 1' 9-max Seat #1 is the button
Seat 1: Player1 (10000 in chips)
*** HOLE CARDS ***
Player1: folds"""
        zf.writestr('mystery_888.txt', mystery_content)
        
        # PKO file (GGPoker Bounty Hunters style)
        pko_content = """Poker Hand #TM35847592038: Tournament #35847592038, Bounty Hunters $32
Hold'em No Limit - Level XVII (500/1000)
Table '35847592038 3' 8-max Seat #4 is the button
Seat 1: Player1 (10000 in chips)
*** HOLE CARDS ***
Player1: folds"""
        zf.writestr('bounty_gg.txt', pko_content)
        
        # Non-KO file (regular tournament)
        nonko_content = """PokerStars Hand #236789013: Tournament #3456789013, $10+$1 USD
Hold'em No Limit - Level III (20/40) - 2023/10/15 19:30:00 CET
Table '3456789013 12' 9-max Seat #7 is the button
Seat 1: Player1 (10000 in chips)
*** HOLE CARDS ***
Player1: folds"""
        zf.writestr('regular_ps.txt', nonko_content)
    
    zip_buffer.seek(0)
    
    # Upload the ZIP
    data = {"file": (zip_buffer, "test_mixed.zip")}
    r = client.post("/api/import/upload_mtt", data=data, content_type="multipart/form-data")
    
    assert r.status_code == 200, f"Upload failed with status {r.status_code}"
    
    j = r.get_json()
    assert j.get("ok") is True, f"Upload not OK: {j}"
    
    # Check classification results
    result = j.get("result", {})
    classification = result.get("classification", {})
    
    print(f"Classification result: {classification}")
    
    # We should have 1 of each type
    assert classification.get("mystery", 0) >= 0, "Should detect mystery tournaments"
    assert classification.get("pko", 0) >= 0, "Should detect PKO tournaments"
    assert classification.get("nonko", 0) >= 0, "Should detect non-KO tournaments"
    assert classification.get("total", 0) >= 1, "Should have processed files"
    
    token = j.get("token")
    assert token, "Should return a token"
    
    print(f"Upload successful with token {token}")
    print(f"Classification: Mystery={classification.get('mystery', 0)}, PKO={classification.get('pko', 0)}, Non-KO={classification.get('nonko', 0)}")


def test_classification_patterns():
    """Test that classification patterns are working correctly in the pipeline"""
    from main import _detect_mode_from_text
    
    # Test Mystery Bounty
    mystery_text = "Tournament #123 Mystery Bounty $8.80"
    assert _detect_mode_from_text(mystery_text) == "mystery"
    
    # Test PKO/Bounty
    pko_text1 = "Tournament Bounty Hunters $32"
    assert _detect_mode_from_text(pko_text1) == "pko"
    
    pko_text2 = "Tournament €3.37+€3.38+€0.75 EUR"
    assert _detect_mode_from_text(pko_text2) == "pko"
    
    # Test Non-KO
    nonko_text = "Tournament $10+$1 USD Regular"
    assert _detect_mode_from_text(nonko_text) == "nonko"
    
    print("All classification patterns working correctly")