"""
Smoke test for complete pipeline run
"""
import os
import sys
import json
import zipfile
import tempfile
from pathlib import Path
from io import BytesIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Sample hand histories for testing
POKERSTARS_HAND = """PokerStars Hand #234567890: Tournament #3456789012, $50+$50 USD Hold'em Progressive Knockout - Level V (30/60)
Table '3456789012 45' 9-max Seat #3 is the button
Seat 1: Player1 (5000 in chips, $50 bounty)
Seat 2: Player2 (4800 in chips, $50 bounty)
Seat 3: Player3 (5200 in chips, $50 bounty)
Seat 4: Player4 (5100 in chips, $50 bounty)
Seat 5: Player5 (4900 in chips, $50 bounty)
Seat 6: Player6 (5000 in chips, $50 bounty)
Seat 7: Player7 (5000 in chips, $50 bounty)
Seat 8: Player8 (5000 in chips, $50 bounty)
Seat 9: Hero (5000 in chips, $50 bounty)
Player4: posts small blind 30
Player5: posts big blind 60
*** HOLE CARDS ***
Dealt to Hero [As Ks]
Player6: folds
Player7: folds
Player8: folds
Hero: raises 120 to 180
Player1: folds
Player2: folds
Player3: folds
Player4: folds
Player5: folds
Uncalled bet (120) returned to Hero
Hero collected 150 from pot
*** SUMMARY ***
Total pot 150 | Rake 0
Seat 9: Hero collected (150)

"""

GGPOKER_HAND = """GGPoker Hand #123456789: Tournament #999999 "Daily Mystery Bounty $100"
Blinds 100/200 ante 25
Table 1 (9-max)
Seat 1: Player1 (10000)
Seat 2: Player2 (10000)
Seat 3: Player3 (10000)
Seat 4: Player4 (10000)
Seat 5: Hero (10000)
Seat 6: Player6 (10000)
Seat 7: Player7 (10000)
Seat 8: Player8 (10000)
Seat 9: Player9 (10000)
Player1 posts SB 100
Player2 posts BB 200
*** Hole Cards ***
Dealt to Hero [Ac Kc]
Player3: folds
Player4: folds
Hero: raises 400 to 600
Player6: folds
Player7: folds
Player8: folds
Player9: folds
Player1: folds
Player2: folds
Hero wins 525
*** Summary ***
Total pot 525
Hero wins 525

"""

EIGHT88_HAND = """888poker Hand #345678901
Tournament ID: 987654321 - $25 Bounty Hunters Daily
Level 2 Blinds: 15/30
Table '987654321 1' 9-max Seat #1 is the button
Seat 1: Player1 (5000)
Seat 2: Hero (5000)
Seat 3: Player3 (5000)
Seat 4: Player4 (5000)
Seat 5: Player5 (5000)
Seat 6: Player6 (5000)
Seat 7: Player7 (5000)
Seat 8: Player8 (5000)
Seat 9: Player9 (5000)
Hero: posts small blind 15
Player3: posts big blind 30
*** HOLE CARDS ***
Dealt to Hero [Ah Kh]
Player4: folds
Player5: folds
Player6: folds
Player7: folds
Player8: folds
Player9: folds
Player1: folds
Hero: raises 45 to 60
Player3: folds
Hero collected 60 from pot
*** SUMMARY ***
Total pot 60

"""

def create_test_zip():
    """Create a ZIP file with test hand histories"""
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        # Add hand histories
        zf.writestr('pokerstars_pko.txt', POKERSTARS_HAND)
        zf.writestr('ggpoker_mystery.txt', GGPOKER_HAND)
        zf.writestr('888_bounty.txt', EIGHT88_HAND)
    
    zip_buffer.seek(0)
    return zip_buffer

def test_pipeline_smoke():
    """Run full pipeline smoke test"""
    from main import app
    
    with app.test_client() as client:
        # Create test ZIP
        zip_data = create_test_zip()
        
        # Upload to pipeline
        print("📤 Uploading test ZIP to pipeline...")
        response = client.post(
            '/api/pipeline/run',
            data={'file': (zip_data, 'test_hands.zip')},
            content_type='multipart/form-data'
        )
        
        assert response.status_code == 200
        result = json.loads(response.data)
        assert result.get('ok') == True
        token = result.get('token')
        assert token is not None
        print(f"✅ Pipeline completed with token: {token}")
        
        # Check logs
        print("\n📝 Checking pipeline logs...")
        logs_response = client.get(f'/api/pipeline/logs?token={token}')
        if logs_response.status_code == 200:
            logs_data = json.loads(logs_response.data)
            if logs_data.get('ok'):
                for log in logs_data.get('logs', []):
                    status_icon = '✅' if log['status'] == 'completed' else '⏳' if log['status'] == 'started' else '❌'
                    print(f"  {status_icon} {log['step']:12} - {log.get('message', '')}")
        
        # Check flat API
        print("\n📊 Checking flat API response...")
        flat_response = client.get(f'/api/stats/flat?token={token}')
        assert flat_response.status_code == 200
        flat_data = json.loads(flat_response.data)
        assert flat_data.get('ok') == True
        
        data = flat_data.get('data', {})
        assert data.get('token') == token
        
        # Validate structure
        groups = data.get('groups', [])
        assert len(groups) > 0, "Expected at least 1 group"
        
        # Find a group with stats
        found_stat = False
        for group in groups:
            print(f"\n  Group: {group.get('label', group.get('key'))}")
            for subgroup in group.get('subgroups', []):
                print(f"    Subgroup: {subgroup.get('label', subgroup.get('key'))}")
                for stat in subgroup.get('rows', []):
                    if stat.get('opps', 0) > 0:
                        found_stat = True
                        print(f"      ✅ {stat.get('label')}: {stat.get('att')}/{stat.get('opps')} = {stat.get('pct')}%")
                        break
                if found_stat:
                    break
            if found_stat:
                break
        
        assert found_stat, "Expected at least 1 stat with opportunities > 0"
        
        # Test CSV export
        print("\n📄 Testing CSV export...")
        csv_response = client.get(f'/api/stats/export.csv?token={token}')
        assert csv_response.status_code == 200
        assert csv_response.mimetype == 'text/csv'
        assert len(csv_response.data) > 100  # Should have some content
        print("✅ CSV export working")
        
        print("\n✅ Smoke test completed successfully!")

if __name__ == "__main__":
    test_pipeline_smoke()
    print("\n🎉 All smoke tests passed!")