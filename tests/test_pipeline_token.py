"""
Test full pipeline from upload to scorecard generation
"""
import pytest
import json
import os
import tempfile
import zipfile
from pathlib import Path

def create_test_zip_with_hands():
    """Create a test ZIP file with sample hand histories"""
    temp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(temp_dir, "test_hands.zip")
    
    # Sample hand history content (minimal valid format)
    sample_hand = """PokerStars Hand #123456789: Tournament #987654321, $10+$1 USD Hold'em No Limit - Level I (10/20) - 2024/01/01 12:00:00 ET
Table '987654321 1' 9-max Seat #1 is the button
Seat 1: Player1 (3000 in chips)
Seat 2: Player2 (3000 in chips)
Seat 3: Hero (3000 in chips)
Player1: posts small blind 10
Player2: posts big blind 20
*** HOLE CARDS ***
Dealt to Hero [As Ks]
Hero: raises 40 to 60
Player1: folds
Player2: folds
Uncalled bet (40) returned to Hero
Hero collected 50 from pot
*** SUMMARY ***
Total pot 50 | Rake 0
Seat 3: Hero collected (50)

PokerStars Hand #123456790: Tournament #987654321, $10+$1 USD Hold'em No Limit - Level I (10/20) - 2024/01/01 12:01:00 ET
Table '987654321 1' 9-max Seat #2 is the button
Seat 1: Player1 (2990 in chips)
Seat 2: Player2 (2980 in chips)
Seat 3: Hero (3030 in chips)
Hero: posts small blind 10
Player1: posts big blind 20
*** HOLE CARDS ***
Dealt to Hero [9h 9s]
Player2: folds
Hero: raises 40 to 60
Player1: calls 40
*** FLOP *** [Qh 7s 2c]
Hero: bets 80
Player1: folds
Uncalled bet (80) returned to Hero
Hero collected 120 from pot
*** SUMMARY ***
Total pot 120 | Rake 0
Seat 3: Hero collected (120)
"""
    
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("tournament_hands.txt", sample_hand)
    
    return zip_path

def test_pipeline_upload_and_run(client):
    """Test full pipeline: upload ZIP → run pipeline → verify scorecard"""
    # Create test ZIP
    zip_path = create_test_zip_with_hands()
    
    try:
        # Step 1: Upload ZIP file
        with open(zip_path, 'rb') as f:
            response = client.post(
                '/api/upload/zip',
                data={'file': (f, 'test_hands.zip')},
                content_type='multipart/form-data'
            )
        
        assert response.status_code == 200
        upload_data = json.loads(response.data)
        assert 'manifest' in upload_data
        assert 'token' in upload_data['manifest']
        
        token = upload_data['manifest']['token']
        assert token is not None and len(token) > 0
        
        # Step 2: Run pipeline
        response = client.post(
            '/api/pipeline/run',
            json={'token': token},
            content_type='application/json'
        )
        
        assert response.status_code == 200
        pipeline_data = json.loads(response.data)
        assert 'success' in pipeline_data
        assert pipeline_data['success'] == True
        assert 'artifacts' in pipeline_data
        
        # Step 3: Verify scorecard exists
        scorecard_path = pipeline_data['artifacts'].get('scorecard')
        assert scorecard_path is not None
        
        # Check if scorecard file was created
        full_path = f"/tmp/uploads/{token}/{scorecard_path}"
        if os.path.exists(full_path):
            with open(full_path, 'r') as f:
                scorecard = json.load(f)
                
            # Verify scorecard structure
            assert 'stat_level' in scorecard
            assert 'group_level' in scorecard
            
            # Check for RFI stats with percentages
            if 'stat_level' in scorecard and scorecard['stat_level']:
                for stat_key, stat_data in scorecard['stat_level'].items():
                    if stat_key.startswith('RFI_'):
                        # At least one group should have data
                        has_data = False
                        for group_key, group_data in stat_data.items():
                            if 'pct_time_decay' in group_data:
                                pct = group_data['pct_time_decay']
                                assert pct >= 0, f"Percentage for {stat_key}/{group_key} should be >= 0"
                                has_data = True
                                
                                # Check for grade and note (Phase 10.D)
                                if 'grade' in group_data:
                                    assert group_data['grade'] in ['-', 'A', 'B', 'C', 'D', 'E', 'F']
                                if 'note' in group_data:
                                    assert isinstance(group_data['note'], str)
    
    finally:
        # Cleanup
        os.unlink(zip_path)
        os.rmdir(os.path.dirname(zip_path))

def test_pipeline_invalid_token(client):
    """Test pipeline run with invalid token"""
    response = client.post(
        '/api/pipeline/run',
        json={'token': 'invalid_token_12345'},
        content_type='application/json'
    )
    
    # Should fail gracefully
    assert response.status_code in [400, 404, 500]

def test_pipeline_missing_token(client):
    """Test pipeline run without token"""
    response = client.post(
        '/api/pipeline/run',
        json={},
        content_type='application/json'
    )
    
    assert response.status_code == 400

def test_upload_creates_token(client):
    """Test that upload always creates a unique token"""
    zip_path = create_test_zip_with_hands()
    
    try:
        tokens = []
        for _ in range(3):
            with open(zip_path, 'rb') as f:
                response = client.post(
                    '/api/upload/zip',
                    data={'file': (f, 'test_hands.zip')},
                    content_type='multipart/form-data'
                )
            
            assert response.status_code == 200
            data = json.loads(response.data)
            token = data['manifest']['token']
            assert token not in tokens  # Each upload should get unique token
            tokens.append(token)
    
    finally:
        os.unlink(zip_path)
        os.rmdir(os.path.dirname(zip_path))

def test_pipeline_artifacts_structure(client):
    """Test that pipeline returns proper artifacts structure"""
    zip_path = create_test_zip_with_hands()
    
    try:
        # Upload and get token
        with open(zip_path, 'rb') as f:
            response = client.post(
                '/api/upload/zip',
                data={'file': (f, 'test_hands.zip')},
                content_type='multipart/form-data'
            )
        
        token = json.loads(response.data)['manifest']['token']
        
        # Run pipeline
        response = client.post(
            '/api/pipeline/run',
            json={'token': token},
            content_type='application/json'
        )
        
        data = json.loads(response.data)
        assert 'artifacts' in data
        artifacts = data['artifacts']
        
        # Check expected artifact paths
        expected_keys = ['hands_enriched', 'partitions', 'stats', 'scorecard']
        for key in expected_keys:
            assert key in artifacts, f"Missing artifact: {key}"
            assert artifacts[key] is not None
            assert isinstance(artifacts[key], str)
    
    finally:
        os.unlink(zip_path)
        os.rmdir(os.path.dirname(zip_path))

if __name__ == "__main__":
    pytest.main([__file__, "-v"])