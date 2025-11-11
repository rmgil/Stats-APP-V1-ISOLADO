"""
Test dashboard rendering with token
"""
import pytest
import json
import os
import tempfile
import zipfile

def create_mock_scorecard(token):
    """Create a mock scorecard.json for testing dashboard"""
    base_dir = f"/tmp/uploads/{token}"
    os.makedirs(f"{base_dir}/out/scores", exist_ok=True)
    
    scorecard = {
        "generated_at": "2024-01-01T12:00:00Z",
        "overall": 75.5,
        "group_level": {
            "nonko_pref": 78.2,
            "pko_pref": 72.8,
            "postflop_all": 75.1
        },
        "stat_level": {
            "RFI_EARLY": {
                "nonko_combined": {
                    "pct_time_decay": 10.5,
                    "score_time_decay": 85.0,
                    "months_used": 3,
                    "grade": "A",
                    "note": "Dentro do ideal (8.00–12.00)."
                }
            },
            "RFI_MIDDLE": {
                "nonko_combined": {
                    "pct_time_decay": 18.2,
                    "score_time_decay": 90.0,
                    "months_used": 3,
                    "grade": "A",
                    "note": "Dentro do ideal (15.00–20.00)."
                }
            },
            "RFI_BTN_STEAL": {
                "nonko_combined": {
                    "pct_time_decay": 45.5,
                    "score_time_decay": 70.0,
                    "months_used": 3,
                    "grade": "C",
                    "note": "Acima do ideal (40.00–44.00); excede 1.50 pp."
                }
            }
        },
        "subgroup_level": {
            "RFI": {
                "nonko_combined": 81.7
            }
        }
    }
    
    scorecard_path = f"{base_dir}/out/scores/scorecard.json"
    with open(scorecard_path, 'w') as f:
        json.dump(scorecard, f, indent=2)
    
    return scorecard_path

def test_dashboard_v2_renders_with_token(client):
    """Test dashboard_v2 renders correctly with valid token"""
    # Create a test token and mock data
    token = "test_token_dashboard_" + str(os.getpid())
    scorecard_path = create_mock_scorecard(token)
    
    try:
        # Access dashboard_v2 with token
        response = client.get(f'/dashboard_v2?token={token}')
        
        assert response.status_code == 200
        html = response.data.decode('utf-8')
        
        # Check for key dashboard elements
        assert 'Dashboard v2' in html or 'Dashboard' in html
        assert 'Overall Score' in html or 'Score Global' in html
        
        # Check for stat families/groups (configured in the dashboard)
        assert 'nonko' in html.lower() or 'non-ko' in html.lower()
        
        # Check for Bootstrap cards structure
        assert 'card' in html
        assert 'col-' in html  # Bootstrap grid columns
        
        # Check for interactive elements
        assert 'btn' in html  # Bootstrap buttons
        
        # Check for score display elements
        assert 'badge' in html or 'score' in html.lower()
        
    finally:
        # Cleanup
        if os.path.exists(scorecard_path):
            os.unlink(scorecard_path)

def test_dashboard_v2_without_token(client):
    """Test dashboard_v2 behavior without token"""
    response = client.get('/dashboard_v2')
    
    # Should either redirect or show an error
    assert response.status_code in [200, 400, 404]
    
    if response.status_code == 200:
        html = response.data.decode('utf-8')
        # Should show message about missing token or no data
        assert ('token' in html.lower() or 
                'no data' in html.lower() or 
                'sem dados' in html.lower())

def test_dashboard_v2_with_invalid_token(client):
    """Test dashboard_v2 with invalid token"""
    response = client.get('/dashboard_v2?token=invalid_token_xyz')
    
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        html = response.data.decode('utf-8')
        # Should handle gracefully
        assert ('not found' in html.lower() or 
                'não encontrado' in html.lower() or
                'no data' in html.lower() or
                'sem dados' in html.lower())

def test_dashboard_legacy_renders(client):
    """Test legacy dashboard still renders"""
    response = client.get('/dashboard')
    
    assert response.status_code == 200
    html = response.data.decode('utf-8')
    
    # Check for dashboard elements
    assert 'dashboard' in html.lower()
    assert ('chart' in html.lower() or 
            'graph' in html.lower() or 
            'stats' in html.lower())

def test_serve_token_files_endpoint(client):
    """Test /files/<token>/<path> endpoint security"""
    token = "test_token_files_" + str(os.getpid())
    base_dir = f"/tmp/uploads/{token}"
    os.makedirs(base_dir, exist_ok=True)
    
    # Create a test file
    test_file_path = f"{base_dir}/test.json"
    test_data = {"test": "data"}
    with open(test_file_path, 'w') as f:
        json.dump(test_data, f)
    
    try:
        # Valid request
        response = client.get(f'/files/{token}/test.json')
        assert response.status_code == 200
        assert json.loads(response.data) == test_data
        
        # Path traversal attempt (should fail)
        response = client.get(f'/files/{token}/../../../etc/passwd')
        assert response.status_code == 404
        
        # Non-existent file
        response = client.get(f'/files/{token}/nonexistent.json')
        assert response.status_code == 404
        
        # Invalid token
        response = client.get('/files/invalid_token/test.json')
        assert response.status_code == 404
        
    finally:
        # Cleanup
        if os.path.exists(test_file_path):
            os.unlink(test_file_path)
        if os.path.exists(base_dir):
            os.rmdir(base_dir)

def test_dashboard_displays_grades_and_notes(client):
    """Test that dashboard displays grades (A-F) and explanatory notes"""
    token = "test_grades_" + str(os.getpid())
    scorecard_path = create_mock_scorecard(token)
    
    try:
        response = client.get(f'/dashboard_v2?token={token}')
        assert response.status_code == 200
        html = response.data.decode('utf-8')
        
        # Check for grade badges
        assert ('badge' in html and 
                ('bg-success' in html or  # Grade A
                 'bg-warning' in html or  # Grade C
                 'bg-danger' in html))    # Grade D
        
        # Check for explanatory notes
        assert ('Dentro do ideal' in html or 
                'Acima do ideal' in html or 
                'Abaixo do ideal' in html)
        
    finally:
        if os.path.exists(scorecard_path):
            os.unlink(scorecard_path)

def test_dashboard_responsive_layout(client):
    """Test dashboard has responsive Bootstrap layout"""
    token = "test_responsive_" + str(os.getpid())
    scorecard_path = create_mock_scorecard(token)
    
    try:
        response = client.get(f'/dashboard_v2?token={token}')
        assert response.status_code == 200
        html = response.data.decode('utf-8')
        
        # Check for responsive Bootstrap classes
        assert 'container' in html
        assert 'row' in html
        assert ('col-sm' in html or 'col-md' in html or 'col-lg' in html)
        
        # Check for mobile-friendly viewport meta tag
        assert 'viewport' in html
        
    finally:
        if os.path.exists(scorecard_path):
            os.unlink(scorecard_path)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])