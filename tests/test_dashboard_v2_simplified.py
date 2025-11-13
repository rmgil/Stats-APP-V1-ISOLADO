"""Test simplified dashboard v2"""

def test_dashboard_v2_loads(client):
    """Test that dashboard v2 page loads"""
    r = client.get('/dashboard_v2')
    assert r.status_code == 200
    assert b'renderTables' in r.data
    assert b'/api/dashboard/payload' in r.data
    assert b'Dashboard' in r.data
    assert b'Novo Upload' in r.data

def test_dashboard_v2_with_token(client):
    """Test that dashboard v2 handles token parameter"""
    r = client.get('/dashboard_v2?token=abc123')
    assert r.status_code == 200
    assert b'renderTables' in r.data
    assert b'/api/dashboard/payload' in r.data
    
def test_dashboard_v2_no_old_endpoints(client):
    """Test that old endpoints are removed"""
    r = client.get('/dashboard_v2')
    assert r.status_code == 200
    # Ensure old endpoints are not used
    assert b'/api/stats/flat' not in r.data
    assert b'/api/score/summary' not in r.data
    assert b'loadFlat' not in r.data