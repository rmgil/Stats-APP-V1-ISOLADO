"""Test /api/dashboard/payload endpoint"""

def test_api_dashboard_payload_no_token(client):
    """Test endpoint without token"""
    r = client.get('/api/dashboard/payload')
    assert r.status_code == 200
    
    data = r.get_json()
    assert 'run' in data
    assert 'overall' in data
    assert 'group_level' in data
    assert 'samples' in data
    assert 'months' in data
    assert 'counts' in data
    assert data['run']['token'] is None
    assert data['run']['base'] == '.'


def test_api_dashboard_payload_with_token(client):
    """Test endpoint with token"""
    r = client.get('/api/dashboard/payload?token=test123')
    assert r.status_code == 200
    
    data = r.get_json()
    assert 'run' in data
    assert 'overall' in data
    assert 'group_level' in data
    assert 'samples' in data
    assert 'months' in data
    assert 'counts' in data
    assert data['run']['token'] == 'test123'
    assert data['run']['base'] == 'runs/test123'