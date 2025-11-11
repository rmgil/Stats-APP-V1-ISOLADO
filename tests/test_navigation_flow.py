"""Test navigation flow: / -> /import_mtt -> dashboard"""

def test_main_redirect_to_import(client):
    """Test that / redirects to /import_mtt"""
    r = client.get('/', follow_redirects=False)
    assert r.status_code == 302
    assert r.location == '/import_mtt'
    
def test_import_redirect_to_import_mtt(client):
    """Test that /import redirects to /import_mtt"""
    r = client.get('/import', follow_redirects=False)
    assert r.status_code == 302
    assert r.location == '/import_mtt'
    
def test_import_mtt_page_loads(client):
    """Test that /import_mtt page loads correctly"""
    r = client.get('/import_mtt')
    assert r.status_code == 200
    assert b'Importar MTT' in r.data
    assert b'Processar MTT' in r.data
    assert b'/api/import/upload_mtt' in r.data
    
def test_dashboard_has_new_upload_link(client):
    """Test that dashboard has link back to /import_mtt"""
    r = client.get('/dashboard_v2')
    assert r.status_code == 200
    assert b'Novo Upload' in r.data
    assert b'/import_mtt' in r.data