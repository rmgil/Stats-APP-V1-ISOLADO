"""Smoke test for /api/run endpoint"""

import io
import zipfile
import json
from pathlib import Path


def test_api_run_smoke_basic(client):
    """Basic smoke test - upload returns 200 and token"""
    # Create a simple ZIP file with one TXT file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr('hand1.txt', 'PokerStars Hand #12345: Tournament #9999\n')
    
    zip_buffer.seek(0)
    
    data = {
        'file': (zip_buffer, 'smoke_test.zip')
    }
    
    response = client.post('/api/run', data=data, content_type='multipart/form-data')
    
    # Basic assertions - just check it works
    assert response.status_code == 200
    result = response.get_json()
    assert 'token' in result
    assert result.get('ok') is True
    
    # Cleanup
    token = result.get('token')
    if token:
        import shutil
        for path in [Path(f'runs/{token}'), Path(f'/tmp/jobs/{token}')]:
            if path.exists():
                shutil.rmtree(path)


def test_api_run_smoke_multifile(client):
    """Smoke test with multiple files of different types"""
    # Create ZIP with various tournament types
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        # Regular tournament
        zf.writestr('regular.txt', 'PokerStars Hand #1: Tournament #1000, $10+$1\n')
        
        # PKO tournament
        zf.writestr('pko.txt', 'PokerStars Hand #2: Tournament #2000, $10+$10+$2 USD\nbounty tournament\n')
        
        # Mystery bounty
        zf.writestr('mystery.txt', 'Hand #3: Mystery Bounty Tournament\n')
        
        # Another regular
        zf.writestr('regular2.txt', 'Hand #4: Tournament #3000\n')
    
    zip_buffer.seek(0)
    
    data = {
        'file': (zip_buffer, 'multifile_test.zip')
    }
    
    response = client.post('/api/run', data=data, content_type='multipart/form-data')
    
    # Check response
    assert response.status_code == 200
    result = response.get_json()
    assert result['ok'] is True
    assert 'token' in result
    assert 'files' in result
    assert result['files'] == 4
    
    # Cleanup
    token = result.get('token')
    if token:
        import shutil
        for path in [Path(f'runs/{token}'), Path(f'/tmp/jobs/{token}')]:
            if path.exists():
                shutil.rmtree(path)


def test_api_run_error_handling(client):
    """Test error cases"""
    # Test without file
    response = client.post('/api/run')
    assert response.status_code == 400
    
    # Test with empty form data
    response = client.post('/api/run', data={})
    assert response.status_code == 400
    
    # Test with non-ZIP file
    data = {
        'file': (io.BytesIO(b'not a zip'), 'test.txt')
    }
    response = client.post('/api/run', data=data)
    assert response.status_code == 400
    result = response.get_json()
    assert result['ok'] is False
    assert 'error' in result


def test_api_run_creates_artifacts(client):
    """Test that /api/run creates expected artifacts"""
    # Create test ZIP
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr('test.txt', 'PokerStars Hand #1: Tournament #1\n')
        zf.writestr('test2.txt', 'GGPoker Hand #2: PKO Tournament\n')
    
    zip_buffer.seek(0)
    
    data = {
        'file': (zip_buffer, 'artifacts_test.zip')
    }
    
    response = client.post('/api/run', data=data)
    assert response.status_code == 200
    
    result = response.get_json()
    token = result['token']
    
    # Check that expected directories were created
    run_dir = Path(f'runs/{token}')
    assert run_dir.exists()
    assert (run_dir / 'raw').exists()
    # Stats and scores may not exist if pipeline fails in test environment
    # Just check the main directory structure
    
    # Check that manifest was created
    assert (run_dir / 'manifest.json').exists()
    
    # Load and verify manifest
    with open(run_dir / 'manifest.json', 'r') as f:
        manifest = json.load(f)
    
    assert manifest['files_total'] == 2
    assert 'timestamp' in manifest
    assert 'token' in manifest
    
    # Cleanup
    import shutil
    for path in [run_dir, Path(f'/tmp/jobs/{token}')]:
        if path.exists():
            shutil.rmtree(path)