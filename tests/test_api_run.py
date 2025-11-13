"""Test /api/run one-shot endpoint"""

import io
import zipfile
import json


def test_api_run_without_file(client):
    """Test that endpoint rejects request without file"""
    r = client.post('/api/run')
    assert r.status_code == 400
    data = r.get_json()
    assert data['ok'] is False
    assert 'error' in data


def test_api_run_with_invalid_file(client):
    """Test that endpoint rejects non-ZIP files"""
    data = {
        'file': (io.BytesIO(b'not a zip'), 'test.txt')
    }
    r = client.post('/api/run', data=data)
    assert r.status_code == 400
    data = r.get_json()
    assert data['ok'] is False


def test_api_run_with_valid_zip(client):
    """Test complete flow with valid ZIP"""
    # Create a ZIP with TXT files
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        # Add NON-KO hands
        zf.writestr('nonko1.txt', 'PokerStars Hand #1234: Tournament #999, $10+$1 USD\n')
        zf.writestr('nonko2.txt', 'PokerStars Hand #5678: Tournament #888, $5+$0.50 USD\n')
        
        # Add PKO hand
        zf.writestr('pko1.txt', 'GGPoker Hand #ABC123: Bounty Hunters $25\n')
        
        # Add Mystery hand
        zf.writestr('mystery1.txt', '888poker Hand #XYZ: Mystery Bounty Tournament\n')
    
    zip_buffer.seek(0)
    
    data = {
        'file': (zip_buffer, 'test_hands.zip')
    }
    
    r = client.post('/api/run', data=data, content_type='multipart/form-data')
    assert r.status_code == 200
    
    result = r.get_json()
    assert result['ok'] is True
    assert 'token' in result
    assert len(result['token']) == 16
    assert 'files' in result
    assert result['files'] == 4
    assert 'categories' in result
    
    # Categories should have classified files
    assert result['categories']['nonko'] == 2
    assert result['categories']['pko'] == 1
    assert result['categories']['mystery'] == 1
    
    # Verify runs/<token> structure was created
    token = result['token']
    from pathlib import Path
    
    run_dir = Path(f'runs/{token}')
    assert run_dir.exists()
    assert (run_dir / 'raw').exists()
    assert (run_dir / 'dashboard.json').exists()
    
    # Cleanup
    import shutil
    if run_dir.exists():
        shutil.rmtree(run_dir)
    
    job_dir = Path(f'/tmp/jobs/{token}')
    if job_dir.exists():
        shutil.rmtree(job_dir)