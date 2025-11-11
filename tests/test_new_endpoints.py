"""Test new upload and pipeline endpoints"""
import sys
import os
import json
import io
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app


def create_test_zip():
    """Create a simple test ZIP file"""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr('test.txt', 'Test content for PKO tournament')
    zip_buffer.seek(0)
    return zip_buffer


def test_upload_zip_endpoint():
    """Test /api/upload/zip endpoint"""
    print("\nTesting /api/upload/zip endpoint...")
    
    with app.test_client() as client:
        # Test without file
        response = client.post('/api/upload/zip')
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'missing file'
        print("✓ Returns error when no file provided")
        
        # Test with file
        test_zip = create_test_zip()
        response = client.post(
            '/api/upload/zip',
            data={'file': (test_zip, 'test.zip')},
            content_type='multipart/form-data'
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['ok'] == True
        assert 'manifest' in data
        assert 'token' in data['manifest']
        print(f"✓ Upload successful, token: {data['manifest']['token']}")
        
        return data['manifest']['token']


def test_pipeline_run_endpoint(token=None):
    """Test /api/pipeline/run endpoint"""
    print("\nTesting /api/pipeline/run endpoint...")
    
    with app.test_client() as client:
        # Test without token
        response = client.post(
            '/api/pipeline/run',
            json={}
        )
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'missing token'
        print("✓ Returns error when no token provided")
        
        # Test with token
        if token:
            response = client.post(
                '/api/pipeline/run',
                json={'token': token}
            )
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data['ok'] == True
            assert 'message' in data  # Placeholder message
            print(f"✓ Pipeline run accepted (placeholder): {data.get('message')}")


if __name__ == "__main__":
    print("=" * 60)
    print("TESTING NEW ENDPOINTS")
    print("=" * 60)
    
    # Test upload endpoint and get token
    token = test_upload_zip_endpoint()
    
    # Test pipeline endpoint with token
    test_pipeline_run_endpoint(token)
    
    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)