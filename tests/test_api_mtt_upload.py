"""Test for /api/import/upload_mtt endpoint"""
import io
import json
from pathlib import Path


def test_upload_mtt_success(client):
    """Test successful upload and pipeline execution via upload_mtt"""
    # Use the test archive
    zpath = Path("tests/test_archive.zip")
    assert zpath.exists()
    
    with zpath.open("rb") as f:
        data = {"file": (io.BytesIO(f.read()), "test.zip")}
    
    r = client.post("/api/import/upload_mtt", data=data, content_type="multipart/form-data")
    
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    
    j = r.get_json()
    assert j is not None, "Response should be JSON"
    assert j.get("ok") is True, f"Expected ok=True, got {j}"
    
    # Check for token
    assert j.get("token"), "Response should include token"
    assert len(j["token"]) == 16, "Token should be 16 chars"
    
    # Check for result structure
    result = j.get("result", {})
    assert "classification" in result, "Result should have classification"
    assert "paths" in result, "Result should have paths"
    assert "summary" in result, "Result should have summary"
    
    # Check classification
    classification = result.get("classification", {})
    assert "total" in classification
    assert classification["total"] >= 0
    
    print(f"Upload MTT success: token={j['token']}, hands={result['summary'].get('hands', 0)}")


def test_upload_mtt_no_file(client):
    """Test upload_mtt with no file"""
    r = client.post("/api/import/upload_mtt", data={}, content_type="multipart/form-data")
    
    assert r.status_code == 400, f"Expected 400 for missing file, got {r.status_code}"
    
    j = r.get_json()
    assert j is not None, "Response should be JSON"
    assert not j.get("ok", True), "Should not be ok without file"
    assert "zip" in j.get("error", "").lower(), "Error should mention zip"


def test_upload_mtt_wrong_format(client):
    """Test upload_mtt with non-ZIP file"""
    data = {"file": (io.BytesIO(b"not a zip"), "test.txt")}
    
    r = client.post("/api/import/upload_mtt", data=data, content_type="multipart/form-data")
    
    assert r.status_code == 400, f"Expected 400 for non-ZIP, got {r.status_code}"
    
    j = r.get_json()
    assert j is not None, "Response should be JSON"
    assert not j.get("ok", True), "Should not be ok with non-ZIP"
    assert "zip" in j.get("error", "").lower(), "Error should mention zip"


def test_upload_mtt_dashboard_redirect(client):
    """Test that token can be used to access dashboard_v2"""
    # First upload a file
    zpath = Path("tests/test_archive.zip")
    with zpath.open("rb") as f:
        data = {"file": (io.BytesIO(f.read()), "test.zip")}
    
    r = client.post("/api/import/upload_mtt", data=data, content_type="multipart/form-data")
    assert r.status_code == 200
    
    token = r.get_json()["token"]
    
    # Access dashboard with token
    r2 = client.get(f"/dashboard_v2?token={token}")
    assert r2.status_code == 200, f"Dashboard should be accessible with token, got {r2.status_code}"
    
    # Check that token is in the page
    html = r2.data.decode()
    assert token in html or "token" in html.lower(), "Dashboard should reference the token"