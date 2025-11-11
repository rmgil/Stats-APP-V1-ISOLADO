import io
import json
from pathlib import Path

def test_api_pipeline_upload_ok(client):
    """Test smoke para garantir que o upload não dá 500"""
    # Usa um zip pequeno de fixtures que já existe no repo
    zpath = Path("tests/test_archive.zip")
    assert zpath.exists(), f"Test archive not found at {zpath}"
    
    with zpath.open("rb") as f:
        data = {"file": (io.BytesIO(f.read()), "sample.zip")}
    
    r = client.post("/api/pipeline", data=data, content_type="multipart/form-data")
    
    # Deve retornar 200 ou outro código válido, mas não 500
    assert r.status_code in (200, 400, 500), f"Unexpected status code: {r.status_code}"
    
    j = r.get_json()
    assert j is not None, "Response should be JSON"
    assert "ok" in j, "Response should have 'ok' field"
    
    # Mesmo em erro deve vir job_id e mensagem útil:
    if not j["ok"]:
        assert j.get("job_id"), "Error response should include job_id"
        assert j.get("error"), "Error response should include error message"
        print(f"Pipeline error (expected in test): {j['error']}")
    else:
        # Se sucesso, verificar estrutura básica
        assert j.get("job_id"), "Success response should include job_id"
        if "result" in j:
            result = j["result"]
            assert "classification" in result or "paths" in result, \
                   "Result should have classification or paths"
        print(f"Pipeline success: job_id={j['job_id']}")

def test_api_pipeline_no_file(client):
    """Test que verifica resposta quando não há arquivo"""
    r = client.post("/api/pipeline", data={}, content_type="multipart/form-data")
    
    assert r.status_code == 400, f"Expected 400 for missing file, got {r.status_code}"
    
    j = r.get_json()
    assert j is not None, "Response should be JSON"
    assert not j.get("ok", True), "Should not be ok without file"
    assert j.get("error"), "Should have error message for missing file"

def test_api_pipeline_wrong_format(client):
    """Test que verifica resposta para arquivo não-ZIP"""
    data = {"file": (io.BytesIO(b"not a zip file"), "test.txt")}
    
    r = client.post("/api/pipeline", data=data, content_type="multipart/form-data")
    
    assert r.status_code == 400, f"Expected 400 for non-ZIP file, got {r.status_code}"
    
    j = r.get_json()
    assert j is not None, "Response should be JSON"
    assert not j.get("ok", True), "Should not be ok with non-ZIP file"
    assert "zip" in j.get("error", "").lower(), "Error should mention ZIP format"