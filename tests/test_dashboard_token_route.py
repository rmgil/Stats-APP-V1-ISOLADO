from app.dashboard import api as dashboard_api


def test_dashboard_with_token_global_mode(monkeypatch, client):
    captured = {}

    def fake_load_global(token):
        captured["token"] = token
        return {"hello": "world"}

    monkeypatch.setattr(dashboard_api, "_load_global_dashboard_payload", fake_load_global)

    response = client.get("/api/dashboard/sampletoken")

    assert response.status_code == 200
    assert response.get_json() == {"ok": True, "data": {"hello": "world"}}
    assert captured["token"] == "sampletoken"


def test_dashboard_with_and_without_month(monkeypatch, client):
    def fake_load_global(token):
        return {"mode": "global", "token": token}

    captured = {}

    def fake_build_payload(token, month=None, include_months=False):
        captured["args"] = (token, month, include_months)
        return {"mode": "monthly", "month": month}

    monkeypatch.setattr(dashboard_api, "_load_global_dashboard_payload", fake_load_global)
    monkeypatch.setattr(dashboard_api, "build_dashboard_payload", fake_build_payload)

    global_response = client.get("/api/dashboard/sampletoken")
    monthly_response = client.get("/api/dashboard/sampletoken?month=2024-02")

    assert global_response.status_code == 200
    assert global_response.get_json() == {"ok": True, "data": {"mode": "global", "token": "sampletoken"}}

    assert monthly_response.status_code == 200
    assert monthly_response.get_json() == {"ok": True, "data": {"mode": "monthly", "month": "2024-02"}}
    assert captured["args"] == ("sampletoken", "2024-02", True)


def test_dashboard_with_token_month_payload(monkeypatch, client):
    captured = {}

    def fake_build_payload(token, month=None, include_months=False):
        captured["args"] = (token, month, include_months)
        return {"month_not_found": False, "requested_month": month}

    monkeypatch.setattr(dashboard_api, "build_dashboard_payload", fake_build_payload)

    response = client.get("/api/dashboard/sampletoken?month=2024-05")

    assert response.status_code == 200
    body = response.get_json()
    assert body["ok"] is True
    assert body["data"]["requested_month"] == "2024-05"
    assert captured["args"] == ("sampletoken", "2024-05", True)


def test_dashboard_with_token_missing_month(monkeypatch, client):
    def fake_build_payload(token, month=None, include_months=False):
        return {"month_not_found": True, "requested_month": month}

    monkeypatch.setattr(dashboard_api, "build_dashboard_payload", fake_build_payload)

    response = client.get("/api/dashboard/sampletoken?month=2024-07")

    assert response.status_code == 404
    body = response.get_json()
    assert body["ok"] is False
    assert body["error"] == "not_found"


def test_dashboard_with_token_empty_payload(monkeypatch, client):
    def fake_build_payload(token, month=None, include_months=False):
        return {}

    monkeypatch.setattr(dashboard_api, "build_dashboard_payload", fake_build_payload)

    response = client.get("/api/dashboard/sampletoken?month=2023-12")

    assert response.status_code == 404
    body = response.get_json()
    assert body == {"ok": False, "error": "not_found"}


def test_dashboard_with_token_invalid_month(client):
    response = client.get("/api/dashboard/sampletoken?month=2024/07")

    assert response.status_code == 400
    assert response.get_json() == {"ok": False, "error": "invalid_month"}
