def test_api_stats_flat(client):
    r = client.get("/api/stats/flat")
    assert r.status_code == 200
    js = r.get_json()
    assert "rows" in js
    # se houver dados, pelo menos um item tem pct calculado ou opp/att presentes
    if js["rows"]:
        r0 = js["rows"][0]
        assert "opportunities" in r0 and "attempts" in r0