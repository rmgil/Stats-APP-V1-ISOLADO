# tests/test_phase10_overview.py
import json, os, tempfile, shutil
from app.dashboard.aggregate import build_overview

def test_overview_minimal(tmp_path):
    job = tmp_path / "job1" / "stats"
    job.parent.mkdir(parents=True)
    job.mkdir()
    
    # counts com 2 meses e 1 stat
    counts = {"counts": {
      "2025-06": {"nonko_9max_pref":{"RFI_EARLY":{"opportunities":100,"attempts":25}}},
      "2025-05": {"nonko_9max_pref":{"RFI_EARLY":{"opportunities":50,"attempts":10}}}
    }}
    (job.parent/"stat_counts.json").write_text(json.dumps(counts), encoding="utf-8")

    # scorecard mínimo
    score = {"overall":{"score":82},"group_level":{
      "nonko_9max_pref":{"weight":0.4,"score":81,"subgroups":{
        "PREFLOP_RFI":{"score":80,"stats":{"RFI_EARLY":{"score":79}}}
      }}
    }}
    (tmp_path/"job1"/"scores").mkdir()
    (tmp_path/"job1"/"scores"/"scorecard.json").write_text(json.dumps(score), encoding="utf-8")

    out = build_overview(str(tmp_path/"job1"))
    assert out["overall"]["score"] == 82
    s = out["groups"]["nonko_9max_pref"]["subgroups"]["PREFLOP_RFI"]["stats"][0]
    assert s["name"] == "RFI_EARLY"
    assert s["opps"] > 0 and s["pct"] is not None

def test_overview_empty_stats(tmp_path):
    """Testa dashboard com stats vazias (0 oportunidades)"""
    job = tmp_path / "job2"
    job.mkdir(parents=True)
    
    # counts vazio
    counts = {"counts": {
        "2025-06": {"nonko_6max": {"RFI_LATE": {"opportunities": 0, "attempts": 0}}}
    }}
    (job/"stat_counts.json").write_text(json.dumps(counts), encoding="utf-8")
    
    # scorecard com score mas sem amostras
    score = {"overall": None, "group_level": {
        "nonko_6max": {"weight": 0.5, "score": None, "subgroups": {
            "RFI": {"score": None, "stats": {"RFI_LATE": {"score": None}}}
        }}
    }}
    (job/"scores").mkdir()
    (job/"scores"/"scorecard.json").write_text(json.dumps(score), encoding="utf-8")
    
    out = build_overview(str(job))
    assert out["overall"] is None
    s = out["groups"]["nonko_6max"]["subgroups"]["RFI"]["stats"][0]
    assert s["name"] == "RFI_LATE"
    assert s["opps"] == 0
    assert s["pct"] is None  # Deve ser None quando opps == 0
    assert s["grade"] == "-"  # Grade padrão para sem score

def test_overview_time_decay(tmp_path):
    """Testa aplicação correta dos weights de time-decay"""
    job = tmp_path / "job3"
    job.mkdir(parents=True)
    
    # 3 meses de dados
    counts = {"counts": {
        "2025-06": {"nonko_9max_pref": {"VPIP": {"opportunities": 100, "attempts": 20}}},  # 20%
        "2025-05": {"nonko_9max_pref": {"VPIP": {"opportunities": 100, "attempts": 30}}},  # 30%
        "2025-04": {"nonko_9max_pref": {"VPIP": {"opportunities": 100, "attempts": 40}}}   # 40%
    }}
    (job/"stat_counts.json").write_text(json.dumps(counts), encoding="utf-8")
    
    score = {"overall": {"score": 75}, "group_level": {
        "nonko_9max_pref": {"weight": 1.0, "score": 75, "subgroups": {
            "VPIP": {"score": 75, "stats": {"VPIP": {"score": 75}}}
        }}
    }}
    (job/"scores").mkdir()
    (job/"scores"/"scorecard.json").write_text(json.dumps(score), encoding="utf-8")
    
    out = build_overview(str(job))
    
    # Verifica weights aplicados (50%, 30%, 20%)
    assert out["weights"] == [0.5, 0.3, 0.2]
    
    # Verifica cálculo weighted: (20*0.5 + 30*0.3 + 40*0.2) = 10 + 9 + 8 = 27%
    s = out["groups"]["nonko_9max_pref"]["subgroups"]["VPIP"]["stats"][0]
    assert s["name"] == "VPIP"
    assert abs(s["pct"] - 27.0) < 0.01  # 27% com tolerância
    
    # Verifica amostras weighted: (100*0.5 + 100*0.3 + 100*0.2) = 100
    assert s["opps"] == 100
    assert s["attempts"] == 27