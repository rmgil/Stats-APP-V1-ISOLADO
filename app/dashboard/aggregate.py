# app/dashboard/aggregate.py
from __future__ import annotations
import json, os
from typing import Dict, Any, List, Tuple

# Pesos padrão (3, 2, 1 meses). Re-normaliza se houver menos meses.
DECAY_3 = [0.50, 0.30, 0.20]
DECAY_2 = [0.50, 0.50]
DECAY_1 = [1.00]

def _weights(n: int) -> List[float]:
    if n <= 1: return DECAY_1[:n]
    if n == 2: return DECAY_2
    w = DECAY_3
    return (w[:n] if n <= 3 else [0.50, 0.30, 0.20][:3])  # limita a 3

def _sorted_months(counts: Dict[str, Any]) -> List[str]:
    # meses "YYYY-MM" do mais recente para o mais antigo
    return sorted(counts.keys(), reverse=True)

def _wd_ratio(parts: List[Tuple[int,int]], weights: List[float]) -> Tuple[float,int,int]:
    """Weighted attempts/opps. Retorna (pct, attempts, opps)."""
    num = den = att = opp = 0.0
    for (a,o), w in zip(parts, weights):
        att += a * w
        opp += o * w
    if opp <= 0: return (None, int(att), int(opp))
    pct = (att / opp) * 100.0
    return (pct, int(att), int(opp))

def _grade(score: float) -> str:
    if score is None: return "-"
    if score >= 90: return "A"
    if score >= 80: return "B"
    if score >= 70: return "C"
    if score >= 60: return "D"
    return "E"

def load_job_paths(job_dir: str) -> Dict[str,str]:
    # stat_counts.json pode estar em stats/ ou na raiz
    counts_path = os.path.join(job_dir, "stats", "stat_counts.json")
    if not os.path.exists(counts_path):
        counts_path = os.path.join(job_dir, "stat_counts.json")
    
    # ids_dir também pode estar em stats/index ou index/
    ids_dir = os.path.join(job_dir, "stats", "index")
    if not os.path.exists(ids_dir):
        ids_dir = os.path.join(job_dir, "index")
    
    paths = {
        "counts": counts_path,
        "score":  os.path.join(job_dir, "scores", "scorecard.json"),
        "ids_dir": ids_dir,
    }
    
    # Verifica apenas os arquivos obrigatórios
    for k,p in paths.items():
        if k != "ids_dir" and not os.path.exists(p):
            raise FileNotFoundError(f"Artefato ausente: {p}")
    return paths

def build_overview(job_dir: str) -> Dict[str, Any]:
    p = load_job_paths(job_dir)
    with open(p["counts"], "r", encoding="utf-8") as f:
        counts = json.load(f).get("counts", {})  # Fase 5 shape

    # scorecard com groups/subgrupos/pesos/nota
    with open(p["score"], "r", encoding="utf-8") as f:
        scorecard = json.load(f)

    months = _sorted_months(counts)
    ws = _weights(min(3, len(months)))

    def stat_pct(group: str, stat: str) -> Tuple[float,int,int]:
        parts = []
        for i, m in enumerate(months[:len(ws)]):
            g = counts.get(m, {}).get(group, {})
            s = g.get(stat, {})
            a = int(s.get("attempts", 0))
            o = int(s.get("opportunities", 0))
            parts.append((a,o))
        return _wd_ratio(parts, ws)

    # Mapa score -> group -> stat; e vincular subgrupos a partir do scorecard
    out = {
        "job_dir": job_dir,
        "months": months,
        "weights": ws,
        "overall": scorecard.get("overall"),
        "groups": {},
        "sample": scorecard.get("sample", {}),
    }

    # O scorecard guarda a hierarquia (group_level -> subgroups -> stats)
    group_level = scorecard.get("group_level", {})
    for group, gdata in group_level.items():
        gscore = gdata.get("score")
        gweight = gdata.get("weight")
        subgroups = gdata.get("subgroups", {})
        gentry = {"score": gscore, "weight": gweight, "subgroups": {}}
        for sgrp, sdata in subgroups.items():
            sscore = sdata.get("score")
            stats = sdata.get("stats", {})
            sentry = {"score": sscore, "stats": []}
            for stat_name, meta in stats.items():
                pct, att_w, opp_w = stat_pct(group, stat_name)
                note = meta.get("score")  # 0-100 da Fase 6
                sentry["stats"].append({
                    "name": stat_name,
                    "pct": pct, "attempts": att_w, "opps": opp_w,
                    "score": note,
                    "grade": _grade(note if isinstance(note,(int,float)) else None),
                    "ids": {  # caminhos dos id-files p/ click-through (se existirem)
                        "opps": f"{p['ids_dir']}/{months[0]}__{group}__{stat_name}__opps.ids" if months else None,
                        "attempts": f"{p['ids_dir']}/{months[0]}__{group}__{stat_name}__attempts.ids" if months else None
                    }
                })
            gentry["subgroups"][sgrp] = sentry
        out["groups"][group] = gentry
    return out