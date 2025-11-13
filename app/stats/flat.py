# app/stats/flat.py
from __future__ import annotations
import json, os
from typing import Any, Dict, List, Tuple
from app.score.scoring import score_to_note

def _load(path: str, default: Any):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def build_flat_response(base_dir: str = ".") -> Dict[str, Any]:
    # fontes base
    counts = _load(os.path.join(base_dir, "stat_counts.json"), {})
    scorecard = _load(os.path.join(base_dir, "scores", "scorecard.json"), {})
    cfg = _load(os.path.join(base_dir, "app", "score", "config.yml"), {})  # opcional, se usares YAML já carregado noutro sítio

    # tenta descobrir último mês com dados
    months = []
    if isinstance(counts, dict) and "counts" in counts:
        months = sorted(list(counts["counts"].keys()))
    last_month = months[-1] if months else None

    rows: List[Dict[str, Any]] = []

    # 1) aplanar counts (stat × group × month)
    if last_month:
        month_bucket = counts["counts"][last_month]
        for group, gnode in month_bucket.items():
            for stat_key, node in gnode.items():
                opp = node.get("opportunities", 0) or 0
                att = node.get("attempts", 0) or 0
                pct = (att / opp * 100.0) if opp else None
                rows.append({
                    "month": last_month,
                    "group": group,
                    "stat_key": stat_key,
                    "opportunities": opp,
                    "attempts": att,
                    "pct": round(pct, 2) if pct is not None else None,
                    # placeholders; vamos completar com scoring a seguir
                    "subgroup": None, "label": stat_key, "ideal_low": None, "ideal_high": None,
                    "weight": None, "score": None, "note": None
                })

    # 2) enriquecer com scoring (ideais, weights, score por stat)
    # Estrutura do scorecard pode variar; procurar de modo defensivo:
    # - group_level[group]["stats"][stat_key] OU
    # - stat_level[stat_key]["groups"][group] OU
    # - outro nó equivalente. Caímos no que existir.
    sc = scorecard or {}
    for r in rows:
        g = r["group"]; s = r["stat_key"]
        node = None

        # tentativas de localização
        gl = (sc.get("group_level", {}) or {}).get(g, {})
        if isinstance(gl, dict):
            node = ((gl.get("stats", {}) or {}).get(s)) or ((gl.get("subgroups", {}) or {}).get(s))

        if node is None:
            sl = (sc.get("stat_level", {}) or {}).get(s, {})
            if isinstance(sl, dict):
                node = ((sl.get("groups", {}) or {}).get(g))

        # ler campos
        if isinstance(node, dict):
            r["score"] = node.get("score")
            r["weight"] = node.get("weight") or node.get("w")
            ideal = node.get("ideal") or {}
            if isinstance(ideal, dict):
                r["ideal_low"] = ideal.get("low")
                r["ideal_high"] = ideal.get("high")
            r["subgroup"] = node.get("subgroup") or node.get("family") or node.get("bucket")

        # label amigável
        if r["label"] == r["stat_key"]:
            # transformar p.ex. RFI_EARLY → "RFI — Early"
            parts = r["stat_key"].split("_")
            if len(parts) > 1:
                r["label"] = f"{parts[0]} — {' '.join(p.title() for p in parts[1:])}"

        r["note"] = score_to_note(r.get("score"))

    meta = {
        "month": last_month,
        "overall": sc.get("overall"),
        "sample": sc.get("sample", {}),
    }
    return {"meta": meta, "rows": rows}