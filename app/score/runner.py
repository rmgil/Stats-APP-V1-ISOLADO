"""
Runner with minimum samples, cache and CSV export
"""
import os
import json
import time
import csv
import logging
import hashlib
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple

from app.score.loader import load_config, config_hash
from app.score.time_decay import weights_for_n, apply_time_decay
from app.score.combine import combine_nonko_stat, NONKO_6, NONKO_9, NONKO_COMBINED
from app.score.scoring import pick_scorer, explain_stat

logger = logging.getLogger("score.runner")

def _months_sorted(counts: dict) -> List[str]:
    """Get months sorted in reverse (most recent first)"""
    ms = list(counts.keys())
    ms.sort()
    return ms[::-1]  # mais recente primeiro

def _safe_get(d, *path, default=None):
    """Safe nested dict navigation"""
    cur = d
    for k in path:
        if cur is None: return default
        cur = cur.get(k)
    return default if cur is None else cur

def _cache_key(stat_counts_path: str, cfg_hash: str) -> Dict:
    """Generate cache key from file stats and config hash"""
    st = os.stat(stat_counts_path)
    return {
        "stat_counts_path": os.path.abspath(stat_counts_path),
        "stat_counts_mtime": st.st_mtime,
        "cfg_hash": cfg_hash,
    }

def _cache_ok(cache_path: str, key: Dict) -> Tuple[bool, Dict]:
    """Check if cache is valid"""
    if not (cache_path and os.path.exists(cache_path)): return (False, {})
    try:
        data = json.load(open(cache_path, "r", encoding="utf-8"))
        return (data.get("key") == key, data)
    except Exception:
        return (False, {})

def _write_cache(cache_path: str, key: Dict, scorecard_path: str, payload: Dict):
    """Write cache file"""
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        json.dump({"key": key, "scorecard_path": scorecard_path, "payload": payload},
                  open(cache_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"[cache] write failed: {e}")

def _export_csvs(out_dir: str, stat_level: dict, subgroup_level: dict, group_level: dict, overall):
    """Export results to CSV files"""
    ex_dir = os.path.join(out_dir, "exports")
    os.makedirs(ex_dir, exist_ok=True)

    # stat_level.csv
    with open(os.path.join(ex_dir, "stat_level.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["stat_id","group","pct_time_decay","score_time_decay","months_used"])
        for sid, gmap in stat_level.items():
            for g, rec in gmap.items():
                w.writerow([sid, g, rec["pct_time_decay"], rec["score_time_decay"], rec["months_used"]])

    # subgroup_level.csv
    with open(os.path.join(ex_dir, "subgroup_level.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["subgroup","group","score"])
        for sg, gmap in subgroup_level.items():
            for g, val in gmap.items():
                w.writerow([sg, g, val])

    # group_level.csv
    with open(os.path.join(ex_dir, "group_level.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["group","score"])
        for g, val in group_level.items():
            w.writerow([g, val])

    # overall.txt
    with open(os.path.join(ex_dir, "overall.txt"), "w", encoding="utf-8") as f:
        f.write(str(overall if overall is not None else ""))

def build_scorecard(stat_counts_path: str, cfg_path: str, out_dir: str = "scores", force: bool = False) -> Dict:
    """
    Build scorecard from stat counts and configuration
    
    Args:
        stat_counts_path: Path to stat_counts.json
        cfg_path: Path to config.yml
        out_dir: Output directory
        force: Force rebuild (ignore cache)
        
    Returns:
        Dict with scorecard_path and overall score
    """
    os.makedirs(out_dir, exist_ok=True)
    cfg = load_config(cfg_path)
    stat_counts = json.load(open(stat_counts_path, "r", encoding="utf-8"))
    counts = stat_counts.get("counts", {}) or {}

    # cache
    cache_cfg = cfg.get("cache", {})
    cache_enabled = bool(cache_cfg.get("enabled", True))
    cache_path = cache_cfg.get("path", os.path.join(out_dir, ".cache.json"))
    key = _cache_key(stat_counts_path, config_hash(cfg))
    if cache_enabled and not force:
        ok, cached = _cache_ok(cache_path, key)
        if ok and os.path.exists(cached.get("scorecard_path","")):
            logger.info("[cache] hit")
            return {"scorecard_path": cached["scorecard_path"], "overall": cached["payload"].get("overall")}

    months = _months_sorted(counts)

    # config shorthands
    w3, w2, w1 = cfg["time_decay"]["weights_3"], cfg["time_decay"]["weights_2"], cfg["time_decay"]["weights_1"]
    combine_by = cfg["nonko_combine"]["by"]
    sc = cfg["scoring"]; mode = sc.get("mode","step"); defaults = sc["default"]
    scorer = pick_scorer(mode)
    ideals = cfg["ideals"]
    w_groups = cfg["weights"]["groups"]
    w_subgroups = cfg["weights"]["subgroups"]
    w_stats = cfg["weights"]["stats"]

    min_total = int(defaults.get("min_opportunities_total", 0))
    min_month = int(defaults.get("min_opportunities_month", 0))

    # 1) Stat level (por group)
    stat_level = defaultdict(dict)  # sid -> group -> rec
    for sid, ideal_map in ideals.items():
        # grupos com percentagem própria
        for group in [NONKO_9, NONKO_6, "pko_pref"]:
            per_month_pct = []
            per_month_score = []
            used = 0
            # coleciona 3 meses mais recentes com opp>=min_month
            for m in months:
                node = _safe_get(stat_counts, "counts", m, group, sid, default=None)
                if not node: continue
                opp = int(node.get("opportunities", 0))
                if opp < min_month: 
                    continue
                pct = float(node.get("percentage", 0.0))
                per_month_pct.append((pct, 1.0))
                ideal = ideal_map.get(group, ideal_map.get(NONKO_COMBINED, pct))
                s = scorer(pct, ideal,
                           defaults["step_down_pct"], defaults["step_up_pct"],
                           defaults["points_per_step_down"], defaults["points_per_step_up"])
                per_month_score.append((s, 1.0))
                used += 1
                if used == 3: break

            if not per_month_pct:
                # Sem meses com amostra suficiente
                continue

            ws = weights_for_n(len(per_month_pct), w3, w2, w1)
            pct_td   = apply_time_decay(per_month_pct, ws)
            score_td = apply_time_decay(per_month_score, ws)

            # Add grade and note
            grade, note = explain_stat(sid, pct_td, cfg)
            stat_level[sid][group] = {
                "pct_time_decay": round(pct_td, 2),
                "score_time_decay": round(score_td, 2),
                "months_used": len(per_month_pct),
                "grade": grade,
                "note": note
            }

        # NON‑KO combinado (9+6) por opportunities
        per_month_combined_pct = []
        per_month_combined_score = []
        used = 0
        for m in months:
            o, a, pct = combine_nonko_stat(stat_counts, m, sid, by=combine_by)
            if o < min_month: 
                continue
            per_month_combined_pct.append((pct, 1.0))
            ideal = ideal_map.get(NONKO_COMBINED, ideal_map.get(NONKO_9, pct))
            s = scorer(pct, ideal,
                       defaults["step_down_pct"], defaults["step_up_pct"],
                       defaults["points_per_step_down"], defaults["points_per_step_up"])
            per_month_combined_score.append((s, 1.0))
            used += 1
            if used == 3: break

        if per_month_combined_pct:
            ws = weights_for_n(len(per_month_combined_pct), w3, w2, w1)
            pct_td   = apply_time_decay(per_month_combined_pct, ws)
            score_td = apply_time_decay(per_month_combined_score, ws)
            # Add grade and note
            grade, note = explain_stat(sid, pct_td, cfg)
            stat_level[sid][NONKO_COMBINED] = {
                "pct_time_decay": round(pct_td, 2),
                "score_time_decay": round(score_td, 2),
                "months_used": len(per_month_combined_pct),
                "grade": grade,
                "note": note
            }

    # 2) Sub‑grupo RFI: média ponderada por pesos de stat
    subgroup_level = {"RFI": {}}
    for group in [NONKO_COMBINED, NONKO_9, NONKO_6, "pko_pref"]:
        parts = []
        for sid in ["RFI_EARLY","RFI_MIDDLE","RFI_CO_STEAL","RFI_BTN_STEAL"]:
            rec = stat_level.get(sid, {}).get(group)
            if not rec: continue
            w = float(w_stats.get(sid, 0.0))
            if w <= 0: continue
            parts.append((rec["score_time_decay"], w))
        if parts and sum(w for _,w in parts) > 0:
            s = sum(v*w for v,w in parts) / sum(w for _,w in parts)
            subgroup_level["RFI"][group] = round(s, 2)

    # 3) Grupo: aplica pesos de sub‑grupos
    group_level = {}
    for group in [NONKO_COMBINED, "pko_pref", "postflop_all"]:
        rfi = subgroup_level["RFI"].get(group)
        if rfi is None: continue
        score = rfi * float(w_subgroups.get("RFI", 1.0))
        group_level[group] = round(score, 2)

    # 4) Overall
    overall = None
    if group_level and sum(w_groups.values()) > 0:
        parts = [(group_level.get("nonko_pref",0.0), float(w_groups.get("nonko_pref",0.0))),
                 (group_level.get("pko_pref",0.0),   float(w_groups.get("pko_pref",0.0))),
                 (group_level.get("postflop_all",0.0),float(w_groups.get("postflop_all",0.0)))]
        denom = sum(w for _,w in parts if w>0)
        if denom > 0:
            overall = round(sum(v*w for v,w in parts)/denom, 2)

    out = {
        "generated_at": datetime.utcnow().isoformat()+"Z",
        "inputs": {"stat_counts": stat_counts_path, "config": cfg_path},
        "weights": cfg["weights"],
        "time_decay": cfg["time_decay"],
        "nonko_combine_by": combine_by,
        "min_sample": {"total": min_total, "per_month": min_month},
        "stat_level": stat_level,
        "subgroup_level": subgroup_level,
        "group_level": group_level,
        "overall": overall
    }
    out_path = os.path.join(out_dir, "scorecard.json")
    json.dump(out, open(out_path,"w",encoding="utf-8"), ensure_ascii=False, indent=2)

    # export CSVs
    _export_csvs(out_dir, stat_level, subgroup_level, group_level, overall)

    # cache write
    cache_path = cfg.get("cache", {}).get("path", os.path.join(out_dir, ".cache.json"))
    _write_cache(cache_path, key, out_path, out)

    return {"scorecard_path": out_path, "overall": overall}