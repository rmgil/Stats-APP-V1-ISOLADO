# app/stats/engine.py
import os
import json
import yaml
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from app.partition.groups import groups_for_hand
from app.partition.months import month_bucket, make_hand_id

logger = logging.getLogger(__name__)

def build_context(hand: dict) -> Dict[str, Any]:
    """
    Build complete context from hand with all derived fields.
    Includes comprehensive logging for missing fields.
    """
    derived = hand.get("derived", {}) or {}
    positions = derived.get("positions", {}) or {}
    preflop   = derived.get("preflop", {}) or {}
    ip        = derived.get("ip", {}) or {}
    stacks    = derived.get("stacks", {}) or {}
    flags     = derived.get("flags", {}) or {}
    post      = derived.get("postflop", {}) or {}

    hero = hand.get("hero")
    hero_pos_group = positions.get("pos_group", {}).get(hero) if hero else None
    hero_position  = positions.get("abs_positions", {}).get(hero) if hero else None
    
    # Log missing hero data
    if not hero:
        logger.debug(f"Hand {hand.get('hand_id', 'unknown')}: No hero defined")

    ctx = {
        # ========== HERO & META ==========
        "hero": hero,
        "hero_pos_group": hero_pos_group,
        "hero_position":  hero_position,
        "hand_id": make_hand_id(hand),
        "month": month_bucket(
            hand.get("timestamp_utc", ""),
            fallback_month=hand.get("month"),
            debug_context=f"engine:{hand.get('hand_id', 'unknown')}",
        ),
        "groups": groups_for_hand(hand),
        
        # ========== PREFLOP CORE ==========
        "unopened_pot": preflop.get("unopened_pot", False),
        "pot_type": preflop.get("pot_type", "none"),
        "hero_raised_first_in": preflop.get("hero_raised_first_in", False),
        "hero_vpip": preflop.get("hero_vpip", False),
        "faced_3bet": preflop.get("faced_3bet", False),
        "folded_to_3bet": preflop.get("folded_to_3bet", False),
        "is_squeeze": preflop.get("is_squeeze", False),
        "is_resteal_vs_btn": preflop.get("is_resteal_vs_btn", False),
        "pfr_player": post.get("pfr_player"),
        "open_raiser": preflop.get("open_raiser"),
        "three_bettor": preflop.get("three_bettor"),
        
        # ========== IP/OOP & MULTIWAY ==========
        # Heads-up flags by street
        "heads_up_flop": ip.get("heads_up_flop", False) or post.get("heads_up_flop", False),
        "heads_up_turn": ip.get("heads_up_turn", False) or post.get("heads_up_turn", False),
        "heads_up_river": ip.get("heads_up_river", False) or post.get("heads_up_river", False),
        
        # Hero IP/OOP by street
        "hero_ip_flop": ip.get("hero_ip_flop") if ip.get("hero_ip_flop") is not None else post.get("hero_ip_flop"),
        "hero_ip_turn": ip.get("hero_ip_turn") if ip.get("hero_ip_turn") is not None else post.get("hero_ip_turn"),
        "hero_ip_river": ip.get("hero_ip_river") if ip.get("hero_ip_river") is not None else post.get("hero_ip_river"),
        
        # Players to street
        "players_to_flop": ip.get("players_to_flop", 0),
        "players_to_turn": ip.get("players_to_turn", 0),
        "players_to_river": ip.get("players_to_river", 0),
        
        # ========== STACKS ==========
        "eff_stack_srp": stacks.get("eff_stack_bb_srp"),
        "eff_stack_vs_3bet": stacks.get("eff_stack_bb_vs_3bettor"),
        
        # Stack validation fields for stat filtering
        "hero_stack_bb": stacks.get("hero_stack_bb"),
        "avg_stacks_after_hero_bb": stacks.get("avg_stacks_after_hero_bb"),
        "raiser_stack_bb": stacks.get("raiser_stack_bb"),
        "three_bettor_stack_bb": stacks.get("three_bettor_stack_bb"),
        "squeeze_avg_stack_bb": stacks.get("squeeze_avg_stack_bb"),
        "bvb_villain_stack_bb": stacks.get("bvb_villain_stack_bb"),
        
        # ========== FLAGS ==========
        "any_allin_preflop": flags.get("any_allin_preflop", False),
        
        # ========== POSTFLOP STREETS VISIBILITY ==========
        "saw_flop": post.get("saw_flop", False),
        "saw_turn": post.get("saw_turn", False),
        "saw_river": post.get("saw_river", False),
        
        # ========== CBET FLOP ==========
        "cbet_flop_opp_ip": post.get("cbet_flop_opp_ip", False),
        "cbet_flop_att_ip": post.get("cbet_flop_att_ip", False),
        "cbet_flop_opp_oop": post.get("cbet_flop_opp_oop", False),
        "cbet_flop_att_oop": post.get("cbet_flop_att_oop", False),
        
        # ========== CBET TURN ==========
        "cbet_turn_opp_ip": post.get("cbet_turn_opp_ip", False),
        "cbet_turn_att_ip": post.get("cbet_turn_att_ip", False),
        "cbet_turn_opp_oop": post.get("cbet_turn_opp_oop", False),
        "cbet_turn_att_oop": post.get("cbet_turn_att_oop", False),
        
        # ========== CBET RIVER ==========
        "cbet_river_opp_ip": post.get("cbet_river_opp_ip", False),
        "cbet_river_att_ip": post.get("cbet_river_att_ip", False),
        "cbet_river_opp_oop": post.get("cbet_river_opp_oop", False),
        "cbet_river_att_oop": post.get("cbet_river_att_oop", False),
        
        # ========== VS CBET FLOP ==========
        "vs_cbet_flop_fold_ip": post.get("vs_cbet_flop_fold_ip", False),
        "vs_cbet_flop_call_ip": post.get("vs_cbet_flop_call_ip", False),
        "vs_cbet_flop_raise_ip": post.get("vs_cbet_flop_raise_ip", False),
        "vs_cbet_flop_fold_oop": post.get("vs_cbet_flop_fold_oop", False),
        "vs_cbet_flop_call_oop": post.get("vs_cbet_flop_call_oop", False),
        "vs_cbet_flop_raise_oop": post.get("vs_cbet_flop_raise_oop", False),
        
        # ========== VS CBET TURN ==========
        "vs_cbet_turn_fold_ip": post.get("vs_cbet_turn_fold_ip", False),
        "vs_cbet_turn_call_ip": post.get("vs_cbet_turn_call_ip", False),
        "vs_cbet_turn_raise_ip": post.get("vs_cbet_turn_raise_ip", False),
        "vs_cbet_turn_fold_oop": post.get("vs_cbet_turn_fold_oop", False),
        "vs_cbet_turn_call_oop": post.get("vs_cbet_turn_call_oop", False),
        "vs_cbet_turn_raise_oop": post.get("vs_cbet_turn_raise_oop", False),
        
        # ========== VS CBET RIVER ==========
        "vs_cbet_river_fold_ip": post.get("vs_cbet_river_fold_ip", False),
        "vs_cbet_river_call_ip": post.get("vs_cbet_river_call_ip", False),
        "vs_cbet_river_raise_ip": post.get("vs_cbet_river_raise_ip", False),
        "vs_cbet_river_fold_oop": post.get("vs_cbet_river_fold_oop", False),
        "vs_cbet_river_call_oop": post.get("vs_cbet_river_call_oop", False),
        "vs_cbet_river_raise_oop": post.get("vs_cbet_river_raise_oop", False),
        
        # ========== PROBE BETTING ==========
        "probe_flop_opp_ip": post.get("probe_flop_opp_ip", False),
        "probe_flop_att_ip": post.get("probe_flop_att_ip", False),
        "probe_flop_opp_oop": post.get("probe_flop_opp_oop", False),
        "probe_flop_att_oop": post.get("probe_flop_att_oop", False),
        "probe_turn_opp_ip": post.get("probe_turn_opp_ip", False),
        "probe_turn_att_ip": post.get("probe_turn_att_ip", False),
        "probe_turn_opp_oop": post.get("probe_turn_opp_oop", False),
        "probe_turn_att_oop": post.get("probe_turn_att_oop", False),
        "probe_river_opp_ip": post.get("probe_river_opp_ip", False),
        "probe_river_att_ip": post.get("probe_river_att_ip", False),
        "probe_river_opp_oop": post.get("probe_river_opp_oop", False),
        "probe_river_att_oop": post.get("probe_river_att_oop", False),
        
        # ========== DELAYED CBET ==========
        "delayed_cbet_turn_opp_ip": post.get("delayed_cbet_turn_opp_ip", False),
        "delayed_cbet_turn_att_ip": post.get("delayed_cbet_turn_att_ip", False),
        "delayed_cbet_turn_opp_oop": post.get("delayed_cbet_turn_opp_oop", False),
        "delayed_cbet_turn_att_oop": post.get("delayed_cbet_turn_att_oop", False),
        "delayed_cbet_river_opp_ip": post.get("delayed_cbet_river_opp_ip", False),
        "delayed_cbet_river_att_ip": post.get("delayed_cbet_river_att_ip", False),
        "delayed_cbet_river_opp_oop": post.get("delayed_cbet_river_opp_oop", False),
        "delayed_cbet_river_att_oop": post.get("delayed_cbet_river_att_oop", False),
        
        # ========== DONK BETTING ==========
        "donk_flop": post.get("donk_flop", False),
        "donk_flop_opp": post.get("donk_flop_opp", False),
        "donk_flop_att": post.get("donk_flop_att", False),
        "donk_turn": post.get("donk_turn", False),
        "donk_turn_opp": post.get("donk_turn_opp", False),
        "donk_turn_att": post.get("donk_turn_att", False),
        "donk_river_opp": post.get("donk_river_opp", False),
        "donk_river_att": post.get("donk_river_att", False),
        
        # ========== CHECK-RAISE ==========
        "xr_flop_opp": post.get("xr_flop_opp", False),
        "xr_flop_att": post.get("xr_flop_att", False),
        "xr_turn_opp": post.get("xr_turn_opp", False),
        "xr_turn_att": post.get("xr_turn_att", False),
        "xr_river_opp": post.get("xr_river_opp", False),
        "xr_river_att": post.get("xr_river_att", False),
        
        # ========== BET VS MISSED CBET ==========
        "flop_bet_vs_missed_cbet_srp": post.get("flop_bet_vs_missed_cbet_srp", False),
        "turn_bet_vs_missed_cbet_srp_oop": post.get("turn_bet_vs_missed_cbet_srp_oop", False),
        "bet_vs_missed_flop_opp_ip": post.get("bet_vs_missed_flop_opp_ip", False),
        "bet_vs_missed_flop_att_ip": post.get("bet_vs_missed_flop_att_ip", False),
        "bet_vs_missed_flop_opp_oop": post.get("bet_vs_missed_flop_opp_oop", False),
        "bet_vs_missed_flop_att_oop": post.get("bet_vs_missed_flop_att_oop", False),
        "bet_vs_missed_turn_opp_ip": post.get("bet_vs_missed_turn_opp_ip", False),
        "bet_vs_missed_turn_att_ip": post.get("bet_vs_missed_turn_att_ip", False),
        "bet_vs_missed_turn_opp_oop": post.get("bet_vs_missed_turn_opp_oop", False),
        "bet_vs_missed_turn_att_oop": post.get("bet_vs_missed_turn_att_oop", False),
        "bet_vs_missed_river_opp_ip": post.get("bet_vs_missed_river_opp_ip", False),
        "bet_vs_missed_river_att_ip": post.get("bet_vs_missed_river_att_ip", False),
        "bet_vs_missed_river_opp_oop": post.get("bet_vs_missed_river_opp_oop", False),
        "bet_vs_missed_river_att_oop": post.get("bet_vs_missed_river_att_oop", False),
        
        # ========== AGGRESSION FREQUENCY ==========
        "agg_pct_flop": post.get("agg_pct_flop"),
        "agg_pct_turn": post.get("agg_pct_turn"),
        "agg_pct_river": post.get("agg_pct_river"),
        "river_agg_pct": post.get("river_agg_pct") or post.get("agg_pct_river"),
        
        # ========== SHOWDOWN METRICS ==========
        "saw_showdown": post.get("saw_showdown", False),
        "won_showdown": post.get("won_showdown", False),
        "won_when_saw_flop": post.get("won_when_saw_flop", False),
        "wtsd": post.get("wtsd", False),
        "w_sd": post.get("w_sd", False),
        "w_wsf": post.get("w_wsf", False),
        
        # ========== FOLD VS CHECK-RAISE ==========
        "fold_vs_check_raise_opp": post.get("fold_vs_check_raise_opp", False),
        "fold_vs_check_raise_att": post.get("fold_vs_check_raise_att", False),
        
        # ========== RIVER BET SRP ==========
        "river_bet_srp_opp": post.get("river_bet_srp_opp", False),
        "river_bet_srp_att": post.get("river_bet_srp_att", False),
        
        # ========== WON SD AFTER BET RIVER ==========
        "w_sd_b_river_opp": post.get("w_sd_b_river_opp", False),
        "w_sd_b_river_att": post.get("w_sd_b_river_att", False),
    }
    
    # Log warnings for critical missing fields
    if ctx["saw_flop"] and ctx["hero"]:
        # Check multiway gating
        if ctx["hero_ip_flop"] is None:
            logger.debug(f"Hand {ctx['hand_id']}: hero_ip_flop not set despite seeing flop")
        
        # Check if heads_up flags are consistent
        if ctx["players_to_flop"] == 2 and not ctx["heads_up_flop"]:
            logger.warning(f"Hand {ctx['hand_id']}: 2 players to flop but heads_up_flop=False")
        
        # Log missing stack data
        if ctx["pot_type"] == "SRP" and ctx["eff_stack_srp"] is None:
            logger.debug(f"Hand {ctx['hand_id']}: Missing eff_stack_srp for SRP pot")
    
    return ctx

def eval_clause(clause: Any, ctx: Dict[str, Any]) -> bool:
    if clause is None:
        return False
    if isinstance(clause, bool):
        return clause
    if isinstance(clause, str):
        return bool(ctx.get(clause, False))
    if isinstance(clause, dict):
        if "all" in clause:
            return all(eval_clause(c, ctx) for c in clause["all"])
        if "any" in clause:
            return any(eval_clause(c, ctx) for c in clause["any"])
        if "not" in clause:
            return not eval_clause(clause["not"], ctx)
        if "eq" in clause:
            k, v = clause["eq"]
            return ctx.get(k) == v
        if "in" in clause:
            k, arr = clause["in"]
            return ctx.get(k) in arr
        if "gte" in clause:
            k, v = clause["gte"]
            x = ctx.get(k)
            return (x is not None) and (x >= v)
        if "lte" in clause:
            k, v = clause["lte"]
            x = ctx.get(k)
            return (x is not None) and (x <= v)
        if "gt" in clause:
            k, v = clause["gt"]
            x = ctx.get(k)
            return (x is not None) and (x > v)
        if "lt" in clause:
            k, v = clause["lt"]
            x = ctx.get(k)
            return (x is not None) and (x < v)
        if "is_true" in clause:
            return bool(ctx.get(clause["is_true"], False))
        if "is_false" in clause:
            return not bool(ctx.get(clause["is_false"], True))  # missing -> False
    # Handle list of conditions (from YAML parsing)
    if isinstance(clause, list):
        # Treat as AND condition
        return all(eval_condition(c, ctx) for c in clause)
    logger.warning(f"[DSL] Unknown clause: {clause}")
    return False

def eval_condition(cond: Any, ctx: Dict[str, Any]) -> bool:
    """Helper for evaluating individual conditions from lists."""
    if isinstance(cond, dict):
        # Check for operators
        for key in cond:
            if key in ["eq", "in", "gte", "lte", "gt", "lt", "is_true", "is_false", "not"]:
                return eval_clause(cond, ctx)
    elif isinstance(cond, list) and len(cond) == 2:
        # Handle ["key", "value"] format as eq
        if cond[0] == "eq" and isinstance(cond[1], list) and len(cond[1]) == 2:
            return ctx.get(cond[1][0]) == cond[1][1]
        elif cond[0] == "is_true" and isinstance(cond[1], str):
            return bool(ctx.get(cond[1], False))
    return eval_clause(cond, ctx)

def pass_filters(stat: dict, ctx: Dict[str, Any]) -> bool:
    f = stat.get("filters", {}) or {}
    
    # Heads-up filter with multiway gating
    if f.get("heads_up_only"):
        # Check heads-up on the relevant street
        scope = stat.get("scope", "preflop")
        if scope == "postflop":
            # For postflop stats, check the first street with action
            if ctx.get("saw_flop") and not ctx.get("heads_up_flop", False):
                logger.debug(f"Stat {stat.get('id')}: Filtered out - not heads-up on flop")
                return False
        elif not ctx.get("heads_up_flop", False):
            return False
    
    # Pot type filter
    allowed = f.get("pot_type")
    if allowed and ctx.get("pot_type") not in allowed:
        logger.debug(f"Stat {stat.get('id')}: Filtered out - pot_type {ctx.get('pot_type')} not in {allowed}")
        return False
    
    # Stack filter
    min_bb = f.get("eff_stack_min_bb")
    if min_bb is not None:
        eff = ctx.get("eff_stack_srp")
        if eff is None or eff < float(min_bb):
            logger.debug(f"Stat {stat.get('id')}: Filtered out - stack {eff} < {min_bb}")
            return False
    
    # All-in preflop filter
    if f.get("exclude_allin_preflop") and ctx.get("any_allin_preflop", False):
        logger.debug(f"Stat {stat.get('id')}: Filtered out - all-in preflop")
        return False
    
    return True

def load_catalog(yaml_path: str) -> dict:
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"DSL catalog not found: {yaml_path}")
    with open(yaml_path, "r", encoding="utf-8") as f:
        cat = yaml.safe_load(f)
    if not cat.get("stats"):
        raise ValueError("DSL catalog missing 'stats'")
    return cat

def ensure_dirs(*paths): 
    for p in paths: 
        os.makedirs(p, exist_ok=True)

def run_stats(in_jsonl: str, dsl_path: str, out_dir: str) -> dict:
    catalog = load_catalog(dsl_path)
    stats_defs = catalog.get("stats", [])
    defaults  = catalog.get("defaults", {})
    metric    = defaults.get("metric", {"type": "percent", "decimals": 2})
    

    out_index = os.path.join(out_dir, "index")
    ensure_dirs(out_dir, out_index)

    # (month -> group -> stat -> {'opp':int,'att':int})
    counts: Dict[str, Dict[str, Dict[str, Dict[str, int]]]] = {}
    id_files = {}  # (month, group, stat, kind) -> fh
    
    def _fh(m, g, s, k):
        key = (m, g, s, k)
        if key not in id_files:
            path = os.path.join(out_index, f"{m}__{g}__{s}__{k}.ids")
            id_files[key] = open(path, "w", encoding="utf-8")
        return id_files[key]

    hands_processed = 0
    errors = []
    try:
        with open(in_jsonl, "r", encoding="utf-8") as fi:
            for line_num, line in enumerate(fi, 1):
                try:
                    hand = json.loads(line)
                    hands_processed += 1
                    ctx = build_context(hand)
                    month = ctx["month"]
                    hand_groups = ctx["groups"]
                    hid = ctx["hand_id"]
                    

                    for stat in stats_defs:
                        stat_id = stat["id"]
                        s_groups = stat.get("applies_to_groups", [])
                        # restrição aos grupos aplicáveis
                        for g in (grp for grp in hand_groups if grp in s_groups):
                            if not pass_filters(stat, ctx):
                                continue
                                
                            # Handle opportunity conditions
                            opp = stat.get("opportunity")
                            
                            if isinstance(opp, dict) and "all" in opp:
                                # List of conditions from YAML
                                conditions = opp["all"]
                                opp_met = True
                                for cond in conditions:
                                    if not eval_condition(cond, ctx):
                                        opp_met = False
                                        break
                                if not opp_met:
                                    continue
                            elif not eval_clause(opp, ctx):
                                continue
                                
                            counts.setdefault(month, {}).setdefault(g, {}).setdefault(stat_id, {"opp": 0, "att": 0})
                            counts[month][g][stat_id]["opp"] += 1
                            _fh(month, g, stat_id, "opps").write(hid + "\n")

                            # Handle attempt conditions
                            att = stat.get("attempt")
                            if isinstance(att, dict) and "is_true" in att:
                                if bool(ctx.get(att["is_true"], False)):
                                    counts[month][g][stat_id]["att"] += 1
                                    _fh(month, g, stat_id, "attempts").write(hid + "\n")
                            elif eval_clause(att, ctx):
                                counts[month][g][stat_id]["att"] += 1
                                _fh(month, g, stat_id, "attempts").write(hid + "\n")

                except Exception as e:
                    errors.append({"line": line_num, "error": str(e)})
                    logger.error(f"[stats] Error at line {line_num}: {e}")

    finally:
        for f in id_files.values():
            try: 
                f.close()
            except: 
                pass

    # Manifest com percentagens
    dec = int(metric.get("decimals", 2))
    manifest = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "input": in_jsonl,
        "dsl": os.path.relpath(dsl_path),
        "metric": metric,
        "hands_processed": hands_processed,
        "errors": len(errors),
        "stats_computed": len(stats_defs),
        "counts": {}
    }
    for m, by_group in counts.items():
        mobj = manifest["counts"].setdefault(m, {})
        for g, by_stat in by_group.items():
            gobj = mobj.setdefault(g, {})
            for sid, agg in by_stat.items():
                opp = agg["opp"]
                att = agg["att"]
                pct = round((att/opp*100) if opp else 0.0, dec)
                gobj[sid] = {
                    "opportunities": opp,
                    "attempts": att,
                    "percentage": pct,
                    "index_files": {
                        "opps": f"index/{m}__{g}__{sid}__opps.ids",
                        "attempts": f"index/{m}__{g}__{sid}__attempts.ids"
                    }
                }

    out_path = os.path.join(out_dir, "stat_counts.json")
    with open(out_path, "w", encoding="utf-8") as fo:
        json.dump(manifest, fo, ensure_ascii=False, indent=2)

    # log de erros (se houver)
    if errors:
        with open(os.path.join(out_dir, "stats_errors.log"), "w", encoding="utf-8") as fe:
            json.dump(errors, fe, ensure_ascii=False, indent=2)

    return {
        "output_path": out_path,
        "index_dir": out_index,
        "hands_processed": hands_processed,
        "stats_computed": len(stats_defs),
        "errors": len(errors),
        "months_generated": len(counts),
        "stats": [s["id"] for s in stats_defs],
    }