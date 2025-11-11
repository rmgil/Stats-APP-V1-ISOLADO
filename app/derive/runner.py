"""
Runner for hand enrichment with derived data (Phase 3)
"""
import json
import os
import logging
from typing import Dict, List, Optional
from app.parse.schemas import Hand
from app.derive.schemas import (
    Derived, DerivedPositions, DerivedPreflop, 
    DerivedIP, DerivedStacks, DerivedFlags
)
from app.derive.positions import assign_positions, group_buckets
from app.derive.preflop import (
    is_unopened_pot, has_limper_before, first_raiser,
    actor_is_first_raiser, who_3bet_4bet, hero_faced_3bet,
    hero_folded_to_3bet, detect_squeeze, detect_resteal_vs_btn,
    classify_pot_type, detect_freeplay_bb, hero_vpip, 
    compute_pot_size_flop_bb, limper_exists
)
from app.derive.ip import derive_ip
from app.derive.stacks import eff_stack_bb_srp, eff_stack_bb_vs_3bettor
from app.derive.postflop import derive_postflop

logger = logging.getLogger(__name__)


def enrich_hands(in_jsonl: str, out_jsonl: str, force: bool = False) -> dict:
    """
    Enrich parsed hands with derived data and generate statistics.
    
    Args:
        in_jsonl: Path to input JSONL file with parsed hands
        out_jsonl: Path to output JSONL file with enriched hands
        force: If True, always reprocess and overwrite output file even if it exists
        
    Returns:
        Dict with processing summary and stats_path
    """
    # Se force=False e o arquivo de saída já existe, pular processamento
    if not force and os.path.exists(out_jsonl):
        logger.info(f"Output file {out_jsonl} already exists and force=False, skipping enrichment")
        stats_path = os.path.join(os.path.dirname(out_jsonl), "derive_stats.json")
        # Contar hands no arquivo existente
        with open(out_jsonl, "r", encoding="utf-8") as f:
            hand_count = sum(1 for _ in f)
        return {
            "input": in_jsonl,
            "output": out_jsonl,
            "hands": hand_count,
            "stats_path": stats_path,
            "skipped": True,
            "message": "Using existing enriched file (force=False)"
        }
    
    out = []
    stats = {
        "hands_processed": 0,
        "position_distribution": {"EP": 0, "MP": 0, "LP": 0},
        "pot_type_distribution": {"SRP": 0, "3bet": 0, "4bet": 0, "none": 0},
        "heads_up_percentage": 0.0,
        "average_eff_stack_srp": 0.0,
        "average_eff_stack_vs_3bet": 0.0,
        "errors": []
    }
    eff_srp_acc = []
    eff_3b_acc = []
    hu_count = 0
    
    # Process hands with memory optimization - flush every 1000 hands
    batch_size = 1000
    current_batch = []
    
    try:
        with open(in_jsonl, "r", encoding="utf-8") as fi:
            for line_num, line in enumerate(fi, 1):
                try:
                    obj = json.loads(line.strip())
                    hand = Hand(**obj)
                    hero = hand.hero or ""
                    
                    # POSITIONS
                    abs_pos = assign_positions(hand)
                    n_active = len(hand.players)
                    table_max = hand.table_max or n_active
                    pos_group_raw = group_buckets(abs_pos, n_active)  # Pass active players count
                    # Filter out BLINDS from pos_group - schema only accepts EP/MP/LP
                    pos_group = {k: v for k, v in pos_group_raw.items() if v in ("EP", "MP", "LP")}
                    positions = DerivedPositions(
                        table_max_resolved=table_max,
                        abs_positions=abs_pos,
                        pos_group=pos_group,
                        button_seat=hand.button_seat or 0
                    )
                    
                    # PREFLOP
                    preflop = hand.streets.get("preflop")
                    acts = preflop.actions if preflop else []
                    unopened = is_unopened_pot(acts, until_actor=hero) if hero else is_unopened_pot(acts)
                    limper_before = has_limper_before(acts, hero) if hero else False
                    opener = first_raiser(acts)
                    three, four = who_3bet_4bet(acts)
                    hero_rfi = unopened and actor_is_first_raiser(acts, hero) if hero else False
                    is_iso = (not unopened) and actor_is_first_raiser(acts, hero) and limper_exists(acts) if hero else False
                    faced_3b = hero_faced_3bet(acts, hero, opener) if hero else False
                    folded_3b = hero_folded_to_3bet(acts, hero) if hero else False
                    is_sqz = detect_squeeze(acts, hero) if hero else False
                    is_rst_btn = detect_resteal_vs_btn(acts, hero, hand, opener) if hero else False
                    pot_type = classify_pot_type(acts)
                    freeplay = detect_freeplay_bb(acts, hand)
                    
                    pf = DerivedPreflop(
                        unopened_pot=unopened,
                        has_limper_before_hero=limper_before,
                        open_raiser=opener,
                        hero_raised_first_in=hero_rfi,
                        is_isoraiser=is_iso,
                        three_bettor=three,
                        four_bettor=four,
                        faced_3bet=faced_3b,
                        folded_to_3bet=folded_3b,
                        is_squeeze=is_sqz,
                        is_resteal_vs_btn=is_rst_btn,
                        pot_type=pot_type,
                        freeplay_bb=freeplay,
                        hero_vpip=hero_vpip(acts, hero) if hero else False,
                        hero_position=abs_pos.get(hero) if hero else None,
                        pot_size_bb=compute_pot_size_flop_bb(hand)
                    )
                    
                    # IP / MW por street
                    ipd = derive_ip(hand)
                    ip = DerivedIP(**ipd)
                    
                    # STACKS
                    s_srp = eff_stack_bb_srp(hand, hero) if hero else None
                    s_3b = eff_stack_bb_vs_3bettor(hand, hero, three) if hero and three else None
                    stacks = DerivedStacks(
                        eff_stack_bb_srp=s_srp, 
                        eff_stack_bb_vs_3bettor=s_3b
                    )
                    
                    # FLAGS
                    flags = DerivedFlags(
                        any_allin_preflop=hand.any_allin_preflop if hasattr(hand, 'any_allin_preflop') else False
                    )
                    
                    # POSTFLOP - precisa dos dados derived já presentes
                    # Cria um objeto temporário com os dados derived para o postflop
                    temp_obj = dict(obj)
                    temp_obj["derived"] = {
                        "positions": positions.model_dump(),
                        "preflop": pf.model_dump(),
                        "ip": ip.model_dump(),
                        "stacks": stacks.model_dump(),
                        "flags": flags.model_dump()
                    }
                    postflop_data = derive_postflop(temp_obj)
                    from app.derive.schemas import DerivedPostflop
                    postflop = DerivedPostflop(**postflop_data)
                    
                    # Build complete derived structure
                    derived = Derived(
                        positions=positions, 
                        preflop=pf, 
                        ip=ip, 
                        stacks=stacks, 
                        flags=flags,
                        postflop=postflop
                    )
                    
                    # Add derived to original object
                    obj["derived"] = derived.model_dump()
                    current_batch.append(obj)
                    
                    # Flush batch when it reaches size limit
                    if len(current_batch) >= batch_size:
                        _write_batch_to_file(current_batch, out_jsonl, line_num <= batch_size)
                        out.extend(current_batch)
                        current_batch = []
                        
                        if line_num % 5000 == 0:
                            logger.info(f"Processed {line_num} hands...")
                    
                    # Telemetria
                    hg = positions.pos_group.get(hero) if hero else None
                    if hg in ("EP", "MP", "LP"): 
                        stats["position_distribution"][hg] += 1
                    stats["pot_type_distribution"][pot_type] += 1
                    if ip.heads_up_flop: 
                        hu_count += 1
                    if s_srp: 
                        eff_srp_acc.append(s_srp)
                    if s_3b: 
                        eff_3b_acc.append(s_3b)
                    
                except Exception as e:
                    stats["errors"].append({
                        "line": line_num,
                        "error": str(e)
                    })
                    logger.error(f"Error processing line {line_num}: {e}")
                    
    except FileNotFoundError:
        logger.error(f"Input file not found: {in_jsonl}")
        return {"error": f"File not found: {in_jsonl}"}
    
    # Write remaining batch
    if current_batch:
        _write_batch_to_file(current_batch, out_jsonl, len(out) == 0)
        out.extend(current_batch)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(out_jsonl), exist_ok=True)
    
    # Calculate final statistics
    stats["hands_processed"] = len(out)
    stats["heads_up_percentage"] = round((hu_count / max(1, len(out))) * 100, 2)
    if eff_srp_acc: 
        stats["average_eff_stack_srp"] = round(sum(eff_srp_acc) / len(eff_srp_acc), 2)
    if eff_3b_acc: 
        stats["average_eff_stack_vs_3bet"] = round(sum(eff_3b_acc) / len(eff_3b_acc), 2)
    
    # Save statistics
    stats_path = os.path.join(os.path.dirname(out_jsonl), "derive_stats.json")
    with open(stats_path, "w", encoding="utf-8") as sf:
        json.dump(stats, sf, ensure_ascii=False, indent=2)
    
    logger.info(f"Enriched {stats['hands_processed']} hands, saved to {out_jsonl}")
    logger.info(f"Statistics saved to {stats_path}")
    
    return {
        "input": in_jsonl, 
        "output": out_jsonl, 
        "hands": len(out), 
        "stats_path": stats_path
    }


def _write_batch_to_file(batch: list, out_jsonl: str, is_first_batch: bool = False):
    """Write a batch of objects to JSONL file"""
    mode = "w" if is_first_batch else "a"
    os.makedirs(os.path.dirname(out_jsonl), exist_ok=True)
    
    with open(out_jsonl, mode, encoding="utf-8") as fo:
        for obj in batch:
            fo.write(json.dumps(obj, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Enrich parsed hands with derived data")
    parser.add_argument("--in", dest="input", required=True, help="Input JSONL file")
    parser.add_argument("--out", dest="output", required=True, help="Output JSONL file")
    
    args = parser.parse_args()
    
    result = enrich_hands(args.input, args.output)
    print(json.dumps(result, indent=2))