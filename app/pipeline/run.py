# app/pipeline/run.py
from pathlib import Path
import json
import subprocess
import os
import logging

BASE = Path("/tmp/uploads")
logger = logging.getLogger(__name__)

def run(cmd: list[str]) -> None:
    ok = subprocess.run(cmd, check=False)
    if ok.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")

def run_full_pipeline(token: str) -> dict:
    root = BASE / token
    if not root.exists():
        raise FileNotFoundError("invalid token")

    # Cria diretório de saída
    out_dir = root / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    parsed_dir = out_dir / "parsed"
    parsed_dir.mkdir(exist_ok=True)
    
    # 1) Parse - converter arquivos classificados para JSONL
    # Os arquivos estão em raw/ após classificação
    raw_dir = root / "raw"
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw text dir not found: {raw_dir}")
    
    # Parser básico para extrair hands
    hands_jsonl = parsed_dir / "hands.jsonl"
    run(["python", "-m", "app.parse.runner", 
         "--in", str(root),  # Root contém os arquivos classificados
         "--out", str(hands_jsonl),
         "--aliases", "app/config/hero_aliases.json"])
    
    # 2) Derive - enriquecer com análise postflop
    enriched_jsonl = parsed_dir / "hands_enriched.jsonl"
    run(["python", "-m", "app.derive.runner", 
         "--in", str(hands_jsonl), 
         "--out", str(enriched_jsonl)])

    # 3) Partitions
    run(["python", "-m", "app.partition.runner", 
         "--in", str(enriched_jsonl), 
         "--out", str(out_dir)])

    # 4) Stats
    run(["python", "-m", "app.stats.runner", 
         "--in", str(enriched_jsonl), 
         "--out", str(out_dir)])

    # 5) Score
    # Check for stat_counts.json in both possible locations
    stats_path = out_dir / "stats" / "stat_counts.json"
    if not stats_path.exists():
        stats_path = out_dir / "stat_counts.json"
    
    if stats_path.exists():
        run(["python", "-m", "app.score.runner_cli", 
             str(stats_path), 
             "-o", str(out_dir / "scores"), 
             "--force"])

    # retornos p/ UI
    return {
        "token": token,
        "paths": {
            "enriched": str(root / "out" / "parsed" / "hands_enriched.jsonl"),
            "stat_counts": str(root / "out" / "stats" / "stat_counts.json"),
            "scorecard": str(root / "out" / "scores" / "scorecard.json"),
        }
    }

def run_all_for_job(inbox_dir: str, out_root: str):
    """
    Executes the complete pipeline for a job directory.
    
    Args:
        inbox_dir: Directory with unpacked files from upload.zip
        out_root: Root directory for all output (job_dir)
    """
    from main import process_txt_tree
    
    # Ensure directories exist
    Path(out_root).mkdir(parents=True, exist_ok=True)
    
    # 1) Classification - NON-KO, PKO, Mystery
    logger.info(f"Starting classification for job at {inbox_dir}")
    classified_dir = os.path.join(out_root, "test_classified")
    os.makedirs(classified_dir, exist_ok=True)
    
    # Run the classification
    manifest = process_txt_tree(
        Path(inbox_dir),
        Path(classified_dir)
    )
    
    # Save classification manifest
    manifest_path = os.path.join(out_root, "manifest.json")
    manifest["job_dir"] = out_root
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    
    # 2) Parse - extract hands from classified files
    logger.info("Starting parse phase...")
    parsed_dir = os.path.join(out_root, "parsed")
    os.makedirs(parsed_dir, exist_ok=True)
    hands_jsonl = os.path.join(parsed_dir, "hands.jsonl")
    
    parse_cmd = [
        "python", "-m", "app.parse.runner",
        "--in", classified_dir,
        "--out", hands_jsonl,
        "--aliases", "app/config/hero_aliases.json"
    ]
    subprocess.run(parse_cmd, check=True, capture_output=True, text=True)
    
    # 3) Derive - enrich with postflop analysis
    logger.info("Starting derive phase...")
    enriched_jsonl = os.path.join(parsed_dir, "hands_enriched.jsonl")
    
    derive_cmd = [
        "python", "-m", "app.derive.runner",
        "--in", hands_jsonl,
        "--out", enriched_jsonl
    ]
    subprocess.run(derive_cmd, check=True, capture_output=True, text=True)
    
    # 4) Partitions - create analysis groups
    logger.info("Starting partition phase...")
    from app.partition.runner import build_partitions
    
    part_result = build_partitions(enriched_jsonl, out_root)
    
    # 5) Stats - calculate statistics
    logger.info("Starting stats phase...")
    from app.stats.engine import run_stats
    
    stats_result = run_stats(
        enriched_jsonl,
        "app/stats/dsl/stats.yml",
        out_root
    )
    
    # 6) Score - generate scorecard
    logger.info("Starting score phase...")
    scores_dir = os.path.join(out_root, "scores")
    os.makedirs(scores_dir, exist_ok=True)
    
    # Check where stat_counts.json is located
    stats_path = os.path.join(out_root, "stats", "stat_counts.json")
    if not os.path.exists(stats_path):
        stats_path = os.path.join(out_root, "stat_counts.json")
    
    if os.path.exists(stats_path):
        score_cmd = [
            "python", "-m", "app.score.runner_cli",
            stats_path,
            "-o", scores_dir,
            "--force"
        ]
        subprocess.run(score_cmd, check=True, capture_output=True, text=True)
    
    logger.info(f"Pipeline completed for job at {out_root}")
    return {
        "manifest": manifest_path,
        "enriched": enriched_jsonl,
        "stats": stats_path,
        "scorecard": os.path.join(scores_dir, "scorecard.json")
    }