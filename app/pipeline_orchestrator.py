# app/pipeline.py
import json
import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def build_all(manifest_path: str, out_dir: str):
    """
    Lê o manifest e executa pipeline completo:
    parse -> derive -> partitions -> stats -> score
    
    Args:
        manifest_path: Caminho para manifest.json com info dos arquivos
        out_dir: Diretório de saída para todos os artefatos
        
    Returns:
        Dict com paths gerados e summary para o dashboard
    """
    import subprocess
    import shutil
    from pathlib import Path
    import hashlib
    
    # Garante que out_dir existe
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    parsed_dir = Path(out_dir) / "parsed"
    parsed_dir.mkdir(exist_ok=True)
    
    # Lê manifest
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    
    # Cache inteligente - verifica se já processamos estes ficheiros
    cache_key = _generate_cache_key(manifest)
    cache_dir = Path(out_dir) / ".cache"
    cache_file = cache_dir / f"{cache_key}.json"
    
    if cache_file.exists():
        logger.info("Cache hit - using previously processed results")
        with open(cache_file, "r") as f:
            cached_result = json.load(f)
        
        # Verifica se os artefatos ainda existem
        if _validate_cached_artifacts(cached_result, out_dir):
            return cached_result
    
    # Descobre diretório base dos arquivos
    job_dir = manifest.get("job_dir", os.path.dirname(manifest_path))
    
    # 1) Parse - processa os arquivos classificados
    logger.info("Starting parse phase...")
    hands_jsonl = parsed_dir / "hands.jsonl"
    
    # Executa parser
    parse_cmd = [
        "python", "-m", "app.parse.runner",
        "--in", job_dir,
        "--out", str(hands_jsonl),
        "--aliases", "app/config/hero_aliases.json"
    ]
    subprocess.run(parse_cmd, check=True)
    
    # 2) Derive - enriquece com análise postflop
    logger.info("Starting derive phase...")
    enriched_jsonl = parsed_dir / "hands_enriched.jsonl"
    
    derive_cmd = [
        "python", "-m", "app.derive.runner",
        "--in", str(hands_jsonl),
        "--out", str(enriched_jsonl)
    ]
    subprocess.run(derive_cmd, check=True)
    
    # 3) Partitions - cria grupos para análise
    logger.info("Starting partition phase...")
    from app.partition.runner import build_partitions
    
    part_result = build_partitions(str(enriched_jsonl), str(out_dir))
    
    # 4) Stats - calcula estatísticas
    logger.info("Starting stats phase...")
    from app.stats.engine import run_stats
    
    stats_result = run_stats(
        str(enriched_jsonl),
        "app/stats/dsl/stats.yml", 
        str(out_dir)
    )
    
    # 5) Score - gera scorecard
    logger.info("Starting score phase...")
    scores_dir = Path(out_dir) / "scores"
    scores_dir.mkdir(exist_ok=True)
    
    # Verifica onde está stat_counts.json
    stat_counts_path = Path(out_dir) / "stats" / "stat_counts.json"
    if not stat_counts_path.exists():
        stat_counts_path = Path(out_dir) / "stat_counts.json"
    
    if stat_counts_path.exists():
        from app.score.runner import build_scorecard
        
        score_result = build_scorecard(
            str(stat_counts_path),
            "app/score/config.yml",
            str(scores_dir),
            force=True
        )
    else:
        logger.warning("stat_counts.json not found, skipping score phase")
        score_result = {}
    
    # Conta hands processadas
    hands_processed = 0
    if enriched_jsonl.exists():
        with open(enriched_jsonl, "r") as f:
            hands_processed = sum(1 for _ in f)
    
    # Extrai grupos do partition counts
    groups = []
    partition_counts_file = Path(out_dir) / "partition_counts.json"
    if partition_counts_file.exists():
        with open(partition_counts_file, "r") as f:
            partition_data = json.load(f)
            groups = list(partition_data.get("groups", {}).keys())
    
    # Summary para o dashboard
    summary = {
        "hands": hands_processed,
        "groups": groups,
        "score_overall": score_result.get("overall", "N/A"),
        "classification": manifest.get("inputs", {}),
        "stats_computed": stats_result.get("stats_computed", 0)
    }
    
    # Paths dos artefatos gerados
    paths = {
        "enriched": str(enriched_jsonl),
        "stat_counts": str(stat_counts_path) if stat_counts_path.exists() else None,
        "scorecard": str(Path(out_dir) / "scores" / "scorecard.json"),
        "partition_counts": str(partition_counts_file)
    }
    
    logger.info(f"Pipeline complete! {hands_processed} hands processed")
    
    # Copia artefatos para runs/{token} para acesso posterior
    # Extrai token do out_dir (formato esperado: /tmp/jobs/{token}/out)
    out_path = Path(out_dir)
    if out_path.parts[-1] == "out" and len(out_path.parts) >= 2:
        token = out_path.parts[-2]
        
        # Cria diretórios de destino
        run_dir = Path('runs') / token
        (run_dir / 'stats').mkdir(parents=True, exist_ok=True)
        (run_dir / 'scores').mkdir(parents=True, exist_ok=True)
        
        # Copia stat_counts.json se existe
        if stat_counts_path.exists():
            dest_stats = run_dir / 'stats' / 'stat_counts.json'
            shutil.copy2(str(stat_counts_path), str(dest_stats))
            logger.info(f"Copied stats to: {dest_stats.absolute()}")
        
        # Copia scorecard.json se existe
        scorecard_path = Path(out_dir) / "scores" / "scorecard.json"
        if scorecard_path.exists():
            dest_score = run_dir / 'scores' / 'scorecard.json'
            shutil.copy2(str(scorecard_path), str(dest_score))
            logger.info(f"Copied scorecard to: {dest_score.absolute()}")
        
        logger.info(f"Artifacts saved to runs/{token} for future access")
    
    result = {
        "paths": paths,
        "summary": summary
    }
    
    # Salva no cache para futuras execuções
    cache_dir.mkdir(exist_ok=True)
    with open(cache_file, "w") as f:
        json.dump(result, f, indent=2)
    
    return result


def _generate_cache_key(manifest: dict) -> str:
    """Gera chave de cache baseada nos inputs do manifest"""
    # Combina paths e timestamps dos ficheiros para detectar mudanças
    cache_data = {
        "inputs": manifest.get("inputs", {}),
        "file_count": sum(manifest.get("inputs", {}).values()),
        "job_dir": manifest.get("job_dir", "")
    }
    
    cache_str = json.dumps(cache_data, sort_keys=True)
    return hashlib.md5(cache_str.encode()).hexdigest()[:12]


def _validate_cached_artifacts(cached_result: dict, out_dir: str) -> bool:
    """Valida se os artefatos em cache ainda existem e são válidos"""
    paths = cached_result.get("paths", {})
    
    required_files = [
        "enriched", "stat_counts", "scorecard"
    ]
    
    for file_key in required_files:
        file_path = paths.get(file_key)
        if not file_path or not Path(file_path).exists():
            logger.info(f"Cache invalid - missing {file_key}")
            return False
    
    logger.info("Cache validation successful")
    return True


def build_all_safe(manifest_path: str, out_dir: str):
    """
    Versão do build_all com tratamento de erros robusto.
    """
    try:
        return build_all(manifest_path, out_dir)
    except Exception as e:
        logger.exception("Pipeline failed for manifest=%s", manifest_path)
        import traceback
        error_log = Path(out_dir) / "pipeline_error.log"
        with open(error_log, "w") as f:
            f.write(traceback.format_exc())
        
        return {
            "paths": {},
            "summary": {
                "error": str(e),
                "error_log": str(error_log)
            }
        }