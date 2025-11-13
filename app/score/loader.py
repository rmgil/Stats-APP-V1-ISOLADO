"""
Configuration loader for scoring system with validation and caching
"""
import os
import yaml
import json
import hashlib
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("score.loader")

DEFAULT_CFG = "app/score/config.yml"

def _sum_and_fix(weights: dict, label: str, mode: str) -> dict:
    """
    Validate and optionally normalize weights to sum to 1.0
    
    Args:
        weights: Dictionary of weights
        label: Label for logging
        mode: Validation mode ('auto', 'strict', 'off')
    
    Returns:
        Original or normalized weights
    """
    if not weights: 
        return weights
    
    s = sum(float(v) for v in weights.values())
    if s == 0: 
        return weights
    if abs(s - 1.0) < 1e-9: 
        return weights
    
    if mode == "off":
        logger.info(f"[weights] {label} sum={s:.4f} (accepted)")
        return weights
    
    if mode == "strict":
        raise ValueError(f"[weights] {label} sum={s:.4f} (strict mode)")
    
    # auto: normalize
    fixed = {k: float(v)/s for k, v in weights.items()}
    logger.warning(f"[weights] {label} sum={s:.4f} → auto-normalized to 1.0")
    return fixed

def load_config(path: str = DEFAULT_CFG) -> dict:
    """
    Load and validate configuration from YAML file
    
    Args:
        path: Path to configuration file
        
    Returns:
        Validated configuration dictionary
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    
    # Get validation mode
    mode = (cfg.get("weights", {}).get("validate") or "auto").lower()
    
    # Normalize/validate weights
    cfg["weights"]["groups"] = _sum_and_fix(
        cfg["weights"].get("groups", {}), 
        "groups", 
        mode
    )
    cfg["weights"]["stats"] = _sum_and_fix(
        cfg["weights"].get("stats", {}), 
        "stats", 
        mode
    )
    cfg["weights"]["subgroups"] = _sum_and_fix(
        cfg["weights"].get("subgroups", {}), 
        "subgroups", 
        mode
    )
    
    logger.info(f"Loaded config from {path}")
    return cfg

def save_config(cfg: dict, path: str = DEFAULT_CFG) -> None:
    """
    Save configuration to YAML file
    
    Args:
        cfg: Configuration dictionary
        path: Path to save file
    """
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=True)
    logger.info(f"Saved config to {path}")

def config_hash(cfg: dict) -> str:
    """
    Generate SHA1 hash of configuration for caching
    
    Args:
        cfg: Configuration dictionary
        
    Returns:
        SHA1 hash string
    """
    blob = yaml.safe_dump(cfg, sort_keys=True, allow_unicode=True)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()

def get_stat_families(config: Dict[str, Any]) -> Dict[str, list]:
    """
    Extract stat families and their component stats from config
    
    Returns:
        Dict mapping family name to list of stat IDs
    """
    # In new config, stats are listed directly in weights.stats
    stat_keys = list(config['weights'].get('stats', {}).keys())
    
    # Group by family prefix (RFI, etc.)
    families = {}
    for stat in stat_keys:
        family = stat.split('_')[0]  # Get family from stat name
        if family not in families:
            families[family] = []
        families[family].append(stat)
    
    return families

def load_cache(config: Dict[str, Any]) -> Optional[Dict]:
    """
    Load cached scores if enabled and available
    
    Args:
        config: Configuration dict
        
    Returns:
        Cached data or None
    """
    cache_config = config.get('cache', {})
    if not cache_config.get('enabled', False):
        return None
    
    cache_path = cache_config.get('path', 'scores/.cache.json')
    
    try:
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)
                
                # Check if cache is valid for current config
                if 'config_hash' in cache_data:
                    current_hash = config_hash(config)
                    if cache_data['config_hash'] == current_hash:
                        logger.info(f"Loaded valid cache from {cache_path}")
                        return cache_data
                    else:
                        logger.info("Cache invalidated due to config change")
                
    except Exception as e:
        logger.warning(f"Failed to load cache: {e}")
    
    return None

def save_cache(data: Dict, config: Dict[str, Any]) -> bool:
    """
    Save data to cache if enabled
    
    Args:
        data: Data to cache
        config: Configuration dict
        
    Returns:
        True if saved successfully
    """
    cache_config = config.get('cache', {})
    if not cache_config.get('enabled', False):
        return False
    
    cache_path = cache_config.get('path', 'scores/.cache.json')
    
    try:
        # Create directory if needed
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        
        # Add config hash to cache data
        data['config_hash'] = config_hash(config)
        
        with open(cache_path, 'w') as f:
            json.dump(data, f, indent=2)
            logger.info(f"Saved cache to {cache_path}")
            return True
    except Exception as e:
        logger.warning(f"Failed to save cache: {e}")
        return False

def get_ideal_value(stat_id: str, group: str, config: Dict[str, Any]) -> Optional[float]:
    """
    Get the ideal percentage value for a stat and group
    
    Args:
        stat_id: Stat identifier (e.g., 'RFI_EARLY')
        group: Group name (e.g., 'nonko_9max_pref')
        config: Configuration dict
        
    Returns:
        Ideal percentage or None if not defined
    """
    ideals = config.get('ideals', {})
    stat_ideals = ideals.get(stat_id, {})
    return stat_ideals.get(group)