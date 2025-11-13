import pytest
import yaml
from pathlib import Path
from app.score.loader import load_config


def test_postflop_weights_in_config():
    """Test that postflop weights are properly configured"""
    config = load_config()
    
    # Check group weights
    assert "postflop_all" in config["weights"]["groups"]
    assert config["weights"]["groups"]["postflop_all"] > 0
    
    # Check subgroup weights for postflop families
    subgroups = config["weights"]["subgroups"]
    postflop_families = [
        "POSTFLOP_CBET",
        "POSTFLOP_VS_CBET", 
        "POSTFLOP_MISSED_CBET",
        "POSTFLOP_TURN",
        "POSTFLOP_RIVER"
    ]
    
    for family in postflop_families:
        assert family in subgroups, f"Missing weight for {family}"
        assert subgroups[family] > 0, f"Weight for {family} must be positive"
    
    # Verify postflop families exist and have reasonable weights
    # Note: They get auto-normalized with preflop families, so we check relative proportions
    postflop_sum = sum(subgroups[f] for f in postflop_families)
    assert postflop_sum > 0.3, f"Postflop subgroup weights sum to {postflop_sum}, should be significant portion of total"


def test_postflop_stat_weights():
    """Test individual postflop stat weights"""
    config = load_config()
    stat_weights = config["weights"]["stats"]
    
    # Check key postflop stats have weights
    postflop_stats = [
        "POST_CBET_FLOP_IP",
        "POST_CBET_FLOP_OOP",
        "POST_VS_CBET_FLOP_FOLD_IP",
        "POST_VS_CBET_FLOP_RAISE_IP",
        "POST_CBET_TURN_IP",
        "POST_WTSD",
        "POST_W$SD",
        "POST_RIVER_AGG_PCT"
    ]
    
    for stat in postflop_stats:
        assert stat in stat_weights, f"Missing weight for {stat}"
        assert stat_weights[stat] > 0, f"Weight for {stat} must be positive"


def test_postflop_ideals():
    """Test that postflop stats have ideals configured"""
    config = load_config()
    ideals = config["ideals"]
    
    # Check key postflop stats have ideals for postflop_all group
    postflop_stats_with_ideals = [
        "POST_CBET_FLOP_IP",
        "POST_CBET_FLOP_OOP",
        "POST_VS_CBET_FLOP_FOLD_IP",
        "POST_CBET_TURN_IP",
        "POST_WTSD",
        "POST_W$SD",
        "POST_RIVER_AGG_PCT"
    ]
    
    for stat in postflop_stats_with_ideals:
        assert stat in ideals, f"Missing ideal for {stat}"
        assert "postflop_all" in ideals[stat], f"{stat} missing ideal for postflop_all group"
        
        ideal_value = ideals[stat]["postflop_all"]
        assert 0 <= ideal_value <= 100, f"{stat} ideal must be between 0-100, got {ideal_value}"


def test_postflop_cbet_ideals_range():
    """Test that CBet ideals are within reasonable ranges"""
    config = load_config()
    ideals = config["ideals"]
    
    # Flop CBet IP should be higher than OOP
    cbet_ip = ideals["POST_CBET_FLOP_IP"]["postflop_all"]
    cbet_oop = ideals["POST_CBET_FLOP_OOP"]["postflop_all"]
    
    assert cbet_ip > cbet_oop, "CBet IP ideal should be higher than OOP"
    assert 50 <= cbet_ip <= 80, f"CBet IP ideal should be 50-80%, got {cbet_ip}"
    assert 30 <= cbet_oop <= 60, f"CBet OOP ideal should be 30-60%, got {cbet_oop}"


def test_postflop_showdown_ideals_range():
    """Test that showdown ideals are within reasonable ranges"""
    config = load_config()
    ideals = config["ideals"]
    
    # WTSD should be reasonable
    wtsd = ideals["POST_WTSD"]["postflop_all"]
    assert 25 <= wtsd <= 45, f"WTSD ideal should be 25-45%, got {wtsd}"
    
    # W$SD should be above 50%
    wsd = ideals["POST_W$SD"]["postflop_all"]
    assert 50 <= wsd <= 65, f"W$SD ideal should be 50-65%, got {wsd}"
    
    # W$WSF should be reasonable
    wwsf = ideals["POST_W$WSF"]["postflop_all"]
    assert 35 <= wwsf <= 55, f"W$WSF ideal should be 35-55%, got {wwsf}"


def test_config_validation_passes():
    """Test that the current configuration loads without errors"""
    # Should not raise any exceptions
    try:
        config = load_config()
        assert config is not None
        assert "weights" in config
        assert "ideals" in config
    except Exception as e:
        pytest.fail(f"Config loading failed: {e}")


def test_weight_normalization():
    """Test that weights are properly normalized when needed"""
    config = load_config()
    
    # Group weights should sum to 1.0 (or be auto-normalized)
    group_weights = config["weights"]["groups"]
    total = sum(group_weights.values())
    
    # Allow for small floating point differences
    assert abs(total - 1.0) < 0.01, f"Group weights sum to {total}, expected 1.0"
    
    # Each family's stat weights should sum to 1.0
    stat_weights = config["weights"]["stats"]
    
    # Check CBet family
    cbet_stats = ["POST_CBET_FLOP_IP", "POST_CBET_FLOP_3BETPOT_IP", "POST_CBET_FLOP_OOP"]
    cbet_sum = sum(stat_weights.get(s, 0) for s in cbet_stats)
    assert abs(cbet_sum - 1.0) < 0.01, f"CBet stat weights sum to {cbet_sum}, expected 1.0"


def test_step_configuration():
    """Test step-based scoring configuration for postflop"""
    config = load_config()
    scoring = config["scoring"]["default"]
    
    # Verify step configuration exists
    assert "step_down_pct" in scoring
    assert "step_up_pct" in scoring
    assert "points_per_step_down" in scoring
    assert "points_per_step_up" in scoring
    
    # Verify reasonable values
    assert 1 <= scoring["step_down_pct"] <= 5
    assert 2 <= scoring["step_up_pct"] <= 10
    assert 5 <= scoring["points_per_step_down"] <= 20
    assert 5 <= scoring["points_per_step_up"] <= 20