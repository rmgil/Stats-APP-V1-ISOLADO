"""
Tests for weight validation and time decay in scoring system
"""

import pytest
from app.score.time_decay import weights_for_n, apply_time_decay


def test_weights_for_n():
    """Test correct weight generation for different month counts"""
    # Define weight configurations
    w3 = [0.5, 0.3, 0.2]
    w2 = [0.5, 0.5]
    w1 = [1.0]
    
    # 3 months: should use w3
    result_3 = weights_for_n(3, w3, w2, w1)
    assert result_3 == [0.5, 0.3, 0.2]
    assert abs(sum(result_3) - 1.0) < 0.001
    
    # 2 months: should use w2
    result_2 = weights_for_n(2, w3, w2, w1)
    assert result_2 == [0.5, 0.5]
    assert abs(sum(result_2) - 1.0) < 0.001
    
    # 1 month: should use w1
    result_1 = weights_for_n(1, w3, w2, w1)
    assert result_1 == [1.0]
    assert abs(sum(result_1) - 1.0) < 0.001
    
    # 4 months: should still use w3 (first 3 weights)
    result_4 = weights_for_n(4, w3, w2, w1)
    assert result_4 == [0.5, 0.3, 0.2]
    
    # 0 months: should use w1
    result_0 = weights_for_n(0, w3, w2, w1)
    assert result_0 == [1.0]


def test_apply_time_decay_ignores_zero_usable():
    """Test that values with usable=0 are ignored"""
    # Values: [(value, usable)]
    values = [
        (80, 1),  # 2025-01
        (40, 0),  # 2024-12 - should be ignored
        (60, 1),  # 2024-11
        (45, 1),  # 2024-10
    ]
    
    # Weights for 3 valid months
    weights = [0.5, 0.3, 0.2, 0.1]  # Extra weight will be ignored
    
    result = apply_time_decay(values, weights)
    
    # Should calculate weighted average of valid values only
    # Valid: (80, 1) with 0.5, (60, 1) with 0.2, (45, 1) with 0.1
    # Note: (40, 0) is skipped, so weights align as 0.5, skip, 0.2, 0.1
    # Wait, I need to re-understand the logic
    
    # Actually looking at the function, it filters usable first
    # So it becomes [(80, 0.5), (60, 0.2), (45, 0.1)]
    # Total weight = 0.5 + 0.2 + 0.1 = 0.8
    # Weighted sum = 80*0.5 + 60*0.2 + 45*0.1 = 40 + 12 + 4.5 = 56.5
    # Result = 56.5 / 0.8 = 70.625
    
    # Hmm, let me look at the actual function more carefully
    # It zips values with weights first, then filters
    # So (80,1) gets 0.5, (40,0) gets 0.3, (60,1) gets 0.2, (45,1) gets 0.1
    # After filtering: [(80, 0.5), (60, 0.2), (45, 0.1)]
    # Total weight = 0.8, weighted = 56.5, result = 70.625
    assert abs(result - 70.625) < 0.01


def test_apply_time_decay_all_zero_usable():
    """Test handling when all values have usable=0"""
    values = [
        (80, 0),
        (40, 0),
    ]
    weights = [0.5, 0.5]
    
    result = apply_time_decay(values, weights)
    
    assert result == 0.0


def test_apply_time_decay_single_value():
    """Test time decay with only one value"""
    values = [(75, 1)]
    weights = [1.0]
    
    result = apply_time_decay(values, weights)
    
    assert result == 75.0


def test_apply_time_decay_two_values():
    """Test time decay with two values (50/50 split)"""
    values = [
        (80, 1),
        (60, 1)
    ]
    weights = [0.5, 0.5]
    
    result = apply_time_decay(values, weights)
    
    # (80*0.5 + 60*0.5) / 1.0 = 70
    assert result == 70.0


def test_weight_normalization():
    """Test weight normalization from loader module"""
    from app.score.loader import _sum_and_fix
    
    # Test auto mode - should normalize
    groups = {
        "group1": 0.3,
        "group2": 0.2,
        "group3": 0.1
    }
    
    normalized = _sum_and_fix(groups, "test", "auto")
    
    # Should be normalized to sum to 1.0
    total = sum(normalized.values())
    assert abs(total - 1.0) < 0.001
    
    # Check proportions are maintained
    assert normalized["group1"] == pytest.approx(0.5, rel=1e-2)  # 0.3/0.6
    assert normalized["group2"] == pytest.approx(0.333, rel=1e-2)  # 0.2/0.6
    assert normalized["group3"] == pytest.approx(0.167, rel=1e-2)  # 0.1/0.6


def test_weight_strict_validation():
    """Test strict weight validation from loader module"""
    from app.score.loader import _sum_and_fix
    
    # Test strict mode - should raise error if not 1.0
    groups = {
        "group1": 0.3,
        "group2": 0.2,
        "group3": 0.1
    }
    
    with pytest.raises(ValueError, match="sum=0.6"):
        _sum_and_fix(groups, "test", "strict")
    
    # Valid weights should pass
    valid_groups = {
        "group1": 0.5,
        "group2": 0.3,
        "group3": 0.2
    }
    
    validated = _sum_and_fix(valid_groups, "test", "strict")
    assert validated == valid_groups


def test_weight_off_validation():
    """Test off mode for weight validation"""
    from app.score.loader import _sum_and_fix
    
    # Test off mode - no validation
    groups = {
        "group1": 0.3,
        "group2": 0.2,
        "group3": 0.1
    }
    
    result = _sum_and_fix(groups, "test", "off")
    assert result == groups