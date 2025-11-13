"""
Time decay calculations for scoring
"""
from typing import List, Tuple

def weights_for_n(n: int, w3: List[float], w2: List[float], w1: List[float]) -> List[float]:
    """
    Return appropriate weights based on number of months available
    
    Args:
        n: Number of months
        w3: Weights for 3 months [0.5, 0.3, 0.2]
        w2: Weights for 2 months [0.5, 0.5]
        w1: Weights for 1 month [1.0]
    
    Returns:
        List of weights adjusted to n
    """
    if n >= 3: 
        return w3[:3]
    if n == 2: 
        return w2[:2]
    return w1[:1]

def apply_time_decay(values: List[Tuple[float, float]], weights: List[float]) -> float:
    """
    Apply time decay to values with weights
    
    Args:
        values: [(value, usable_flag)], most recent first
        weights: e.g. [0.5, 0.3, 0.2] adjusted to length of values
    
    Returns:
        Weighted average of usable values
    """
    usable = [(v, w) for (v, u), w in zip(values, weights) if u > 0]
    if not usable: 
        return 0.0
    
    total_w = sum(w for _, w in usable)
    return sum(v * w for v, w in usable) / total_w