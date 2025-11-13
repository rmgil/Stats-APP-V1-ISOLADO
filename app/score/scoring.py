"""
Scoring functions with step and linear modes
"""
import math

def clamp(x, lo=0.0, hi=100.0):
    """
    Clamp value to range [lo, hi]
    
    Args:
        x: Value to clamp
        lo: Lower bound (default 0)
        hi: Upper bound (default 100)
        
    Returns:
        Clamped value
    """
    return max(lo, min(hi, x))

def score_step(actual_pct: float, ideal_pct: float,
               step_down_pct: float, step_up_pct: float,
               points_per_step_down: float, points_per_step_up: float) -> float:
    """
    Calculate score using step function with inclusive upper boundaries
    
    Args:
        actual_pct: Actual percentage value
        ideal_pct: Ideal percentage value
        step_down_pct: Oscillation size below ideal (e.g., ideal * 0.02 for 2%)
        step_up_pct: Oscillation size above ideal (e.g., ideal * 0.06 for 6%)
        points_per_step_down: Points deducted per step below
        points_per_step_up: Points deducted per step above
        
    Returns:
        Score from 0 to 100
        
    Examples:
        ideal=90, osc_up=5.4 (6% of 90):
        - 90.0 to 95.4 = 100 points (step 0)
        - 95.5 to 100.8 = 90 points (step 1)
    """
    diff = actual_pct - ideal_pct
    
    if diff > 0:
        # Above ideal - use floor with fractional tolerance for inclusive upper bounds
        mult = diff / max(step_up_pct, 1e-9)
        steps = int(math.floor(mult))
        # If mult is very close to an integer > 0, stay in previous step (inclusive upper boundary)
        frac = mult - math.floor(mult)
        if frac < 0.0001 and steps > 0:
            steps -= 1
        penalty = steps * points_per_step_up
    elif diff < 0:
        # Below ideal - same logic
        abs_diff = abs(diff)
        mult = abs_diff / max(step_down_pct, 1e-9)
        steps = int(math.floor(mult))
        frac = mult - math.floor(mult)
        if frac < 0.0001 and steps > 0:
            steps -= 1
        penalty = steps * points_per_step_down
    else:
        # Exactly at ideal
        return 100.0
    
    return clamp(100.0 - penalty)

def score_linear(actual_pct: float, ideal_pct: float,
                 step_down_pct: float, step_up_pct: float,
                 points_per_step_down: float, points_per_step_up: float) -> float:
    """
    Calculate score using linear interpolation
    
    Args:
        actual_pct: Actual percentage value
        ideal_pct: Ideal percentage value
        step_down_pct: % below ideal per step (used as slope)
        step_up_pct: % above ideal per step (used as slope)
        points_per_step_down: Points deducted per step below
        points_per_step_up: Points deducted per step above
        
    Returns:
        Score from 0 to 100
    """
    diff = actual_pct - ideal_pct
    if diff >= 0:
        penalty = (diff / max(step_up_pct, 1e-9)) * points_per_step_up
    else:
        penalty = ((-diff) / max(step_down_pct, 1e-9)) * points_per_step_down
    return clamp(100.0 - penalty)

def pick_scorer(mode: str):
    """
    Select scoring function based on mode
    
    Args:
        mode: "step" or "linear"
        
    Returns:
        Scoring function
    """
    return score_step if (mode or "step") == "step" else score_linear

def score_to_note(score: float) -> str:
    """
    Convert a numerical score to a descriptive note.
    
    Args:
        score: Score value (0-100) or None
        
    Returns:
        Descriptive note in Portuguese
    """
    if score is None: 
        return "Sem amostra"
    if score >= 90: 
        return "Excelente"
    if score >= 75: 
        return "Bom"
    if score >= 60: 
        return "OK"
    if score >= 40: 
        return "A ajustar"
    return "Crítico"

# scoring.explain_stat(stat_key, pct, cfg) -> (grade, note)
def explain_stat(stat_key: str, pct: float, cfg: dict) -> tuple[str, str]:
    ideal = cfg.get("ideals", {}).get(stat_key)
    if not ideal or pct is None: return ("-", "")
    lo, hi = ideal.get("lo"), ideal.get("hi")
    if lo is None or hi is None: return ("-", "")
    if pct < lo: 
        d = round(lo - pct, 2)
        return ("C" if d < 3 else "D", f"Abaixo do ideal ({lo:.2f}–{hi:.2f}); falta {d:.2f} pp.")
    if pct > hi:
        d = round(pct - hi, 2)
        return ("C" if d < 3 else "D", f"Acima do ideal ({lo:.2f}–{hi:.2f}); excede {d:.2f} pp.")
    return ("A", f"Dentro do ideal ({lo:.2f}–{hi:.2f}).")