"""
Preflop derivation logic for poker hands (Phase 3)
"""
import logging
from typing import Dict, Optional, List
from app.parse.schemas import Hand, Action
from app.derive.positions import assign_positions

logger = logging.getLogger(__name__)


def derive_preflop_data(hand: Hand) -> Dict:
    """
    Derive complete preflop statistics and metadata from parsed hand.
    
    Returns:
        Dict with all preflop derived data matching DerivedPreflop schema
    """
    try:
        hero = hand.hero
        preflop = hand.streets.get("preflop")
        acts = preflop.actions if preflop else []
        
        # Get basic data
        positions = assign_positions(hand)
        hero_pos = positions.get(hero) if hero else None
        
        # Analyze preflop action
        unopened = is_unopened_pot(acts, hero)
        limper_before = has_limper_before(acts, hero) if hero else False
        open_raiser = first_raiser(acts)
        hero_rfi = actor_is_first_raiser(acts, hero) if hero else False
        is_iso = hero_rfi and limper_exists(acts)
        
        # 3-bet/4-bet analysis
        three_bettor, four_bettor = who_3bet_4bet(acts)
        faced_3bet = hero_faced_3bet(acts, hero, open_raiser) if hero else False
        folded_to_3bet = hero_folded_to_3bet(acts, hero) if hero else False
        
        # Special situations
        is_squeeze = detect_squeeze(acts, hero) if hero else False
        is_resteal = detect_resteal_vs_btn(acts, hero, hand, open_raiser) if hero else False
        freeplay = detect_freeplay_bb(acts, hand)
        
        # Pot type
        pot_type = classify_pot_type(acts)
        
        return {
            # Basic hero data
            "hero_vpip": hero_vpip(acts, hero) if hero else False,
            "hero_position": hero_pos,
            "pot_size_bb": compute_pot_size_flop_bb(hand),
            
            # Preflop action analysis
            "unopened_pot": unopened,
            "has_limper_before_hero": limper_before,
            "open_raiser": open_raiser,
            "hero_raised_first_in": hero_rfi,
            "is_isoraiser": is_iso,
            "three_bettor": three_bettor,
            "four_bettor": four_bettor,
            "faced_3bet": faced_3bet,
            "folded_to_3bet": folded_to_3bet,
            "is_squeeze": is_squeeze,
            "is_resteal_vs_btn": is_resteal,
            "pot_type": pot_type,
            "freeplay_bb": freeplay
        }
    except Exception as e:
        logger.error(f"Error deriving preflop for hand {getattr(hand, 'tournament_id', 'unknown')}: {e}")
        from app.derive.config import get_default_preflop_values
        # Return complete default values matching schema
        defaults = get_default_preflop_values()
        defaults.update({
            "unopened_pot": False,
            "has_limper_before_hero": False,
            "open_raiser": None,
            "hero_raised_first_in": False,
            "is_isoraiser": False,
            "three_bettor": None,
            "four_bettor": None,
            "faced_3bet": False,
            "folded_to_3bet": False,
            "is_squeeze": False,
            "is_resteal_vs_btn": False,
            "pot_type": "none",
            "freeplay_bb": False
        })
        return defaults


def hero_vpip(acts: list[Action], hero: Optional[str]) -> bool:
    """
    Check if hero voluntarily put money in pot (VPIP) preflop.
    
    Args:
        acts: List of preflop actions
        hero: Hero's name
        
    Returns:
        True if hero made CALL/RAISE/RERAISE/ALLIN/BET, False otherwise
    """
    if not hero:
        return False
        
    for a in acts:
        if a.actor != hero: 
            continue
        if a.type in ("CALL", "RAISE", "RERAISE", "ALLIN", "BET"): 
            return True
        if a.type in ("FOLD", "CHECK"):
            # Se a primeira ação do hero foi fold ou check, não VPIP
            return False
    return False


def compute_pot_size_flop_bb(hand: Hand) -> Optional[float]:
    """
    Aproxima o pote à entrada do flop simulando contribuições do pré-flop.
    
    Regras:
      - Somar POST_SB/POST_BB/POST_ANTE a partir das ações pré-flop (se montadas).
      - Para CALL/RAISE/ALLIN, somar a contribuição incremental (não 'to_amount' bruto).
      
    Fallback: se não conseguir inferir incrementalmente, retorna None.
    """
    bb = float(hand.blinds.get("bb") or 0)
    if bb <= 0: 
        return None
        
    preflop = hand.streets.get("preflop")
    acts = preflop.actions if preflop else []
    if not acts:
        # Pelo menos blinds entram
        base = float(hand.blinds.get("sb") or 0) + float(hand.blinds.get("bb") or 0)
        ante = float(hand.blinds.get("ante") or 0)
        if ante > 0:
            base += ante * len(hand.players_dealt_in or hand.players)
        return round(base / bb, 2) if bb > 0 else None

    invested = {p.name: 0.0 for p in hand.players}
    current_to_call = float(hand.blinds.get("bb") or 0.0)  # após posts
    pot = 0.0

    # Primeiro, tentar somar posts/antes explícitos
    for a in acts:
        if a.type in ("POST_SB", "POST_BB", "POST_ANTE") and a.amount:
            invested[a.actor] = invested.get(a.actor, 0.0) + float(a.amount or 0)
            pot += float(a.amount or 0)

    # Atualiza current_to_call pelo BB após posts
    if current_to_call > 0 and pot == 0.0:
        pot = float(hand.blinds.get("sb") or 0) + float(hand.blinds.get("bb") or 0)
        ante = float(hand.blinds.get("ante") or 0)
        if ante > 0:
            pot += ante * len(hand.players_dealt_in or hand.players)

    # Simulação simples de contribuição incremental
    for a in acts:
        if a.type == "CALL":
            need = max(0.0, current_to_call - invested.get(a.actor, 0.0))
            invested[a.actor] = max(invested.get(a.actor, 0.0), current_to_call)
            pot += need
        elif a.type in ("RAISE", "RERAISE", "ALLIN"):
            # ideal: usar a.to_amount; fallback: usar a.amount como incremento
            to_amt = float(getattr(a, "to_amount", 0.0) or 0.0)
            if to_amt > 0:
                inc = max(0.0, to_amt - invested.get(a.actor, 0.0))
                invested[a.actor] = to_amt
                current_to_call = to_amt
                pot += inc
            elif a.amount:
                # se só amount estiver presente, assume incremento
                inc = float(a.amount or 0.0)
                invested[a.actor] = invested.get(a.actor, 0.0) + inc
                current_to_call = invested[a.actor]
                pot += inc
        elif a.type == "FOLD":
            continue
        # STOP ao encontrar "FLOP": garantido pelo runner/street offsets (pré-flop apenas)

    return round(pot / bb, 2) if pot > 0 else None


def is_unopened_pot(acts: List[Action], until_actor: Optional[str] = None) -> bool:
    """
    Check if pot is unopened (no raises or calls) until specified actor.
    
    Args:
        acts: List of preflop actions
        until_actor: Stop checking when this actor is reached
        
    Returns:
        True if no raises or calls before until_actor
    """
    for a in acts:
        if until_actor and a.actor == until_actor:
            break
        if a.type in ("RAISE", "RERAISE", "ALLIN"): 
            return False
        if a.type == "CALL": 
            return False
    return True


def has_limper_before(acts: List[Action], hero: str) -> bool:
    """
    Check if there's a limper before hero's action.
    
    Args:
        acts: List of preflop actions
        hero: Hero's name
        
    Returns:
        True if someone limped before hero acted
    """
    for a in acts:
        if a.actor == hero: 
            break
        if a.type == "CALL": 
            return True
        if a.type in ("RAISE", "RERAISE", "ALLIN"): 
            return False
    return False


def limper_exists(acts: List[Action]) -> bool:
    """
    Check if any limper exists in the hand.
    
    Args:
        acts: List of preflop actions
        
    Returns:
        True if there's at least one limper before any raise
    """
    for a in acts:
        if a.type == "CALL": 
            return True
        if a.type in ("RAISE", "RERAISE", "ALLIN"): 
            return False
    return False


def actor_raises_before_any_raise(acts: List[Action], actor: str) -> bool:
    """
    Check if specified actor raises before anyone else raises.
    
    Args:
        acts: List of preflop actions
        actor: Actor to check
        
    Returns:
        True if actor raises first (RFI), False if someone else raised first
    """
    for a in acts:
        if a.actor == actor and a.type in ("RAISE", "ALLIN"):
            return True
        if a.type in ("RAISE", "ALLIN") and a.actor != actor:
            return False
    return False


def first_raiser(acts: List[Action]) -> Optional[str]:
    """Get the first player to raise in the hand."""
    for a in acts:
        if a.type in ("RAISE", "ALLIN"):
            return a.actor
    return None


def is_rfi(acts: List[Action], actor: str) -> bool:
    """
    Check if actor made a true RFI (Raise First In).
    
    RFI means:
    - Actor is the first to raise preflop
    - ALL actions before actor's raise are FOLD (or blinds/antes)
    - NO limpers (CALL) before actor's raise
    
    This excludes:
    - ISO (limper + actor raise)
    - 3bet (raise + actor reraise)
    - BB raise after SB limp
    
    Args:
        acts: List of preflop actions
        actor: Actor to check
        
    Returns:
        True if actor made RFI, False otherwise
    """
    if not actor:
        return False
    
    hero_raised = False
    
    for a in acts:
        # Check if this is hero's raise
        if a.actor == actor and a.type in ("RAISE", "ALLIN"):
            hero_raised = True
            break
        
        # Before hero raises, check for invalid actions
        if a.actor != actor:
            # CALL (limp) invalidates RFI
            if a.type == "CALL":
                return False
            # Any RAISE before hero invalidates RFI
            if a.type in ("RAISE", "ALLIN"):
                return False
            # FOLD, POST_SB, POST_BB, POST_ANTE are ok
        else:
            # Hero's own actions before raise
            # POST_SB, POST_BB, POST_ANTE are ok (blinds/antes)
            if a.type in ("POST_SB", "POST_BB", "POST_ANTE"):
                continue  # Allow hero's blind/ante posts
            # Any other action (CALL, CHECK, FOLD) invalidates RFI
            if a.type not in ("RAISE", "ALLIN"):
                return False
    
    return hero_raised


def actor_is_first_raiser(acts: List[Action], actor: str) -> bool:
    """Check if actor is the first raiser (open raiser)."""
    for a in acts:
        if a.type in ("RAISE", "ALLIN"):
            return a.actor == actor
        if a.actor == actor and a.type not in ("RAISE", "ALLIN"):
            return False
    return False


def raises_sequence(acts: List[Action]) -> List[str]:
    """Get sequence of all raisers in order."""
    return [a.actor for a in acts if a.type in ("RAISE", "RERAISE", "ALLIN")]


def who_3bet_4bet(acts: List[Action]) -> tuple[Optional[str], Optional[str]]:
    """Identify the 3-bettor and 4-bettor if they exist."""
    seq = raises_sequence(acts)
    three = seq[1] if len(seq) >= 2 else None
    four = seq[2] if len(seq) >= 3 else None
    return three, four


def hero_faced_3bet(acts: List[Action], hero: str, open_raiser: Optional[str]) -> bool:
    """Check if hero faced a 3-bet after opening."""
    if open_raiser != hero: 
        return False
    seen_hero_raise = False
    for a in acts:
        if a.actor == hero and a.type in ("RAISE", "ALLIN"): 
            seen_hero_raise = True
        elif seen_hero_raise and a.type in ("RERAISE", "RAISE", "ALLIN"): 
            return True
    return False


def hero_folded_to_3bet(acts: List[Action], hero: str) -> bool:
    """Check if hero folded to a 3-bet."""
    seen_reraise = False
    for a in acts:
        if a.actor != hero and a.type in ("RERAISE", "RAISE", "ALLIN"):
            seen_reraise = True
        elif seen_reraise and a.actor == hero:
            return a.type == "FOLD"
    return False


def detect_squeeze(acts: List[Action], hero: str) -> bool:
    """Detect if hero made a squeeze play (3-bet after raise and call)."""
    raiser_seen = False
    call_seen = False
    for a in acts:
        if a.type in ("RAISE", "ALLIN") and not raiser_seen:
            raiser_seen = True
            continue
        if raiser_seen and a.type == "CALL":
            call_seen = True
            continue
        if raiser_seen and call_seen and a.actor == hero and a.type in ("RERAISE", "RAISE", "ALLIN"):
            # Hero 3-bets after raise and call = squeeze
            return True
    return False


def hero_is_3bettor(acts: List[Action], hero: str) -> bool:
    """Check if hero is the 3-bettor."""
    seq = raises_sequence(acts)
    return len(seq) >= 2 and seq[1] == hero


def detect_resteal_vs_btn(acts: List[Action], hero: str, hand: Hand, open_raiser: Optional[str]) -> bool:
    """Detect if hero made a resteal from blinds vs BTN open."""
    if not open_raiser or open_raiser == hero: 
        return False
    abs_pos = assign_positions(hand)
    opener_pos = abs_pos.get(open_raiser, "")
    hero_pos = abs_pos.get(hero, "")
    return (opener_pos == "BTN") and (hero_pos in {"SB", "BB"}) and hero_is_3bettor(acts, hero)


def detect_freeplay_bb(acts: List[Action], hand: Hand) -> bool:
    """Detect if BB got a free play (SB completes, BB checks)."""
    # No raises, SB calls, BB checks = freeplay
    saw_raise = any(a.type in ("RAISE", "ALLIN") for a in acts)
    if saw_raise: 
        return False
    abs_pos = assign_positions(hand)
    inv = {v: k for k, v in abs_pos.items()}
    sb = inv.get("SB")
    bb = inv.get("BB")
    if not sb or not bb: 
        return False
    sb_called = any(a.actor == sb and a.type == "CALL" for a in acts)
    bb_checked = any(a.actor == bb and a.type == "CHECK" for a in acts)
    return sb_called and bb_checked


def classify_pot_type(acts: List[Action]) -> str:
    """Classify pot type based on raise sequence."""
    seq = raises_sequence(acts)
    if not seq: 
        return "none"
    if len(seq) == 1: 
        return "SRP"
    if len(seq) == 2: 
        return "3bet"
    return "4bet"