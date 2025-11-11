# app/derive/postflop.py
from typing import Dict, Any, List, Optional, Tuple

AGGR = {"BET", "RAISE", "ALLIN"}      # ações agressivas
PASS = {"CHECK"}                      # passivas sem meter fichas
CALL = {"CALL"}                       # call
FOLD = {"FOLD"}                       # fold

def _street_actions(hand: Dict[str, Any], street: str) -> List[Dict[str, Any]]:
    s = (hand.get("streets") or {}).get(street) or {}
    return s.get("actions") or []

def _first_action_idx_by(hand_actions: List[Dict[str, Any]], player: str) -> Optional[int]:
    for i, a in enumerate(hand_actions):
        if a.get("actor") == player:
            return i
    return None

def _first_aggr_idx(actions: List[Dict[str, Any]]) -> Optional[int]:
    for i, a in enumerate(actions):
        if (a.get("type") or "").upper() in AGGR:
            return i
    return None

def _is_aggr(action: Dict[str, Any]) -> bool:
    return (action.get("type") or "").upper() in AGGR

def _is_fold(action: Dict[str, Any]) -> bool:
    return (action.get("type") or "").upper() in FOLD

def _is_call(action: Dict[str, Any]) -> bool:
    return (action.get("type") or "").upper() in CALL

def _is_check(action: Dict[str, Any]) -> bool:
    return (action.get("type") or "").upper() in PASS

def _is_raise(action: Dict[str, Any]) -> bool:
    return (action.get("type") or "").upper() in {"RAISE", "ALLIN"}

def _actor_first_action(actions: List[Dict[str, Any]], actor: str) -> Optional[Dict[str, Any]]:
    idx = _first_action_idx_by(actions, actor)
    return actions[idx] if idx is not None else None

def _someone_bet_before(actions: List[Dict[str, Any]], idx_limit: int) -> bool:
    # Há BET/RAISE/ALLIN antes do índice idx_limit?
    if idx_limit is None:
        return False
    for i in range(idx_limit):
        if _is_aggr(actions[i]):
            return True
    return False

def _pfr_from_preflop_derived(hand: Dict[str, Any]) -> Optional[str]:
    dpf = (hand.get("derived") or {}).get("preflop") or {}
    # prioridade: 4bet > 3bet > open_raiser
    for k in ("four_bettor", "three_bettor", "open_raiser"):
        v = dpf.get(k)
        if v:
            return v
    # fallback: varrer preflop e apanhar último agressor
    pre = _street_actions(hand, "preflop")
    last_agg = None
    for a in pre:
        if _is_aggr(a):
            last_agg = a.get("actor")
    return last_agg

def _hero_is_ip_on(hand: Dict[str, Any], street: str) -> Optional[bool]:
    dip = (hand.get("derived") or {}).get("ip") or {}
    return dip.get(f"hero_ip_{street}")

def _saw_street(hand: Dict[str, Any], street: str) -> bool:
    return len(_street_actions(hand, street)) > 0

def _hero(hand: Dict[str, Any]) -> Optional[str]:
    return hand.get("hero")

def _players_to_flop(hand: Dict[str, Any]) -> int:
    dip = (hand.get("derived") or {}).get("ip") or {}
    return int(dip.get("players_to_flop") or 0)

def _pot_type(hand: Dict[str, Any]) -> str:
    dpf = (hand.get("derived") or {}).get("preflop") or {}
    return (dpf.get("pot_type") or "none").upper()

def _is_heads_up_on_street(hand: Dict[str, Any], street: str) -> bool:
    """Verifica se é heads-up numa street específica"""
    dip = (hand.get("derived") or {}).get("ip") or {}
    return bool(dip.get(f"heads_up_{street}", False))

def _hero_faced_cbet_on_street(hand: Dict[str, Any], pfr: str, street: str) -> bool:
    """Hero NÃO é PFR, primeiro bet na street vem do PFR, antes do herói atuar."""
    hero = _hero(hand)
    actions = _street_actions(hand, street)
    if not actions or not hero or not pfr or hero == pfr:
        return False
    first_aggr_i = _first_aggr_idx(actions)
    if first_aggr_i is None:
        return False
    first_aggr = actions[first_aggr_i].get("actor")
    # tem de ser o PFR a apostar primeiro
    if first_aggr != pfr:
        return False
    hero_first_i = _first_action_idx_by(actions, hero)
    return hero_first_i is not None and first_aggr_i < hero_first_i

def _hero_response_after(actions: List[Dict[str, Any]], hero: str, after_idx: int) -> Optional[Dict[str, Any]]:
    for i in range(after_idx + 1, len(actions)):
        a = actions[i]
        if a.get("actor") == hero:
            return a
    return None

def _aggression_frequency_street(hand: Dict[str, Any], street: str) -> Optional[float]:
    """Calcula aggression frequency (bets+raises/total actions) para uma street"""
    hero = _hero(hand)
    if not hero:
        return None
    actions = _street_actions(hand, street)
    if not actions:
        return None
    hero_actions = [a for a in actions if a.get("actor") == hero]
    if not hero_actions:
        return 0.0
    aggr = sum(1 for a in hero_actions if _is_aggr(a))
    return round(100.0 * aggr / max(1, len(hero_actions)), 2)

def _saw_showdown(hand: Dict[str, Any]) -> bool:
    """Verifica se a mão foi ao showdown"""
    streets = hand.get("streets") or {}
    if streets.get("showdown"):
        acts = (streets["showdown"] or {}).get("actions") or []
        if acts:
            return True
    # varre todas as streets à procura de SHOW/MUCK
    for st in ("river","turn","flop","preflop","showdown"):
        for a in _street_actions(hand, st):
            t = (a.get("type") or "").upper()
            if t in {"SHOW", "MUCK"}:
                return True
    return False

def _won_showdown(hand: Dict[str, Any]) -> bool:
    """Verifica se o hero ganhou no showdown"""
    hero = _hero(hand)
    winners = hand.get("winners")
    if hero and isinstance(winners, list):
        return hero in winners
    return False

def _won_when_saw_flop(hand: Dict[str, Any]) -> bool:
    """Verifica se o hero ganhou quando viu o flop"""
    if not _saw_street(hand, "flop"):
        return False
    hero = _hero(hand)
    winners = hand.get("winners")
    if hero and isinstance(winners, list):
        return hero in winners
    return False

def _pfr_checked_on_street(hand: Dict[str, Any], pfr: Optional[str], street: str) -> bool:
    """Verifica se o PFR fez check na street"""
    if not pfr:
        return False
    actions = _street_actions(hand, street)
    for a in actions:
        if a.get("actor") == pfr:
            if _is_check(a):
                return True
            elif _is_aggr(a):
                return False  # Se apostou, não checkaram
    return False

def _hero_check_raised_street(hand: Dict[str, Any], street: str) -> Tuple[bool, bool]:
    """Retorna (opportunity, attempt) para check-raise na street"""
    hero = _hero(hand)
    if not hero:
        return (False, False)
    
    actions = _street_actions(hand, street)
    if not actions:
        return (False, False)
    
    # Procurar sequência: hero check -> alguém bet -> hero raise
    hero_check_idx = None
    for i, a in enumerate(actions):
        if a.get("actor") == hero and _is_check(a):
            hero_check_idx = i
            break
    
    if hero_check_idx is None:
        return (False, False)
    
    # Procurar bet após o check do hero
    bet_after_check = False
    bet_idx = None
    for i in range(hero_check_idx + 1, len(actions)):
        if _is_aggr(actions[i]):
            bet_after_check = True
            bet_idx = i
            break
    
    if not bet_after_check:
        return (False, False)
    
    # Opportunity exists
    opp = True
    
    # Procurar raise do hero após o bet
    att = False
    if bet_idx is not None:
        for i in range(bet_idx + 1, len(actions)):
            if actions[i].get("actor") == hero:
                if _is_raise(actions[i]):
                    att = True
                break
    
    return (opp, att)

def _probe_bet_opportunity_and_attempt(hand: Dict[str, Any], street: str, pfr: Optional[str]) -> Tuple[bool, bool]:
    """
    Probe bet: quando o agressor (PFR) não cbeta na street anterior, 
    e o hero tem oportunidade de apostar
    """
    hero = _hero(hand)
    if not hero or not pfr or hero == pfr:
        return (False, False)
    
    # Para probe no turn, verificar se PFR não cbetou no flop
    # Para probe no river, verificar se PFR não cbetou no turn
    prev_street = "flop" if street == "turn" else "turn" if street == "river" else None
    if not prev_street:
        return (False, False)
    
    # Verificar se PFR checkaram na street anterior
    if not _pfr_checked_on_street(hand, pfr, prev_street):
        return (False, False)
    
    # Verificar ação do hero na street atual
    actions = _street_actions(hand, street)
    if not actions:
        return (False, False)
    
    hero_idx = _first_action_idx_by(actions, hero)
    if hero_idx is None:
        return (False, False)
    
    # Verificar se ninguém apostou antes do hero
    nobody_bet_before = not _someone_bet_before(actions, hero_idx)
    if not nobody_bet_before:
        return (False, False)
    
    # Opportunity exists
    opp = True
    
    # Attempt se hero aposta
    att = _is_aggr(actions[hero_idx])
    
    return (opp, att)

def derive_postflop(hand: Dict[str, Any]) -> Dict[str, Any]:
    """
    Implementação completa das derivações postflop para fase 8.2.A
    Inclui todas as métricas por street com suporte IP/OOP e SRP/3BP
    """
    hero = _hero(hand)
    pfr = _pfr_from_preflop_derived(hand)
    pot_type = _pot_type(hand)
    
    # Inicializar resultado com valores default
    d: Dict[str, Any] = {
        "pfr_player": pfr,
        "pot_type": pot_type,
        
        # Flags de visualização de streets
        "saw_flop": _saw_street(hand, "flop"),
        "saw_turn": _saw_street(hand, "turn"),
        "saw_river": _saw_street(hand, "river"),
        "saw_showdown": _saw_showdown(hand),
        "won_showdown": _won_showdown(hand),
        "won_when_saw_flop": _won_when_saw_flop(hand),
        
        # Heads-up flags por street
        "heads_up_flop": _is_heads_up_on_street(hand, "flop"),
        "heads_up_turn": _is_heads_up_on_street(hand, "turn"),
        "heads_up_river": _is_heads_up_on_street(hand, "river"),
        
        # IP/OOP flags
        "hero_ip_flop": _hero_is_ip_on(hand, "flop"),
        "hero_ip_turn": _hero_is_ip_on(hand, "turn"),
        "hero_ip_river": _hero_is_ip_on(hand, "river"),
        
        # Aggression frequency por street
        "agg_pct_flop": _aggression_frequency_street(hand, "flop"),
        "agg_pct_turn": _aggression_frequency_street(hand, "turn"),
        "agg_pct_river": _aggression_frequency_street(hand, "river"),
    }
    
    # Se não há hero ou não viu flop, retornar apenas básico
    if not hero or not d["saw_flop"]:
        # Inicializar todos os campos com False para consistência
        for street in ["flop", "turn", "river"]:
            for metric in ["cbet", "vs_cbet", "probe", "donk", "xr", "bet_vs_missed"]:
                for suffix in ["_opp_ip", "_att_ip", "_opp_oop", "_att_oop", "_opp", "_att",
                              "_fold_ip", "_call_ip", "_raise_ip", "_fold_oop", "_call_oop", "_raise_oop"]:
                    key = f"{metric}_{street}{suffix}"
                    if key not in d:
                        d[key] = False
        return d
    
    # Processar cada street
    for street in ["flop", "turn", "river"]:
        if not _saw_street(hand, street):
            # Inicializar campos da street como False se não viu
            for metric in ["cbet", "vs_cbet", "probe", "donk", "xr", "bet_vs_missed"]:
                for suffix in ["_opp_ip", "_att_ip", "_opp_oop", "_att_oop", "_opp", "_att",
                              "_fold_ip", "_call_ip", "_raise_ip", "_fold_oop", "_call_oop", "_raise_oop"]:
                    key = f"{metric}_{street}{suffix}"
                    d[key] = False
            continue
        
        actions = _street_actions(hand, street)
        hero_ip = _hero_is_ip_on(hand, street)
        
        # ========== CBET (Hero é PFR) ==========
        d[f"cbet_{street}_opp_ip"] = False
        d[f"cbet_{street}_att_ip"] = False
        d[f"cbet_{street}_opp_oop"] = False
        d[f"cbet_{street}_att_oop"] = False
        
        if pfr and hero == pfr:
            h_i = _first_action_idx_by(actions, hero)
            if h_i is not None:
                nobody_bet_before = not _someone_bet_before(actions, h_i)
                if nobody_bet_before:
                    # Opportunity
                    if hero_ip is True:
                        d[f"cbet_{street}_opp_ip"] = True
                        # Attempt
                        if _is_aggr(actions[h_i]):
                            d[f"cbet_{street}_att_ip"] = True
                    elif hero_ip is False:
                        d[f"cbet_{street}_opp_oop"] = True
                        # Attempt
                        if _is_aggr(actions[h_i]):
                            d[f"cbet_{street}_att_oop"] = True
        
        # ========== VS CBET (Hero não é PFR) ==========
        d[f"vs_cbet_{street}_fold_ip"] = False
        d[f"vs_cbet_{street}_call_ip"] = False
        d[f"vs_cbet_{street}_raise_ip"] = False
        d[f"vs_cbet_{street}_fold_oop"] = False
        d[f"vs_cbet_{street}_call_oop"] = False
        d[f"vs_cbet_{street}_raise_oop"] = False
        
        if pfr and hero != pfr:
            first_aggr_i = _first_aggr_idx(actions)
            if first_aggr_i is not None:
                first_bettor = actions[first_aggr_i].get("actor")
                if first_bettor == pfr:
                    h_i = _first_action_idx_by(actions, hero)
                    if h_i is not None and first_aggr_i < h_i:
                        resp = actions[h_i]
                        if hero_ip is True:
                            if _is_fold(resp):
                                d[f"vs_cbet_{street}_fold_ip"] = True
                            elif _is_call(resp):
                                d[f"vs_cbet_{street}_call_ip"] = True
                            elif _is_aggr(resp):
                                d[f"vs_cbet_{street}_raise_ip"] = True
                        elif hero_ip is False:
                            if _is_fold(resp):
                                d[f"vs_cbet_{street}_fold_oop"] = True
                            elif _is_call(resp):
                                d[f"vs_cbet_{street}_call_oop"] = True
                            elif _is_aggr(resp):
                                d[f"vs_cbet_{street}_raise_oop"] = True
        
        # ========== PROBE BET ==========
        d[f"probe_{street}_opp_ip"] = False
        d[f"probe_{street}_att_ip"] = False
        d[f"probe_{street}_opp_oop"] = False
        d[f"probe_{street}_att_oop"] = False
        
        if street in ["turn", "river"]:
            probe_opp, probe_att = _probe_bet_opportunity_and_attempt(hand, street, pfr)
            if probe_opp:
                if hero_ip is True:
                    d[f"probe_{street}_opp_ip"] = True
                    if probe_att:
                        d[f"probe_{street}_att_ip"] = True
                elif hero_ip is False:
                    d[f"probe_{street}_opp_oop"] = True
                    if probe_att:
                        d[f"probe_{street}_att_oop"] = True
        
        # ========== DONK BET (Hero OOP, não é PFR, aposta antes do PFR) ==========
        d[f"donk_{street}_opp"] = False
        d[f"donk_{street}_att"] = False
        
        if pfr and hero != pfr and hero_ip is False:
            h_i = _first_action_idx_by(actions, hero)
            pfr_i = _first_action_idx_by(actions, pfr)
            if h_i is not None and (pfr_i is None or h_i < pfr_i):
                # Opportunity: hero age antes do PFR
                d[f"donk_{street}_opp"] = True
                # Attempt: se hero aposta
                if _is_aggr(actions[h_i]):
                    d[f"donk_{street}_att"] = True
        
        # ========== CHECK-RAISE ==========
        xr_opp, xr_att = _hero_check_raised_street(hand, street)
        d[f"xr_{street}_opp"] = xr_opp
        d[f"xr_{street}_att"] = xr_att
        
        # ========== BET VS MISSED CBET ==========
        d[f"bet_vs_missed_{street}_opp_ip"] = False
        d[f"bet_vs_missed_{street}_att_ip"] = False
        d[f"bet_vs_missed_{street}_opp_oop"] = False
        d[f"bet_vs_missed_{street}_att_oop"] = False
        
        if pot_type == "SRP" and pfr and hero != pfr:
            # PFR não apostou na street
            first_aggr_i = _first_aggr_idx(actions)
            pfr_first_i = _first_action_idx_by(actions, pfr)
            h_i = _first_action_idx_by(actions, hero)
            
            if h_i is not None:
                nobody_bet_before_hero = not _someone_bet_before(actions, h_i)
                # Verificar se PFR não apostou antes do hero
                pfr_bet_before_hero = False
                if pfr_first_i is not None and pfr_first_i < h_i:
                    for i in range(pfr_first_i, min(h_i, len(actions))):
                        if actions[i].get("actor") == pfr and _is_aggr(actions[i]):
                            pfr_bet_before_hero = True
                            break
                
                if nobody_bet_before_hero and not pfr_bet_before_hero:
                    # Opportunity
                    if hero_ip is True:
                        d[f"bet_vs_missed_{street}_opp_ip"] = True
                        # Attempt
                        if _is_aggr(actions[h_i]):
                            d[f"bet_vs_missed_{street}_att_ip"] = True
                    elif hero_ip is False:
                        d[f"bet_vs_missed_{street}_opp_oop"] = True
                        # Attempt
                        if _is_aggr(actions[h_i]):
                            d[f"bet_vs_missed_{street}_att_oop"] = True
    
    # ========== DELAYED CBET (Turn/River) ==========
    # Delayed CBet Turn: PFR não cbetou flop mas cbeta turn
    d["delayed_cbet_turn_opp_ip"] = False
    d["delayed_cbet_turn_att_ip"] = False
    d["delayed_cbet_turn_opp_oop"] = False
    d["delayed_cbet_turn_att_oop"] = False
    
    if pfr and hero == pfr and d["saw_turn"]:
        # Verificar se não cbetou no flop
        flop_cbet = d["cbet_flop_att_ip"] or d["cbet_flop_att_oop"]
        if not flop_cbet:
            # Verificar oportunidade no turn
            turn_actions = _street_actions(hand, "turn")
            h_i = _first_action_idx_by(turn_actions, hero)
            if h_i is not None:
                nobody_bet_before = not _someone_bet_before(turn_actions, h_i)
                if nobody_bet_before:
                    hero_ip_turn = _hero_is_ip_on(hand, "turn")
                    if hero_ip_turn is True:
                        d["delayed_cbet_turn_opp_ip"] = True
                        if _is_aggr(turn_actions[h_i]):
                            d["delayed_cbet_turn_att_ip"] = True
                    elif hero_ip_turn is False:
                        d["delayed_cbet_turn_opp_oop"] = True
                        if _is_aggr(turn_actions[h_i]):
                            d["delayed_cbet_turn_att_oop"] = True
    
    # Delayed CBet River
    d["delayed_cbet_river_opp_ip"] = False
    d["delayed_cbet_river_att_ip"] = False
    d["delayed_cbet_river_opp_oop"] = False
    d["delayed_cbet_river_att_oop"] = False
    
    if pfr and hero == pfr and d["saw_river"]:
        # Verificar se não cbetou no turn
        turn_cbet = d["cbet_turn_att_ip"] or d["cbet_turn_att_oop"]
        if not turn_cbet:
            # Verificar oportunidade no river
            river_actions = _street_actions(hand, "river")
            h_i = _first_action_idx_by(river_actions, hero)
            if h_i is not None:
                nobody_bet_before = not _someone_bet_before(river_actions, h_i)
                if nobody_bet_before:
                    hero_ip_river = _hero_is_ip_on(hand, "river")
                    if hero_ip_river is True:
                        d["delayed_cbet_river_opp_ip"] = True
                        if _is_aggr(river_actions[h_i]):
                            d["delayed_cbet_river_att_ip"] = True
                    elif hero_ip_river is False:
                        d["delayed_cbet_river_opp_oop"] = True
                        if _is_aggr(river_actions[h_i]):
                            d["delayed_cbet_river_att_oop"] = True
    
    # ========== WWSF (Won When Saw Flop) ==========
    # Já calculado acima
    
    # ========== WTSD (Went To Showdown) ==========
    d["wtsd"] = d["saw_showdown"]  # Opportunity: saw_flop, Attempt: saw_showdown
    
    # ========== W$SD (Won $ at Showdown) ==========
    d["w_sd"] = d["won_showdown"]  # Opportunity: saw_showdown, Attempt: won_showdown
    
    # ========== W$WSF (Won $ When Saw Flop) ==========
    d["w_wsf"] = d["won_when_saw_flop"]  # Já existe
    
    # ========== Fold vs Check-Raise ==========
    d["fold_vs_check_raise_opp"] = False
    d["fold_vs_check_raise_att"] = False
    
    # Verificar se hero enfrentou check-raise em qualquer street
    for street in ["flop", "turn", "river"]:
        if not _saw_street(hand, street):
            continue
        actions = _street_actions(hand, street)
        # Procurar hero bet e rastrear todos que fizeram check antes
        for i, a in enumerate(actions):
            if a.get("actor") == hero and _is_aggr(a):
                # Rastrear todos os jogadores que fizeram check antes do hero apostar
                checkers = set()
                for j in range(i):
                    if _is_check(actions[j]):
                        checkers.add(actions[j].get("actor"))
                
                # Verificar se algum dos checkers faz raise depois do hero
                if checkers:
                    for j in range(i+1, len(actions)):
                        if actions[j].get("actor") in checkers and _is_raise(actions[j]):
                            # Check-raise aconteceu (jogador que fez check agora fez raise)
                            d["fold_vs_check_raise_opp"] = True
                            # Verificar resposta do hero
                            for k in range(j+1, len(actions)):
                                if actions[k].get("actor") == hero:
                                    if _is_fold(actions[k]):
                                        d["fold_vs_check_raise_att"] = True
                                    break
                            break
    
    # ========== River Bet Single Raised Pot ==========
    d["river_bet_srp_opp"] = False
    d["river_bet_srp_att"] = False
    
    if pot_type == "SRP" and d["saw_river"] and _is_heads_up_on_street(hand, "river"):
        river_actions = _street_actions(hand, "river")
        h_i = _first_action_idx_by(river_actions, hero)
        if h_i is not None:
            nobody_bet_before = not _someone_bet_before(river_actions, h_i)
            if nobody_bet_before:
                d["river_bet_srp_opp"] = True
                if _is_aggr(river_actions[h_i]):
                    d["river_bet_srp_att"] = True
    
    # ========== W$SD when Bet River ==========
    d["w_sd_b_river_opp"] = False
    d["w_sd_b_river_att"] = False
    
    if d["saw_river"] and d["saw_showdown"]:
        # Verificar se hero apostou no river
        river_actions = _street_actions(hand, "river")
        hero_bet_river = False
        for a in river_actions:
            if a.get("actor") == hero and _is_aggr(a):
                hero_bet_river = True
                break
        
        if hero_bet_river:
            d["w_sd_b_river_opp"] = True
            if d["won_showdown"]:
                d["w_sd_b_river_att"] = True
    
    # ========== Métricas adicionais para compatibilidade ==========
    # Manter campos legados para backward compatibility
    d["river_agg_pct"] = d["agg_pct_river"]
    
    # Campos específicos do flop para DSL atual
    d["cbet_flop_opp_ip"] = d.get("cbet_flop_opp_ip", False)
    d["cbet_flop_att_ip"] = d.get("cbet_flop_att_ip", False)
    d["cbet_flop_opp_oop"] = d.get("cbet_flop_opp_oop", False)
    d["cbet_flop_att_oop"] = d.get("cbet_flop_att_oop", False)
    
    d["cbet_turn_opp_ip"] = d.get("cbet_turn_opp_ip", False)
    d["cbet_turn_att_ip"] = d.get("cbet_turn_att_ip", False)
    d["cbet_turn_opp_oop"] = d.get("cbet_turn_opp_oop", False)
    d["cbet_turn_att_oop"] = d.get("cbet_turn_att_oop", False)
    
    d["donk_flop"] = d.get("donk_flop_att", False)
    d["donk_turn"] = d.get("donk_turn_att", False)
    
    d["vs_cbet_flop_fold_ip"] = d.get("vs_cbet_flop_fold_ip", False)
    d["vs_cbet_flop_raise_ip"] = d.get("vs_cbet_flop_raise_ip", False)
    d["vs_cbet_flop_fold_oop"] = d.get("vs_cbet_flop_fold_oop", False)
    d["vs_cbet_flop_raise_oop"] = d.get("vs_cbet_flop_raise_oop", False)
    
    d["flop_bet_vs_missed_cbet_srp"] = d.get("bet_vs_missed_flop_att_ip", False) or d.get("bet_vs_missed_flop_att_oop", False)
    d["turn_bet_vs_missed_cbet_srp_oop"] = d.get("bet_vs_missed_turn_att_oop", False)
    
    return d