"""
Postflop Statistics Calculator
Calcula as 20 stats de postflop definidas no DSL
Similar ao PreflopStats mas para ações pós-flop
"""
import re
from typing import Dict, Any, Optional
from collections import defaultdict
from .postflop_site_parsers import get_parser_for_hand
from .scoring_config import SCORING_CONFIG

class PostflopCalculator:
    """
    Calcula estatísticas de postflop a partir de hands de texto.
    """
    
    def __init__(self, hand_collector=None):
        self.stats = defaultdict(lambda: {"opportunities": 0, "attempts": 0})
        self.hands_processed = 0
        self.hand_collector = hand_collector
        self.current_hand_text = None
        self.current_hand_id = None
    
    def _collect_hand(self, stat_name: str):
        """Helper method to collect hand sample for a stat opportunity"""
        if self.hand_collector and self.current_hand_text:
            self.hand_collector.add_hand(stat_name, self.current_hand_text, self.current_hand_id)
        
    def analyze_hand(self, hand_text: str) -> None:
        """
        Analisa uma mão em texto e extrai stats de postflop.
        Usa parser específico para cada sala de poker.
        
        IMPORTANTE: Total de mãos inclui TODAS as mãos.
        Filtros são aplicados DENTRO de cada stat.
        """
        # Store current hand for collection
        if self.hand_collector:
            self.current_hand_text = hand_text
            # Extract hand ID
            hand_id_match = re.search(r'(?:Poker Hand #|PokerStars Hand #|Game #|Hand #)([A-Z0-9]+)', hand_text)
            self.current_hand_id = hand_id_match.group(1) if hand_id_match else None
        
        # Get appropriate parser for this hand
        parser = get_parser_for_hand(hand_text)
        if not parser:
            return  # Cannot identify poker site
        
        # Extract hero name using site-specific parser
        hero_name = parser.extract_hero_name(hand_text)
        if not hero_name:
            return  # No hero identified
        
        # Count ALL hands (critical change - no early filtering)
        self.hands_processed += 1
        
        # Extract all necessary data upfront
        preflop_actions = parser.extract_preflop_actions(hand_text)
        
        # Check conditions that apply to multiple stats (but DON'T filter globally)
        reached_flop = parser.detect_flop(hand_text)
        has_preflop_allin = parser.detect_preflop_allin(hand_text)
        hero_folded_preflop = any(player == hero_name and action == 'fold' 
                                   for player, action, _ in preflop_actions)
        
        # Continue processing - filters applied within each stat
        # Determinar se Hero é o PFR (Pre-Flop Raiser)
        hero_is_pfr = parser.identify_pfr(hand_text, hero_name)
        
        # Determinar posição (IP/OOP) no flop
        position = parser.determine_position_on_flop(hand_text, hero_name)
        hero_ip = position == 'IP'
        hero_oop = position == 'OOP'
        
        # ============== FLOP CBET GROUP ==============
        # Stats in exact order as specified
        
        # Get flop actions using parser
        flop_actions = parser.extract_flop_actions(hand_text)
        
        # Check if it's a 3bet pot (multiple raises preflop) - Define here so it's available everywhere
        raise_count = sum(1 for _, action, _ in preflop_actions if action == 'raise')
        is_3bet_pot = raise_count >= 2
        
        # IMPORTANT: Count opportunities for ALL valid hands (no reached_flop filter)
        # Only attempts are conditional on actually reaching the flop
        if True:  # Process all hands for opportunities
            # Check if Hero bet on flop (CBet)
            hero_bet_flop = any(player == hero_name and action == 'bet' 
                               for player, action, _ in flop_actions)
            
            # Check if Hero is 3better (made the 3bet preflop)
            hero_is_3better = False
            if is_3bet_pot:
                # Count raises until we find the second raise (3bet)
                raise_count = 0
                for player, action, _ in preflop_actions:
                    if action == 'raise':
                        raise_count += 1
                        if raise_count == 2 and player == hero_name:
                            hero_is_3better = True
                            break
            
            # Check if action was checked to Hero (only if reached flop and IP)
            checked_to_hero = False
            if reached_flop and hero_ip and flop_actions:
                for i, (player, action, _) in enumerate(flop_actions):
                    if player != hero_name and action == 'check':
                        # Check if next action is Hero's
                        if i + 1 < len(flop_actions) and flop_actions[i + 1][0] == hero_name:
                            checked_to_hero = True
                            break
            
            if hero_is_pfr and hero_ip and reached_flop and checked_to_hero:
                self.stats["Flop CBet IP %"]["opportunities"] += 1
                self._collect_hand("Flop CBet IP %")
                if hero_is_pfr and hero_ip and reached_flop and checked_to_hero and hero_bet_flop:
                    self.stats["Flop CBet IP %"]["attempts"] += 1
                
            if hero_is_3better and hero_ip and reached_flop and checked_to_hero:
                self.stats["Flop CBet 3BetPot IP"]["opportunities"] += 1
                self._collect_hand("Flop CBet 3BetPot IP")
                if hero_is_3better and hero_ip and reached_flop and checked_to_hero and hero_bet_flop:
                    self.stats["Flop CBet 3BetPot IP"]["attempts"] += 1
                
            if hero_is_pfr and hero_oop and reached_flop and flop_actions and flop_actions[0][0] == hero_name:
                self.stats["Flop Cbet OOP%"]["opportunities"] += 1
                self._collect_hand("Flop Cbet OOP%")
                if hero_is_pfr and hero_oop and reached_flop and flop_actions and flop_actions[0][0] == hero_name and hero_bet_flop:
                    self.stats["Flop Cbet OOP%"]["attempts"] += 1
                    
        # ============== VS CBET GROUP ==============
        # Stats in exact order as specified
        
        # Check if villain (PFR) made a CBet
        villain_cbet = False
        pfr_name = None
        
        # Find who was the PFR (any raiser that's not Hero) - for ALL hands
        for player, action, _ in preflop_actions:
            if action == 'raise' and player != hero_name:
                pfr_name = player
                break
        
        # Check if PFR bet on flop (only if reached flop)
        if reached_flop and pfr_name and flop_actions:
            villain_cbet = any(player == pfr_name and action == 'bet' 
                             for player, action, _ in flop_actions)
        
        # Hero facing a CBet scenario - check actions for ALL hands
        hero_folded = False
        hero_raised = False
        
        # Only check actions if reached flop and villain actually Cbet
        if reached_flop and villain_cbet and flop_actions:
            hero_folded = any(player == hero_name and action == 'fold' 
                            for player, action, _ in flop_actions)
            hero_raised = any(player == hero_name and action == 'raise' 
                            for player, action, _ in flop_actions)
        
        if not hero_is_pfr and hero_ip and reached_flop and villain_cbet:
            self.stats["Flop fold vs Cbet IP"]["opportunities"] += 1
            self._collect_hand("Flop fold vs Cbet IP")
            if not hero_is_pfr and hero_ip and reached_flop and villain_cbet and hero_folded:
                self.stats["Flop fold vs Cbet IP"]["attempts"] += 1
            
        if not hero_is_pfr and hero_ip and reached_flop and villain_cbet:
            self.stats["Flop raise Cbet IP"]["opportunities"] += 1
            self._collect_hand("Flop raise Cbet IP")
            if not hero_is_pfr and hero_ip and reached_flop and villain_cbet and hero_raised:
                self.stats["Flop raise Cbet IP"]["attempts"] += 1
            
        if not hero_is_pfr and hero_oop and reached_flop and villain_cbet:
            self.stats["Flop raise Cbet OOP"]["opportunities"] += 1
            self._collect_hand("Flop raise Cbet OOP")
            if not hero_is_pfr and hero_oop and reached_flop and villain_cbet and hero_raised:
                self.stats["Flop raise Cbet OOP"]["attempts"] += 1
                    
        # 7. Fold vs Check Raise
        # Count opportunity for ALL hands, check-raise only if reached flop
        hero_check_raised = False
        
        # Only check for check-raise if reached flop
        if reached_flop and flop_actions:
            for i, (player, action, _) in enumerate(flop_actions):
                if player == hero_name and action == 'check':
                    # Look for subsequent bet and Hero's raise
                    for j in range(i + 1, len(flop_actions)):
                        if flop_actions[j][1] == 'bet':
                            # Someone bet after Hero checked
                            for k in range(j + 1, len(flop_actions)):
                                if flop_actions[k][0] == hero_name and flop_actions[k][1] == 'raise':
                                    hero_check_raised = True
                                    break
                        if hero_check_raised:
                            break
        
        self.stats["Fold vs Check Raise"]["opportunities"] += 1
        self._collect_hand("Fold vs Check Raise")
        # Only count attempts if actually check-raised and villain folded
        if reached_flop and hero_check_raised and flop_actions:
            villain_folded = any(action == 'fold' for _, action, _ in flop_actions)
            if villain_folded:
                self.stats["Fold vs Check Raise"]["attempts"] += 1
        
        # ============== VS SKIPPED CBET GROUP ==============
        # Stats in exact order as specified
        
        self.stats["Flop bet vs missed Cbet SRP"]["opportunities"] += 1
        self._collect_hand("Flop bet vs missed Cbet SRP")
        
        # Check attempts only if conditions are met
        if not hero_is_pfr and hero_ip and reached_flop and flop_actions:
            # Check if villain (PFR) skipped CBet (checked instead of betting)
            pfr_name = None
            for player, action, _ in preflop_actions:
                if action == 'raise' and player != hero_name:
                    pfr_name = player
                    break
            
            if pfr_name:
                pfr_checked = any(player == pfr_name and action == 'check' 
                                for player, action, _ in flop_actions)
                if pfr_checked:
                    # Check if Hero bet after PFR checked
                    hero_bet = any(player == hero_name and action == 'bet' 
                                 for player, action, _ in flop_actions)
                    if hero_bet:
                        self.stats["Flop bet vs missed Cbet SRP"]["attempts"] += 1
                
        # ============== TURN PLAY GROUP ==============
        # Stats in exact order as specified
        
        # Extract turn actions
        turn_actions = parser.extract_turn_actions(hand_text)
        has_turn = len(turn_actions) > 0 or '*** TURN ***' in hand_text or '** Dealing turn **' in hand_text
        
        # Process turn data for ALL hands
        hero_bet_turn = False
        hero_folded_turn = False
        hero_first_turn = False
        villain_cbet_turn = False
        checked_to_hero_turn = False
        
        # Only check turn actions if reached turn
        if reached_flop and has_turn:
            # Check Hero actions on turn
            hero_bet_turn = any(player == hero_name and action == 'bet' 
                               for player, action, _ in turn_actions)
            hero_folded_turn = any(player == hero_name and action == 'fold' 
                                  for player, action, _ in turn_actions)
            
            # Check if Hero was first to act on turn (OOP)
            hero_first_turn = turn_actions and turn_actions[0][0] == hero_name
            
            # Find PFR for villain cbet detection
            pfr_name = None
            for player, action, _ in preflop_actions:
                if action == 'raise' and player != hero_name:
                    pfr_name = player
                    break
            
            # Check if villain (PFR) made cbet on turn
            if pfr_name:
                villain_cbet_turn = any(player == pfr_name and action == 'bet' 
                                       for player, action, _ in turn_actions)
            
            # Check if action was checked to Hero on turn (IP cbet opportunity)
            if hero_ip:
                for i, (player, action, _) in enumerate(turn_actions):
                    if player != hero_name and action == 'check':
                        # Check if next action is Hero's
                        if i + 1 < len(turn_actions) and turn_actions[i + 1][0] == hero_name:
                            checked_to_hero_turn = True
                            break
        
        # 9. Turn CBet IP% - Count opportunities for ALL hands where Hero is PFR and IP
            if reached_flop and has_turn and checked_to_hero_turn:
                self.stats["Turn CBet IP%"]["opportunities"] += 1
                self._collect_hand("Turn CBet IP%")
                if reached_flop and has_turn and checked_to_hero_turn and hero_bet_turn:
                    self.stats["Turn CBet IP%"]["attempts"] += 1
                    
        # 10. Turn Cbet OOP% - Count opportunities for ALL hands where Hero is PFR and OOP
            if reached_flop and has_turn and hero_first_turn:
                self.stats["Turn Cbet OOP%"]["opportunities"] += 1
                self._collect_hand("Turn Cbet OOP%")
                if reached_flop and has_turn and hero_first_turn and hero_bet_turn:
                    self.stats["Turn Cbet OOP%"]["attempts"] += 1
                    
        # 11. Turn donk bet - Count opportunities for ALL hands where Hero is OOP and not PFR
            self.stats["Turn donk bet"]["opportunities"] += 1
            self._collect_hand("Turn donk bet")
            # Donk bet = Hero OOP bets first when not the PFR
            if reached_flop and has_turn and hero_bet_turn and hero_first_turn:
                self.stats["Turn donk bet"]["attempts"] += 1
                
        # 12. Turn donk bet SRP vs PFR - Count opportunities for ALL hands
            if reached_flop and has_turn and hero_bet_turn:
                self.stats["Turn donk bet SRP vs PFR"]["opportunities"] += 1
                self._collect_hand("Turn donk bet SRP vs PFR")
                if reached_flop and has_turn and hero_bet_turn and hero_first_turn:
                    self.stats["Turn donk bet SRP vs PFR"]["attempts"] += 1
                
        # 13. Turn Fold vs CBet OOP - Count opportunities for ALL hands where Hero not PFR and OOP
            if reached_flop and has_turn and villain_cbet_turn:
                self.stats["Turn Fold vs CBet OOP"]["opportunities"] += 1
                self._collect_hand("Turn Fold vs CBet OOP")
                if reached_flop and has_turn and villain_cbet_turn and hero_folded_turn:
                    self.stats["Turn Fold vs CBet OOP"]["attempts"] += 1
                
        # 14. Bet turn vs Missed Flop Cbet OOP SRP - Count opportunities for ALL hands
            self.stats["Bet turn vs Missed Flop Cbet OOP SRP"]["opportunities"] += 1
            self._collect_hand("Bet turn vs Missed Flop Cbet OOP SRP")
            # Hero must bet first (donk-like action) after villain skipped flop Cbet
            if reached_flop and has_turn and not villain_cbet and hero_bet_turn and hero_first_turn:
                self.stats["Bet turn vs Missed Flop Cbet OOP SRP"]["attempts"] += 1
                    
        # ============== RIVER PLAY GROUP ==============
        # Stats in exact order as specified
        
        # Extract river actions
        river_actions = parser.extract_river_actions(hand_text)
        has_river = len(river_actions) > 0 or '*** RIVER ***' in hand_text or '** Dealing river **' in hand_text
        
        # Check for showdown
        has_showdown = '*** SHOW DOWN ***' in hand_text or '*** SHOWDOWN ***' in hand_text
        
        # Check Hero river actions
        hero_bet_river = any(player == hero_name and action == 'bet' 
                            for player, action, _ in river_actions)
        hero_raise_river = any(player == hero_name and action == 'raise' 
                              for player, action, _ in river_actions)
        hero_is_aggressive_river = hero_bet_river or hero_raise_river
        
        if reached_flop:
            self.stats["WTSD%"]["opportunities"] += 1
            self._collect_hand("WTSD%")
            if reached_flop and has_showdown:
                self.stats["WTSD%"]["attempts"] += 1
            
        if reached_flop:
            self.stats["W$SD%"]["opportunities"] += 1
            self._collect_hand("W$SD%")
            if reached_flop and has_showdown:
                # Check if Hero won at showdown
                hero_won = f'{hero_name} wins' in hand_text or f'{hero_name} collected' in hand_text
                if hero_won:
                    self.stats["W$SD%"]["attempts"] += 1
                
        if reached_flop:
            self.stats["W$WSF Rating"]["opportunities"] += 1
            self._collect_hand("W$WSF Rating")
            if reached_flop and (f'{hero_name} wins' in hand_text or f'{hero_name} collected' in hand_text):
                self.stats["W$WSF Rating"]["attempts"] += 1
        
        # River stats - Process for ALL hands
        if True:  # Process all hands for river stat opportunities
            if reached_flop and has_river:
                self.stats["River Agg %"]["opportunities"] += 1
                self._collect_hand("River Agg %")
                if reached_flop and has_river and hero_is_aggressive_river:
                    self.stats["River Agg %"]["attempts"] += 1
            
            # 19. River bet - Single Rsd Pot - Count opportunities for ALL hands in SRP
                if reached_flop and has_river:
                    self.stats["River bet - Single Rsd Pot"]["opportunities"] += 1
                    self._collect_hand("River bet - Single Rsd Pot")
                    if reached_flop and has_river and hero_bet_river:
                        self.stats["River bet - Single Rsd Pot"]["attempts"] += 1
            
            if reached_flop and has_river and hero_bet_river:
                self.stats["W$SD% B River"]["opportunities"] += 1
                self._collect_hand("W$SD% B River")
                if reached_flop and has_river and hero_bet_river and has_showdown:
                    hero_won = f'{hero_name} wins' in hand_text or f'{hero_name} collected' in hand_text
                    if hero_won:
                        self.stats["W$SD% B River"]["attempts"] += 1
    
    def _is_hero_pfr(self, hand_text: str) -> bool:
        """Verifica se Hero é o Pre-Flop Raiser"""
        preflop = self._extract_street(hand_text, "preflop")
        if not preflop:
            return False
        
        # Track who raised first
        first_raiser = None
        for line in preflop:
            # Hero raises or re-raises
            if "Hero raises" in line or "Hero re-raises" in line or "Hero: raises" in line:
                if first_raiser is None:
                    first_raiser = "Hero"
                return True  # Hero raised/re-raised at some point
            # Check if someone else raised first  
            elif ": raises" in line and "Hero" not in line and first_raiser is None:
                first_raiser = "Other"
        
        return first_raiser == "Hero"
    
    def _is_hero_ip_on_flop(self, hand_text: str) -> bool:
        """Determina se Hero está IP no flop (simplificado)"""
        # Simplificação: se Hero está no BTN, CO ou tem posição após o PFR
        if "Hero (BTN)" in hand_text or "Hero (CO)" in hand_text:
            return True
        # Mais lógica seria necessária para determinar corretamente
        return False
    
    def _is_3bet_pot(self, hand_text: str) -> bool:
        """Verifica se é um 3bet pot"""
        preflop = self._extract_street(hand_text, "preflop")
        raise_count = 0
        for line in preflop:
            if "raises" in line or "re-raises" in line:
                raise_count += 1
        return raise_count >= 2
    
    def _hero_cbets_flop(self, hand_text: str) -> bool:
        """Verifica se Hero faz CBet no flop"""
        flop = self._extract_street(hand_text, "flop")
        for line in flop:
            if "Hero bets" in line:
                return True
        return False
    
    def _villain_cbets_flop(self, hand_text: str) -> bool:
        """Verifica se algum villain faz CBet no flop"""
        flop = self._extract_street(hand_text, "flop")
        for line in flop:
            if "bets" in line and "Hero" not in line:
                return True
        return False
    
    def _hero_folds_flop(self, hand_text: str) -> bool:
        """Verifica se Hero folda no flop"""
        flop = self._extract_street(hand_text, "flop")
        for line in flop:
            if "Hero folds" in line:
                return True
        return False
    
    def _hero_raises_flop(self, hand_text: str) -> bool:
        """Verifica se Hero raisa no flop"""
        flop = self._extract_street(hand_text, "flop")
        for line in flop:
            if "Hero raises" in line:
                return True
        return False
    
    def _hero_check_raises_flop(self, hand_text: str) -> bool:
        """Verifica se Hero faz check-raise no flop"""
        flop = self._extract_street(hand_text, "flop")
        hero_checked = False
        for line in flop:
            if "Hero checks" in line:
                hero_checked = True
            elif hero_checked and "Hero raises" in line:
                return True
        return False
    
    def _villain_folds_to_check_raise(self, hand_text: str) -> bool:
        """Verifica se villain folda para check-raise"""
        # Simplificação - seria necessária lógica mais complexa
        return False
    
    def _hero_bets_flop(self, hand_text: str) -> bool:
        """Verifica se Hero aposta no flop"""
        return self._hero_cbets_flop(hand_text)
    
    def _hero_cbets_turn(self, hand_text: str) -> bool:
        """Verifica se Hero faz CBet no turn"""
        turn = self._extract_street(hand_text, "turn")
        for line in turn:
            if "Hero bets" in line:
                return True
        return False
    
    def _villain_cbets_turn(self, hand_text: str) -> bool:
        """Verifica se villain faz CBet no turn"""
        turn = self._extract_street(hand_text, "turn")
        for line in turn:
            if "bets" in line and "Hero" not in line:
                return True
        return False
    
    def _hero_donk_bets_turn(self, hand_text: str) -> bool:
        """Verifica se Hero faz donk bet no turn"""
        turn = self._extract_street(hand_text, "turn")
        # Primeiro a apostar no turn quando não é PFR
        for line in turn:
            if "Hero bets" in line:
                # Verificar se é a primeira ação
                return True
        return False
    
    def _hero_bets_turn(self, hand_text: str) -> bool:
        """Verifica se Hero aposta no turn"""
        return self._hero_cbets_turn(hand_text)
    
    def _hero_folds_turn(self, hand_text: str) -> bool:
        """Verifica se Hero folda no turn"""
        turn = self._extract_street(hand_text, "turn")
        for line in turn:
            if "Hero folds" in line:
                return True
        return False
    
    def _hero_is_aggressive_river(self, hand_text: str) -> bool:
        """Verifica se Hero é agressivo no river"""
        river = self._extract_street(hand_text, "river")
        for line in river:
            if "Hero bets" in line or "Hero raises" in line:
                return True
        return False
    
    def _hero_bets_river(self, hand_text: str) -> bool:
        """Verifica se Hero aposta no river"""
        river = self._extract_street(hand_text, "river")
        for line in river:
            if "Hero bets" in line:
                return True
        return False
    
    def _hero_wins_showdown(self, hand_text: str) -> bool:
        """Verifica se Hero ganha no showdown"""
        # Check if Hero wins at showdown specifically
        if "*** SHOW DOWN ***" in hand_text:
            lines = hand_text.split('\n')
            for i, line in enumerate(lines):
                if "*** SHOW DOWN ***" in line:
                    # Look for Hero winning after showdown
                    for j in range(i, min(i+10, len(lines))):
                        if "Hero collected" in lines[j] or "Hero wins" in lines[j]:
                            return True
        return False
    
    def _hero_wins_pot(self, hand_text: str) -> bool:
        """Verifica se Hero ganha o pot (com ou sem showdown)"""
        # Hero wins if they collect money
        return "Hero collected" in hand_text or "Hero wins" in hand_text
    
    def _has_preflop_allin(self, hand_text: str) -> bool:
        """Verifica se houve all-in no pré-flop"""
        preflop_lines = self._extract_street(hand_text, "preflop")
        
        # Procura por qualquer all-in antes do flop
        for line in preflop_lines:
            line_lower = line.lower()
            # Verifica diferentes formatos de all-in
            if "all-in" in line_lower or "all in" in line_lower or "allin" in line_lower:
                return True
            # Também verifica se alguém apostou tudo (formato alternativo)
            if "and is all-in" in line_lower or "and is all in" in line_lower:
                return True
        
        return False
    
    def _extract_street(self, hand_text: str, street: str) -> list:
        """Extrai as linhas de ação de uma street específica"""
        lines = hand_text.split('\n')
        
        if street == "preflop":
            start_marker = "*** HOLE CARDS ***"
            end_marker = "*** FLOP ***"
        elif street == "flop":
            start_marker = "*** FLOP ***"
            end_marker = "*** TURN ***"
        elif street == "turn":
            start_marker = "*** TURN ***"
            end_marker = "*** RIVER ***"
        elif street == "river":
            start_marker = "*** RIVER ***"
            end_marker = "*** SHOW DOWN ***"
        else:
            return []
        
        street_lines = []
        in_street = False
        
        for line in lines:
            if start_marker in line:
                in_street = True
                continue
            elif end_marker in line or "*** SUMMARY ***" in line:
                break
            elif in_street:
                street_lines.append(line)
        
        return street_lines
    
    def _extract_hero_name(self, hand_text: str) -> Optional[str]:
        """Extract hero name from 'Dealt to' line."""
        import re
        dealt_match = re.search(r'Dealt\s+to\s+([^\[]+)', hand_text)
        if dealt_match:
            return dealt_match.group(1).strip()
        return None
    
    def _normalize_hero_name(self, hand_text: str, hero_name: str) -> str:
        """Replace actual hero name with 'Hero' for consistent parsing."""
        # Replace all instances of hero name with "Hero"
        # Be careful with word boundaries to avoid partial replacements
        import re
        # Escape special regex characters in the name
        escaped_name = re.escape(hero_name)
        # Replace hero name with "Hero" using word boundaries
        pattern = r'\b' + escaped_name + r'\b'
        return re.sub(pattern, 'Hero', hand_text)
    
    def calculate_stat_score(self, stat_name: str, percentage: float) -> float:
        """
        Calcula o score individual de uma stat usando sistema de steps (igual ao preflop).
        
        Sistema de scoring:
        - Primeiro step (ideal até ideal+osc) = 100 pontos
        - Cada step adicional = -10 pontos
        - Score vai de 100 até 0
        
        Exemplo: Turn CBet IP% ideal=60, osc_up=12 (20% de 60)
        - 60.0 a 72.0 = 100 pontos (primeiro step para cima)
        - 72.1 a 84.0 = 90 pontos (segundo step)
        - 84.1 a 96.0 = 80 pontos (terceiro step)
        """
        import math
        
        config = SCORING_CONFIG.get('postflop_all', {}).get(stat_name)
        if not config or config.get("ideal") is None:
            return 0
        
        ideal = config["ideal"]
        osc_down = config["oscillation_down"]
        osc_up = config["oscillation_up"]
        
        # Diferença entre o valor atual e o ideal
        diff = percentage - ideal
        
        if diff > 0:
            # Acima do ideal - usa oscillation_up
            if osc_up == 0:
                return 0  # Sem tolerância para cima
            # Calcula em qual step estamos usando arredondamento
            # floor(diff / osc) nos dá o step base
            # Se diff/osc é quase um inteiro, ajustamos para limite superior inclusivo
            mult = diff / osc_up
            steps = int(math.floor(mult))
            # Se mult está muito perto de um inteiro > 0, fica no step anterior
            frac = mult - math.floor(mult)
            if frac < 0.0001 and steps > 0:  # Quase múltiplo exato
                steps -= 1
            penalty = steps * 10
        elif diff < 0:
            # Abaixo do ideal - usa oscillation_down
            if osc_down == 0:
                return 0  # Sem tolerância para baixo
            # Mesma lógica para baixo
            abs_diff = abs(diff)
            mult = abs_diff / osc_down
            steps = int(math.floor(mult))
            # Se mult está muito perto de um inteiro > 0, fica no step anterior
            frac = mult - math.floor(mult)
            if frac < 0.0001 and steps > 0:
                steps -= 1
            penalty = steps * 10
        else:
            # Exatamente no ideal
            return 100
        
        # Score final: 100 - penalty, clamped entre 0 e 100
        return max(0, min(100, 100 - penalty))
    
    def calculate_group_scores(self) -> Dict[str, float]:
        """Calcula os scores por grupo"""
        groups = {
            "Flop Cbet": ["Flop CBet IP %", "Flop CBet 3BetPot IP", "Flop Cbet OOP%"],
            "Vs Cbet": ["Flop fold vs Cbet IP", "Flop raise Cbet IP", "Flop raise Cbet OOP", "Fold vs Check Raise"],
            "vs Skipped Cbet": ["Flop bet vs missed Cbet SRP"],
            "Turn Play": ["Turn CBet IP%", "Turn Cbet OOP%", "Turn donk bet", "Turn donk bet SRP vs PFR", 
                         "Bet turn vs Missed Flop Cbet OOP SRP", "Turn Fold vs CBet OOP"],
            "River play": ["WTSD%", "W$SD%", "W$WSF Rating", "River Agg %", "River bet - Single Rsd Pot", "W$SD% B River"]
        }
        
        config = SCORING_CONFIG.get('postflop_all', {})
        group_scores = {}
        
        for group_name, stat_names in groups.items():
            total_score = 0
            total_weight = 0
            
            for stat_name in stat_names:
                if stat_name in self.stats and stat_name in config:
                    stat_config = config[stat_name]
                    weight = stat_config["weight"]
                    
                    if weight > 0:  # Ignora stats com peso 0
                        stat_data = self.stats[stat_name]
                        opportunities = stat_data["opportunities"]
                        attempts = stat_data["attempts"]
                        
                        if opportunities > 0:
                            percentage = (attempts / opportunities) * 100
                            score = self.calculate_stat_score(stat_name, percentage)
                            total_score += score * weight
                            total_weight += weight
            
            # Calcula score médio ponderado do grupo
            if total_weight > 0:
                group_scores[group_name] = round(total_score / total_weight, 1)
            else:
                group_scores[group_name] = 0
        
        return group_scores
    
    def get_hands_count(self) -> int:
        """Retorna número de mãos processadas que chegaram ao pós-flop"""
        return self.hands_processed
    
    def get_stats_summary(self) -> Dict[str, Dict[str, Any]]:
        """
        Retorna resumo das estatísticas calculadas com scores.
        """
        result = {}
        
        # Lista de todas as 20 stats na ordem exata
        all_stats = [
            "Flop CBet IP %", "Flop CBet 3BetPot IP", "Flop Cbet OOP%",
            "Flop fold vs Cbet IP", "Flop raise Cbet IP", "Flop raise Cbet OOP", "Fold vs Check Raise",
            "Flop bet vs missed Cbet SRP",
            "Turn CBet IP%", "Turn Cbet OOP%", "Turn donk bet", "Turn donk bet SRP vs PFR",
            "Bet turn vs Missed Flop Cbet OOP SRP", "Turn Fold vs CBet OOP",
            "WTSD%", "W$SD%", "W$WSF Rating", "River Agg %", "River bet - Single Rsd Pot", "W$SD% B River"
        ]
        
        for stat_name in all_stats:
            if stat_name in self.stats:
                stat_data = self.stats[stat_name]
                opportunities = stat_data["opportunities"]
                attempts = stat_data["attempts"]
                
                result[stat_name] = {
                    "opportunities": opportunities,
                    "attempts": attempts
                }
                
                # Calcular percentagem se houver oportunidades
                if opportunities > 0:
                    percentage = (attempts / opportunities) * 100
                    result[stat_name]["percentage"] = round(percentage, 1)
                    result[stat_name]["score"] = self.calculate_stat_score(stat_name, percentage)
                else:
                    result[stat_name]["percentage"] = 0
                    result[stat_name]["score"] = 0
            else:
                # Stat não existe ainda, criar com zeros
                result[stat_name] = {
                    "opportunities": 0,
                    "attempts": 0,
                    "percentage": 0,
                    "score": 0
                }
        
        return result