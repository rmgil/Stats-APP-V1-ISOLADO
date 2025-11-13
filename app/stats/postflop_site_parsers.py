"""
Site-specific parsers for postflop analysis
Each poker site has different formats that need specific parsing logic
"""
import re
from typing import Optional, List, Tuple, Dict, Any
from abc import ABC, abstractmethod

class BasePostflopParser(ABC):
    """Base class for site-specific postflop parsers"""
    
    @abstractmethod
    def detect_flop(self, hand_text: str) -> bool:
        """Check if hand reached the flop"""
        pass
    
    @abstractmethod
    def detect_preflop_allin(self, hand_text: str) -> bool:
        """Check if there was an all-in before flop"""
        pass
    
    @abstractmethod
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """Extract hero's name from hand"""
        pass
    
    @abstractmethod
    def extract_flop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract flop actions as (player, action, amount) tuples"""
        pass
    
    @abstractmethod
    def extract_preflop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract preflop actions to identify PFR"""
        pass
    
    @abstractmethod
    def extract_turn_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract turn actions as (player, action, amount) tuples"""
        pass
    
    @abstractmethod
    def extract_river_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract river actions as (player, action, amount) tuples"""
        pass
    
    def identify_pfr(self, hand_text: str, hero_name: str) -> bool:
        """Check if hero was the pre-flop raiser"""
        actions = self.extract_preflop_actions(hand_text)
        for player, action, _ in actions:
            if action in ['raise', 'bet']:
                return player == hero_name
        return False
    
    def determine_position_on_flop(self, hand_text: str, hero_name: str) -> str:
        """Determine if hero is IP or OOP on the flop
        Returns: 'IP', 'OOP', or 'UNKNOWN'
        """
        flop_actions = self.extract_flop_actions(hand_text)
        if not flop_actions:
            return 'UNKNOWN'
        
        # Find all unique actors on the flop (excluding folds)
        actors = []
        for player, action, _ in flop_actions:
            if action != 'fold' and player not in actors:
                actors.append(player)
        
        if hero_name not in actors:
            return 'UNKNOWN'
        
        # Hero is IP if they act last, OOP if they act first
        hero_position = actors.index(hero_name)
        if hero_position == 0:
            return 'OOP'  # First to act
        elif hero_position == len(actors) - 1:
            return 'IP'  # Last to act
        else:
            return 'MIDDLE'  # Multiple players, hero in middle


class PokerStarsParser(BasePostflopParser):
    """Parser for PokerStars hands"""
    
    def detect_flop(self, hand_text: str) -> bool:
        """PokerStars format: *** FLOP *** [Ks 2c 4h]"""
        return '*** FLOP ***' in hand_text
    
    def detect_preflop_allin(self, hand_text: str) -> bool:
        """Check for all-ins before flop"""
        lines = hand_text.split('\n')
        found_hole_cards = False
        
        for line in lines:
            if '*** HOLE CARDS ***' in line:
                found_hole_cards = True
            elif '*** FLOP ***' in line:
                return False  # Reached flop, no preflop all-in that ended hand
            elif found_hole_cards and 'all-in' in line.lower():
                return True
        return False
    
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """PokerStars: Dealt to PlayerName [cards]"""
        match = re.search(r'Dealt to ([^\[]+)', hand_text)
        if match:
            return match.group(1).strip()
        return None
    
    def extract_flop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract flop actions from PokerStars format"""
        actions = []
        lines = hand_text.split('\n')
        in_flop = False
        
        for line in lines:
            if '*** FLOP ***' in line:
                in_flop = True
            elif '*** TURN ***' in line or '*** RIVER ***' in line or '*** SHOW DOWN ***' in line:
                break
            elif in_flop and ': ' in line:
                # PokerStars format: "Player: action amount"
                if 'checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?):\s*bets\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'calls' in line:
                    match = re.match(r'(.+?):\s*calls\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'raises' in line:
                    match = re.match(r'(.+?):\s*raises\s+([0-9.]+)\s+to\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(3))))
                elif 'folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold', None))
        
        return actions
    
    def extract_preflop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract preflop actions"""
        actions = []
        lines = hand_text.split('\n')
        in_preflop = False
        
        for line in lines:
            if '*** HOLE CARDS ***' in line:
                in_preflop = True
            elif '*** FLOP ***' in line:
                break
            elif in_preflop and ': ' in line:
                if 'raises' in line:
                    match = re.match(r'(.+?):\s*raises', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?):\s*bets', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', None))
        
        return actions
    
    def extract_turn_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract turn actions from PokerStars format"""
        actions = []
        lines = hand_text.split('\n')
        in_turn = False
        
        for line in lines:
            if '*** TURN ***' in line:
                in_turn = True
            elif '*** RIVER ***' in line or '*** SHOW DOWN ***' in line:
                break
            elif in_turn and ': ' in line:
                if 'checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?):\s*bets\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'calls' in line:
                    match = re.match(r'(.+?):\s*calls\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'raises' in line:
                    match = re.match(r'(.+?):\s*raises\s+([0-9.]+)\s+to\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(3))))
                elif 'folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold', None))
        
        return actions
    
    def extract_river_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract river actions from PokerStars format"""
        actions = []
        lines = hand_text.split('\n')
        in_river = False
        
        for line in lines:
            if '*** RIVER ***' in line:
                in_river = True
            elif '*** SHOW DOWN ***' in line or '*** SUMMARY ***' in line:
                break
            elif in_river and ': ' in line:
                if 'checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?):\s*bets\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'calls' in line:
                    match = re.match(r'(.+?):\s*calls\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'raises' in line:
                    match = re.match(r'(.+?):\s*raises\s+([0-9.]+)\s+to\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(3))))
                elif 'folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold', None))
        
        return actions


class GGPokerParser(BasePostflopParser):
    """Parser for GGPoker hands"""
    
    def detect_flop(self, hand_text: str) -> bool:
        """GGPoker format: *** FLOP *** [Ks 2c 4h]"""
        return '*** FLOP ***' in hand_text or '*** Flop ***' in hand_text
    
    def detect_preflop_allin(self, hand_text: str) -> bool:
        """Check for all-ins before flop"""
        lines = hand_text.split('\n')
        found_hole_cards = False
        
        for line in lines:
            if '*** HOLE CARDS ***' in line or '*** Hole Cards ***' in line:
                found_hole_cards = True
            elif '*** FLOP ***' in line or '*** Flop ***' in line:
                return False
            elif found_hole_cards and 'all-in' in line.lower():
                return True
        return False
    
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """GGPoker: Dealt to PlayerName [cards]"""
        match = re.search(r'Dealt to ([^\[]+)', hand_text)
        if match:
            return match.group(1).strip()
        return None
    
    def extract_flop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract flop actions from GGPoker format"""
        actions = []
        lines = hand_text.split('\n')
        in_flop = False
        
        for line in lines:
            if '*** FLOP ***' in line or '*** Flop ***' in line:
                in_flop = True
            elif '*** TURN ***' in line or '*** Turn ***' in line or '*** RIVER ***' in line:
                break
            elif in_flop and ': ' in line:
                # GGPoker format similar to PokerStars but with $ symbols
                if 'checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?):\s*bets\s+\$?([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'calls' in line:
                    match = re.match(r'(.+?):\s*calls\s+\$?([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'raises' in line:
                    match = re.match(r'(.+?):\s*raises\s+\$?([0-9.]+)\s+to\s+\$?([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(3))))
                elif 'folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold', None))
        
        return actions
    
    def extract_preflop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract preflop actions"""
        actions = []
        lines = hand_text.split('\n')
        in_preflop = False
        
        for line in lines:
            if '*** HOLE CARDS ***' in line or '*** Hole Cards ***' in line:
                in_preflop = True
            elif '*** FLOP ***' in line or '*** Flop ***' in line:
                break
            elif in_preflop and ': ' in line:
                if 'raises' in line:
                    match = re.match(r'(.+?):\s*raises', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?):\s*bets', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', None))
        
        return actions
    
    def extract_turn_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract turn actions from GGPoker format"""
        actions = []
        lines = hand_text.split('\n')
        in_turn = False
        
        for line in lines:
            if '*** TURN ***' in line or '*** Turn ***' in line:
                in_turn = True
            elif '*** RIVER ***' in line or '*** River ***' in line:
                break
            elif in_turn and ': ' in line:
                if 'checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?):\s*bets\s+\$?([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'calls' in line:
                    match = re.match(r'(.+?):\s*calls\s+\$?([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'raises' in line:
                    match = re.match(r'(.+?):\s*raises\s+\$?([0-9.]+)\s+to\s+\$?([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(3))))
                elif 'folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold', None))
        
        return actions
    
    def extract_river_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract river actions from GGPoker format"""
        actions = []
        lines = hand_text.split('\n')
        in_river = False
        
        for line in lines:
            if '*** RIVER ***' in line or '*** River ***' in line:
                in_river = True
            elif '*** SHOW DOWN ***' in line or '*** SUMMARY ***' in line:
                break
            elif in_river and ': ' in line:
                if 'checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?):\s*bets\s+\$?([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'calls' in line:
                    match = re.match(r'(.+?):\s*calls\s+\$?([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'raises' in line:
                    match = re.match(r'(.+?):\s*raises\s+\$?([0-9.]+)\s+to\s+\$?([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(3))))
                elif 'folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold', None))
        
        return actions


class Poker888Parser(BasePostflopParser):
    """Parser for 888poker hands"""
    
    def detect_flop(self, hand_text: str) -> bool:
        """888poker format: ** Dealing flop ** : [ Kh, 7d, 2c ]"""
        return '** Dealing flop **' in hand_text or '*** FLOP ***' in hand_text
    
    def detect_preflop_allin(self, hand_text: str) -> bool:
        """Check for all-ins before flop"""
        lines = hand_text.split('\n')
        found_hole_cards = False
        
        for line in lines:
            if '** Dealing down cards **' in line or '*** HOLE CARDS ***' in line:
                found_hole_cards = True
            elif '** Dealing flop **' in line or '*** FLOP ***' in line:
                return False
            elif found_hole_cards and 'all in' in line.lower():
                return True
        return False
    
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """888poker: Dealt to PlayerName [ cards ]"""
        match = re.search(r'Dealt\s+to\s+([^\[]+)', hand_text)
        if match:
            return match.group(1).strip()
        return None
    
    def extract_flop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract flop actions from 888poker format"""
        actions = []
        lines = hand_text.split('\n')
        in_flop = False
        
        for line in lines:
            if '** Dealing flop **' in line or '*** FLOP ***' in line:
                in_flop = True
            elif '** Dealing turn **' in line or '*** TURN ***' in line:
                break
            elif in_flop and line.strip():
                # 888poker format: "Player checks" or "Player bets [100]"
                if 'checks' in line:
                    match = re.match(r'(.+?)\s+checks', line)
                    if match:
                        actions.append((match.group(1).strip(), 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?)\s+bets\s+\[([0-9.,]+)\]', line)
                    if match:
                        amount = match.group(2).replace(',', '')
                        actions.append((match.group(1).strip(), 'bet', float(amount)))
                elif 'calls' in line:
                    match = re.match(r'(.+?)\s+calls\s+\[([0-9.,]+)\]', line)
                    if match:
                        amount = match.group(2).replace(',', '')
                        actions.append((match.group(1).strip(), 'call', float(amount)))
                elif 'raises' in line:
                    match = re.match(r'(.+?)\s+raises\s+\[([0-9.,]+)\]', line)
                    if match:
                        amount = match.group(2).replace(',', '')
                        actions.append((match.group(1).strip(), 'raise', float(amount)))
                elif 'folds' in line:
                    match = re.match(r'(.+?)\s+folds', line)
                    if match:
                        actions.append((match.group(1).strip(), 'fold', None))
        
        return actions
    
    def extract_preflop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract preflop actions"""
        actions = []
        lines = hand_text.split('\n')
        in_preflop = False
        
        for line in lines:
            if '** Dealing down cards **' in line or '*** HOLE CARDS ***' in line:
                in_preflop = True
            elif '** Dealing flop **' in line or '*** FLOP ***' in line:
                break
            elif in_preflop and line.strip():
                if 'raises' in line:
                    match = re.match(r'(.+?)\s+raises', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?)\s+bets', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', None))
        
        return actions
    
    def extract_turn_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract turn actions from 888poker format"""
        actions = []
        lines = hand_text.split('\n')
        in_turn = False
        
        for line in lines:
            if '** Dealing turn **' in line or '*** TURN ***' in line:
                in_turn = True
            elif '** Dealing river **' in line or '*** RIVER ***' in line:
                break
            elif in_turn and line.strip():
                if 'checks' in line:
                    match = re.match(r'(.+?)\s+checks', line)
                    if match:
                        actions.append((match.group(1).strip(), 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?)\s+bets\s+\[([0-9.,]+)\]', line)
                    if match:
                        amount = match.group(2).replace(',', '')
                        actions.append((match.group(1).strip(), 'bet', float(amount)))
                elif 'calls' in line:
                    match = re.match(r'(.+?)\s+calls\s+\[([0-9.,]+)\]', line)
                    if match:
                        amount = match.group(2).replace(',', '')
                        actions.append((match.group(1).strip(), 'call', float(amount)))
                elif 'raises' in line:
                    match = re.match(r'(.+?)\s+raises\s+\[([0-9.,]+)\]', line)
                    if match:
                        amount = match.group(2).replace(',', '')
                        actions.append((match.group(1).strip(), 'raise', float(amount)))
                elif 'folds' in line:
                    match = re.match(r'(.+?)\s+folds', line)
                    if match:
                        actions.append((match.group(1).strip(), 'fold', None))
        
        return actions
    
    def extract_river_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract river actions from 888poker format"""
        actions = []
        lines = hand_text.split('\n')
        in_river = False
        
        for line in lines:
            if '** Dealing river **' in line or '*** RIVER ***' in line:
                in_river = True
            elif '** Summary **' in line or '*** SHOW DOWN ***' in line:
                break
            elif in_river and line.strip():
                if 'checks' in line:
                    match = re.match(r'(.+?)\s+checks', line)
                    if match:
                        actions.append((match.group(1).strip(), 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?)\s+bets\s+\[([0-9.,]+)\]', line)
                    if match:
                        amount = match.group(2).replace(',', '')
                        actions.append((match.group(1).strip(), 'bet', float(amount)))
                elif 'calls' in line:
                    match = re.match(r'(.+?)\s+calls\s+\[([0-9.,]+)\]', line)
                    if match:
                        amount = match.group(2).replace(',', '')
                        actions.append((match.group(1).strip(), 'call', float(amount)))
                elif 'raises' in line:
                    match = re.match(r'(.+?)\s+raises\s+\[([0-9.,]+)\]', line)
                    if match:
                        amount = match.group(2).replace(',', '')
                        actions.append((match.group(1).strip(), 'raise', float(amount)))
                elif 'folds' in line:
                    match = re.match(r'(.+?)\s+folds', line)
                    if match:
                        actions.append((match.group(1).strip(), 'fold', None))
        
        return actions


class WinamaxParser(BasePostflopParser):
    """Parser for Winamax hands"""
    
    def detect_flop(self, hand_text: str) -> bool:
        """Winamax format: *** FLOP *** [Ks 2c 4h]"""
        return '*** FLOP ***' in hand_text
    
    def detect_preflop_allin(self, hand_text: str) -> bool:
        """Check for all-ins before flop"""
        lines = hand_text.split('\n')
        found_preflop = False
        
        for line in lines:
            if '*** PRE-FLOP ***' in line:
                found_preflop = True
            elif '*** FLOP ***' in line:
                return False
            elif found_preflop and 'all-in' in line.lower():
                return True
        return False
    
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """Winamax: Dealt to PlayerName [cards]"""
        match = re.search(r'Dealt to ([^\[]+)', hand_text)
        if match:
            return match.group(1).strip()
        return None
    
    def extract_flop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract flop actions from Winamax format"""
        actions = []
        lines = hand_text.split('\n')
        in_flop = False
        
        for line in lines:
            if '*** FLOP ***' in line:
                in_flop = True
            elif '*** TURN ***' in line or '*** RIVER ***' in line:
                break
            elif in_flop and line.strip():
                # Winamax format: "Player checks" or "Player bets 100€"
                if 'checks' in line:
                    match = re.match(r'(.+?)\s+checks', line)
                    if match:
                        actions.append((match.group(1).strip(), 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?)\s+bets\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'calls' in line:
                    match = re.match(r'(.+?)\s+calls\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'raises' in line:
                    match = re.match(r'(.+?)\s+raises\s+([0-9.]+)\s+to\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(3))))
                elif 'folds' in line:
                    match = re.match(r'(.+?)\s+folds', line)
                    if match:
                        actions.append((match.group(1).strip(), 'fold', None))
        
        return actions
    
    def extract_preflop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract preflop actions"""
        actions = []
        lines = hand_text.split('\n')
        in_preflop = False
        
        for line in lines:
            if '*** PRE-FLOP ***' in line:
                in_preflop = True
            elif '*** FLOP ***' in line:
                break
            elif in_preflop and line.strip():
                if 'raises' in line:
                    match = re.match(r'(.+?)\s+raises', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?)\s+bets', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', None))
        
        return actions
    
    def extract_turn_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract turn actions from Winamax format"""
        actions = []
        lines = hand_text.split('\n')
        in_turn = False
        
        for line in lines:
            if '*** TURN ***' in line:
                in_turn = True
            elif '*** RIVER ***' in line or '*** SHOW DOWN ***' in line:
                break
            elif in_turn and line.strip():
                if 'checks' in line:
                    match = re.match(r'(.+?)\s+checks', line)
                    if match:
                        actions.append((match.group(1).strip(), 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?)\s+bets\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'calls' in line:
                    match = re.match(r'(.+?)\s+calls\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'raises' in line:
                    match = re.match(r'(.+?)\s+raises\s+([0-9.]+)\s+to\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(3))))
                elif 'folds' in line:
                    match = re.match(r'(.+?)\s+folds', line)
                    if match:
                        actions.append((match.group(1).strip(), 'fold', None))
        
        return actions
    
    def extract_river_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract river actions from Winamax format"""
        actions = []
        lines = hand_text.split('\n')
        in_river = False
        
        for line in lines:
            if '*** RIVER ***' in line:
                in_river = True
            elif '*** SHOW DOWN ***' in line or '*** SUMMARY ***' in line:
                break
            elif in_river and line.strip():
                if 'checks' in line:
                    match = re.match(r'(.+?)\s+checks', line)
                    if match:
                        actions.append((match.group(1).strip(), 'check', None))
                elif 'bets' in line:
                    match = re.match(r'(.+?)\s+bets\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'calls' in line:
                    match = re.match(r'(.+?)\s+calls\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'raises' in line:
                    match = re.match(r'(.+?)\s+raises\s+([0-9.]+)\s+to\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(3))))
                elif 'folds' in line:
                    match = re.match(r'(.+?)\s+folds', line)
                    if match:
                        actions.append((match.group(1).strip(), 'fold', None))
        
        return actions


class WPNParser(BasePostflopParser):
    """Parser for WPN (Winning Poker Network) hands"""
    
    def detect_flop(self, hand_text: str) -> bool:
        """WPN format: *** FLOP *** [Ks 2c 4h]"""
        return '*** FLOP ***' in hand_text
    
    def detect_preflop_allin(self, hand_text: str) -> bool:
        """Check for all-ins before flop"""
        lines = hand_text.split('\n')
        found_hole_cards = False
        
        for line in lines:
            if '*** HOLE CARDS ***' in line:
                found_hole_cards = True
            elif '*** FLOP ***' in line:
                return False
            elif found_hole_cards and 'All-in' in line:
                return True
        return False
    
    def extract_hero_name(self, hand_text: str) -> Optional[str]:
        """WPN: Dealt to PlayerName [cards]"""
        match = re.search(r'Dealt to ([^\[]+)', hand_text)
        if match:
            return match.group(1).strip()
        return None
    
    def extract_flop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract flop actions from WPN format"""
        actions = []
        lines = hand_text.split('\n')
        in_flop = False
        
        for line in lines:
            if '*** FLOP ***' in line:
                in_flop = True
            elif '*** TURN ***' in line or '*** RIVER ***' in line:
                break
            elif in_flop and ': ' in line:
                # WPN format: "Player: Checks" (capitalized actions)
                if 'Checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check', None))
                elif 'Bets' in line:
                    match = re.match(r'(.+?):\s*Bets\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'Calls' in line:
                    match = re.match(r'(.+?):\s*Calls\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'Raises' in line:
                    match = re.match(r'(.+?):\s*Raises\s+to\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(2))))
                elif 'Folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold', None))
        
        return actions
    
    def extract_preflop_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract preflop actions"""
        actions = []
        lines = hand_text.split('\n')
        in_preflop = False
        
        for line in lines:
            if '*** HOLE CARDS ***' in line:
                in_preflop = True
            elif '*** FLOP ***' in line:
                break
            elif in_preflop and ': ' in line:
                if 'Raises' in line:
                    match = re.match(r'(.+?):\s*Raises', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', None))
                elif 'Bets' in line:
                    match = re.match(r'(.+?):\s*Bets', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', None))
        
        return actions
    
    def extract_turn_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract turn actions from WPN format"""
        actions = []
        lines = hand_text.split('\n')
        in_turn = False
        
        for line in lines:
            if '*** TURN ***' in line:
                in_turn = True
            elif '*** RIVER ***' in line or '*** SHOW DOWN ***' in line:
                break
            elif in_turn and ': ' in line:
                if 'Checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check', None))
                elif 'Bets' in line:
                    match = re.match(r'(.+?):\s*Bets\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'Calls' in line:
                    match = re.match(r'(.+?):\s*Calls\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'Raises' in line:
                    match = re.match(r'(.+?):\s*Raises\s+to\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(2))))
                elif 'Folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold', None))
        
        return actions
    
    def extract_river_actions(self, hand_text: str) -> List[Tuple[str, str, Optional[float]]]:
        """Extract river actions from WPN format"""
        actions = []
        lines = hand_text.split('\n')
        in_river = False
        
        for line in lines:
            if '*** RIVER ***' in line:
                in_river = True
            elif '*** SHOW DOWN ***' in line or '*** SUMMARY ***' in line:
                break
            elif in_river and ': ' in line:
                if 'Checks' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'check', None))
                elif 'Bets' in line:
                    match = re.match(r'(.+?):\s*Bets\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'bet', float(match.group(2))))
                elif 'Calls' in line:
                    match = re.match(r'(.+?):\s*Calls\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'call', float(match.group(2))))
                elif 'Raises' in line:
                    match = re.match(r'(.+?):\s*Raises\s+to\s+([0-9.]+)', line)
                    if match:
                        actions.append((match.group(1).strip(), 'raise', float(match.group(2))))
                elif 'Folds' in line:
                    player = line.split(':')[0].strip()
                    actions.append((player, 'fold', None))
        
        return actions


def get_parser_for_hand(hand_text: str) -> Optional[BasePostflopParser]:
    """Detect which poker site the hand is from and return appropriate parser"""
    
    # Check for site-specific identifiers
    if 'PokerStars Hand #' in hand_text or 'PokerStars Zoom Hand' in hand_text:
        return PokerStarsParser()
    elif 'Poker Hand #' in hand_text and 'GGNetwork' in hand_text:
        return GGPokerParser()
    elif '#Game No :' in hand_text or '888poker Hand' in hand_text:
        return Poker888Parser()
    elif 'Winamax Poker' in hand_text:
        return WinamaxParser()
    elif 'Game Hand #' in hand_text and ('Americas Cardroom' in hand_text or 'Black Chip Poker' in hand_text):
        return WPNParser()
    
    # Fallback detection based on format patterns
    if '** Dealing flop **' in hand_text:
        return Poker888Parser()
    elif '*** PRE-FLOP ***' in hand_text:
        return WinamaxParser()
    elif ': Checks' in hand_text or ': Bets' in hand_text:  # Capitalized actions
        return WPNParser()
    elif '*** FLOP ***' in hand_text:
        # Default to PokerStars format as it's most common
        return PokerStarsParser()
    
    return None