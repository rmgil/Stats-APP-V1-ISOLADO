"""
Adapter to connect new multi-site parsers with the existing parsing system.
Keeps original hand text intact while extracting necessary information.
"""
import re
from typing import List, Dict, Optional
from pathlib import Path

from .site_parsers.site_detector import detect_poker_site, get_parser
from .schemas import Hand, Player, Action, StreetInfo
from .utils import normalize_player_name, clean_amount


class MultiSiteAdapter:
    """Adapter for multi-site parsing while preserving original text."""
    
    def __init__(self):
        self.supported_sites = [
            'ggpoker', '888poker', 'pokerstars', 
            'wpn', 'winamax', 'partypoker'
        ]
    
    def parse_mixed_file(self, text: str, file_id: str, filename: str = "") -> List[Dict]:
        """
        Parse a file that may contain hands from multiple sites.
        Returns list of parsed hands with original text preserved.
        """
        results = []
        
        # Split into potential hands (very loose splitting)
        # Try multiple patterns to catch different formats
        patterns = [
            r'(?=Poker\s+Hand\s+#)',  # GG
            r'(?=#Game No\s*:)',  # 888
            r'(?=PokerStars\s+Hand\s+#)',  # PokerStars
            r'(?=Game\s+Hand\s+#)',  # WPN
            r'(?=Winamax\s+Poker\s+-)',  # Winamax
            r'(?=\*{5}\s*Hand History)',  # Party
        ]
        
        # Try each pattern and collect all hands
        potential_hands = []
        for pattern in patterns:
            splits = re.split(pattern, text)
            for split in splits:
                if split.strip():
                    potential_hands.append(split.strip())
        
        # If no splits worked, treat entire text as one potential hand
        if not potential_hands:
            potential_hands = [text]
        
        # Process each potential hand
        for hand_text in potential_hands:
            if len(hand_text) < 50:  # Too short to be a real hand
                continue
            
            # Detect site
            site = detect_poker_site(hand_text, filename)
            if not site:
                continue
            
            # Get appropriate parser
            parser = get_parser(site)
            if not parser:
                continue
            
            # Check if it's a tournament (filter out cash games)
            if not parser.is_tournament(hand_text):
                continue
            
            try:
                # Parse hand info - pass filename if parser supports it
                if hasattr(parser, 'extract_hand_info'):
                    try:
                        # Try passing filename (for WPN, 888poker etc.)
                        info = parser.extract_hand_info(hand_text, filename)
                    except TypeError:
                        # Fallback for parsers that don't accept filename
                        info = parser.extract_hand_info(hand_text)
                else:
                    info = None
                if info:
                    # Add original text and metadata
                    info['original_text'] = hand_text
                    info['file_id'] = file_id
                    info['site'] = site
                    results.append(info)
            except Exception as e:
                # Skip hands that fail to parse
                continue
        
        return results
    
    def convert_to_hand_schema(self, parsed_info: Dict, text_offset: int = 0) -> Optional[Hand]:
        """
        Convert parsed info dict to Hand schema object for compatibility.
        """
        try:
            # Create Hand object
            hand = Hand(
                site=parsed_info.get('site', 'unknown'),
                file_id=parsed_info.get('file_id', 'unknown'),
                tournament_id=parsed_info.get('tournament_id'),
                hand_id=parsed_info.get('hand_id'),
                timestamp_utc=parsed_info.get('timestamp'),
                hero=parsed_info.get('hero'),
                blinds=parsed_info.get('blinds', {}),
                ante=parsed_info.get('ante', 0),
                max_players=parsed_info.get('table_size', 9),
                button_seat=parsed_info.get('button_seat'),
                players=[],
                streets=self._create_streets(parsed_info.get('actions', {})),
                raw_offsets={
                    'hand_start': text_offset,
                    'tournament_class': parsed_info.get('tournament_class', 'non-ko')
                }
            )
            
            # Convert players
            for p in parsed_info.get('players', []):
                player = Player(
                    seat=p.get('seat', 0),
                    name=p.get('name', 'Unknown'),
                    stack_chips=p.get('stack', 0),
                    is_hero=(p.get('name') == hand.hero)
                )
                hand.players.append(player)
            
            # Track dealt players
            hand.players_dealt_in = [p.name for p in hand.players]
            
            # Extract hole cards if hero
            if hand.hero and 'hero_cards' in parsed_info:
                # Store for later use in stats
                hand.hero_cards = parsed_info['hero_cards']
            
            return hand
            
        except Exception as e:
            return None
    
    def _create_streets(self, actions_dict: Dict) -> Dict[str, StreetInfo]:
        """Create street info objects from actions dict."""
        streets = {}
        
        for street_name in ['preflop', 'flop', 'turn', 'river']:
            street_actions = []
            
            for action_data in actions_dict.get(street_name, []):
                # Convert action dict to Action object
                action = Action(
                    actor=action_data.get('player', 'Unknown'),
                    type=self._normalize_action_type(action_data.get('action')),
                    amount=action_data.get('amount'),
                    raw_offset=0  # We don't track offsets in simplified version
                )
                street_actions.append(action)
            
            streets[street_name] = StreetInfo(
                actions=street_actions,
                board=[]  # Board cards handled separately if needed
            )
        
        return streets
    
    def _normalize_action_type(self, action: str) -> str:
        """Normalize action types to standard format."""
        if not action:
            return 'UNKNOWN'
        
        action_lower = action.lower()
        
        if 'fold' in action_lower:
            return 'FOLD'
        elif 'check' in action_lower:
            return 'CHECK'
        elif 'call' in action_lower:
            return 'CALL'
        elif 'raise' in action_lower:
            return 'RAISE'
        elif 'bet' in action_lower:
            return 'BET'
        elif 'all' in action_lower:
            return 'ALLIN'
        else:
            return action.upper()
    
    def process_directory(self, directory: Path, extensions: List[str] = ['.txt']) -> Dict[str, List[Dict]]:
        """
        Process all files in a directory, handling mixed-site files.
        Returns dict mapping filenames to lists of parsed hand info.
        """
        results = {}
        
        for ext in extensions:
            for file_path in directory.glob(f'*{ext}'):
                if file_path.is_file():
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                            text = f.read()
                        
                        hands = self.parse_mixed_file(text, file_path.name)
                        if hands:
                            results[file_path.name] = hands
                    except Exception as e:
                        continue
        
        return results