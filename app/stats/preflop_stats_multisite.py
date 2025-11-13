"""
Multi-site aware preflop statistics module.
Uses multi-site parsers to extract positions and actions from any supported poker site.
"""
import re
from typing import Dict, List, Any, Optional
from app.parse.site_parsers.site_detector import detect_poker_site, get_parser
from app.stats.position_mapping import get_position_map
from app.stats.preflop_validators import PreflopOpportunityValidator


class MultiSitePreflopExtractor:
    """Extract positions and actions from any supported poker site."""
    
    def extract_hand_id(self, hand_text: str) -> Optional[str]:
        """Extract hand ID using site-specific parser."""
        site = detect_poker_site(hand_text)
        if not site:
            return None
        
        parser = get_parser(site)
        if not parser:
            return None
        
        try:
            info = parser.extract_hand_info(hand_text)
            return info.get('hand_id')
        except:
            return None
    
    def extract_stacks_and_bb(self, hand_text: str) -> tuple[Dict[str, float], float]:
        """
        Extract player stacks and big blind using site-specific parser.
        
        Returns:
            (stacks_dict, bb_size) where stacks_dict maps player_name -> stack_size
        """
        import logging
        logger = logging.getLogger(__name__)
        
        stacks = {}
        bb_size = 0.0
        
        site = detect_poker_site(hand_text)
        if not site:
            return (stacks, bb_size)
        
        parser = get_parser(site)
        if not parser:
            return (stacks, bb_size)
        
        try:
            # Parse hand info
            info = parser.extract_hand_info(hand_text)
            if not info:
                return (stacks, bb_size)
            
            # Get hero name for proper mapping
            hero_name = info.get('hero')
            
            # Extract stacks from players
            players = info.get('players', [])
            for player_data in players:
                player_name = player_data.get('name')
                stack = player_data.get('stack', 0)
                
                if player_name and stack > 0:
                    # Map hero to "Hero" for consistency
                    if hero_name and player_name == hero_name:
                        stacks['Hero'] = stack
                    else:
                        stacks[player_name] = stack
            
            # Extract big blind size
            blinds = info.get('blinds', {})
            bb_size = blinds.get('bb', 0.0)
            
            return (stacks, bb_size)
            
        except Exception as e:
            logger.warning(f"[MULTISITE] Error extracting stacks for {site}: {e}")
            return (stacks, bb_size)
    
    def extract_positions(self, hand_text: str) -> Dict[str, str]:
        """Extract player positions using site-specific parser."""
        import logging
        logger = logging.getLogger(__name__)
        
        positions = {}
        
        site = detect_poker_site(hand_text)
        if not site:
            return positions
        
        parser = get_parser(site)
        if not parser:
            return positions
        
        try:
            # Parse hand info
            info = parser.extract_hand_info(hand_text)
            if not info:
                return positions
            
            # Get players and button
            players = info.get('players', [])
            button_seat = info.get('button_seat')
            hero_name = info.get('hero')
            
            # Debug logging for PKO hands
            if 'pko' in hand_text.lower() or 'bounty' in hand_text.lower():
                logger.info(f"[PKO DEBUG] Site: {site}, Hero: {hero_name}, Button: {button_seat}, Players count: {len(players)}")
                if players:
                    logger.info(f"[PKO DEBUG] Players: {[(p['seat'], p['name'], p.get('stack', 'NO_STACK')) for p in players[:3]]}...")
                logger.info(f"[PKO DEBUG] Hand info keys: {list(info.keys())}")
            
            if not players or button_seat is None:
                if 'pko' in hand_text.lower() or 'bounty' in hand_text.lower():
                    logger.warning(f"[PKO DEBUG] CRITICAL: Missing players or button for {site} PKO hand - players={len(players) if players else 'None'}, button={button_seat}")
                return positions
            
            # Create a seat-to-player mapping
            seat_map = {p['seat']: p for p in players}
            
            # Find max seat number (typically 9 for full ring, 6 for 6-max)
            max_seat = max(p['seat'] for p in players)
            # Assume 9-max if max_seat > 6, else 6-max
            table_size = 9 if max_seat > 6 else 6
            
            # CRITICAL FIX: Build proper circular order starting from button
            # Walk clockwise from button seat, collecting occupied seats only
            ordered_players = []
            current_seat = button_seat
            
            for _ in range(table_size):
                if current_seat in seat_map:
                    ordered_players.append(seat_map[current_seat])
                    if len(ordered_players) == len(players):
                        break  # Found all players
                # Move to next seat clockwise (with wraparound)
                current_seat = (current_seat % table_size) + 1
            
            # Now ordered_players has players in true circular order from button
            num_players = len(ordered_players)
            
            # Debug logging for PKO ordered players
            if 'pko' in hand_text.lower() or 'bounty' in hand_text.lower():
                logger.info(f"[PKO DEBUG] Ordered players count: {num_players}, Table size: {table_size}")
                if ordered_players:
                    logger.info(f"[PKO DEBUG] First 3 ordered: {[(p['seat'], p['name']) for p in ordered_players[:3]]}")
            
            # Use centralized position mapping (GG Poker standard)
            position_map = get_position_map(num_players)
            if not position_map:
                if 'pko' in hand_text.lower() or 'bounty' in hand_text.lower():
                    logger.warning(f"[PKO DEBUG] No position map for {num_players} players")
                return positions  # Unsupported player count
            
            for i, p in enumerate(ordered_players):
                # Now i directly corresponds to position relative to button
                # Map player name (use Hero for hero player)
                player_name = p['name']
                if hero_name and player_name == hero_name:
                    player_name = 'Hero'
                    # Debug log for Hero mapping in PKO
                    if 'pko' in hand_text.lower() or 'bounty' in hand_text.lower():
                        logger.info(f"[MULTISITE DEBUG] Mapped '{hero_name}' to 'Hero' at position {position_map.get(i)}")
                
                positions[player_name] = position_map.get(i, "Unknown")
            
            # Final debug log for PKO
            if 'pko' in hand_text.lower() or 'bounty' in hand_text.lower():
                if positions:
                    logger.info(f"[PKO DEBUG] Final positions for {site}: {positions}")
                    if 'Hero' not in positions:
                        logger.warning(f"[PKO DEBUG] ⚠️ Hero NOT in final positions for {site} PKO!")
                else:
                    logger.error(f"[PKO DEBUG] ERROR: No positions generated for {site} PKO hand!")
            
            return positions
            
        except Exception as e:
            if 'pko' in hand_text.lower() or 'bounty' in hand_text.lower():
                logger.error(f"[MULTISITE DEBUG] Error extracting positions for PKO: {e}")
            return positions
    
    def extract_preflop_actions(self, hand_text: str) -> List[Dict[str, Any]]:
        """
        Extract preflop actions using site-specific parser.
        Uses PreflopOpportunityValidator to normalize actions and fix all-in detection.
        """
        import logging
        logger = logging.getLogger(__name__)
        
        actions = []
        
        site = detect_poker_site(hand_text)
        if not site:
            return actions
        
        parser = get_parser(site)
        if not parser:
            return actions
        
        # Debug logging for PKO
        is_pko = 'pko' in hand_text.lower() or 'bounty' in hand_text.lower()
        if is_pko:
            logger.info(f"[PKO DEBUG] Extracting preflop actions for {site} PKO hand")
        
        try:
            # Parse hand info
            info = parser.extract_hand_info(hand_text)
            if not info:
                return actions
            
            # Get hero name for mapping
            hero_name = info.get('hero')
            
            # Get preflop actions
            preflop_actions = info.get('actions', {}).get('preflop', [])
            
            # Create validator for action normalization
            validator = PreflopOpportunityValidator()
            
            for action_data in preflop_actions:
                player = action_data.get('player', 'Unknown')
                
                # Map hero name to "Hero"
                if hero_name and player == hero_name:
                    player = 'Hero'
                
                action_type = action_data.get('action', '').lower()
                
                # Skip blind posts
                if 'post' in action_type or 'blind' in action_type or 'ante' in action_type:
                    continue
                
                # Check for all-in: Need to check ORIGINAL raw action text from parser
                # because parser may return action='raise' for "raises 1000 and is all-in"
                raw_action_text = action_data.get('raw_action', '')  # fallback if parser provides this
                is_allin = (
                    action_data.get('all_in', False) or 
                    action_data.get('is_allin', False) or
                    'all-in' in action_type or 
                    'allin' in action_type or
                    'all-in' in raw_action_text.lower() or
                    'allin' in raw_action_text.lower()
                )
                
                # Build raw action dict
                raw_action = {
                    "player": player,
                    "action": action_type,
                    "is_allin": is_allin
                }
                
                # CRITICAL FIX: Use validator to normalize action
                # This fixes the bug where standalone 'all-in' was not detected as is_raise
                normalized_action = validator.normalize_action(raw_action)
                
                actions.append(normalized_action)
            
            return actions
            
        except Exception as e:
            return actions


# Patch the existing PreflopStats class to use multi-site extractor
def patch_preflop_stats():
    """Monkey-patch the existing PreflopStats class to support multi-site parsing."""
    from app.stats.preflop_stats import PreflopStats
    
    # Create multi-site extractor
    extractor = MultiSitePreflopExtractor()
    
    # Store original methods
    original_extract_hand_id = PreflopStats._extract_hand_id
    original_extract_positions = PreflopStats._extract_positions
    original_extract_preflop_actions = PreflopStats._extract_preflop_actions
    original_extract_stacks_and_bb = PreflopStats._extract_stacks_and_bb
    
    def new_extract_hand_id(self, hand_text: str) -> Optional[str]:
        """Try multi-site parser first, fall back to original."""
        result = extractor.extract_hand_id(hand_text)
        if result:
            return result
        # Fall back to original method for backward compatibility
        return original_extract_hand_id(self, hand_text)
    
    def new_extract_positions(self, hand_text: str) -> Dict[str, str]:
        """Try multi-site parser first, fall back to original."""
        result = extractor.extract_positions(hand_text)
        if result:
            return result
        # Fall back to original method
        return original_extract_positions(self, hand_text)
    
    def new_extract_preflop_actions(self, hand_text: str) -> List[Dict[str, Any]]:
        """Try multi-site parser first, fall back to original."""
        result = extractor.extract_preflop_actions(hand_text)
        if result:
            return result
        # Fall back to original method
        return original_extract_preflop_actions(self, hand_text)
    
    def new_extract_stacks_and_bb(self, hand_text: str) -> tuple[Dict[str, float], float]:
        """Try multi-site parser first, fall back to original."""
        stacks, bb = extractor.extract_stacks_and_bb(hand_text)
        # Only fall back if we got nothing from multisite parser
        if not stacks or bb == 0:
            return original_extract_stacks_and_bb(self, hand_text)
        return (stacks, bb)
    
    # Replace methods
    PreflopStats._extract_hand_id = new_extract_hand_id
    PreflopStats._extract_positions = new_extract_positions
    PreflopStats._extract_preflop_actions = new_extract_preflop_actions
    PreflopStats._extract_stacks_and_bb = new_extract_stacks_and_bb