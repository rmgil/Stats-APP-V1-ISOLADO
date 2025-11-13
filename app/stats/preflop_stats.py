"""
Pre-flop statistics calculation module
Calculates all the required pre-flop stats for poker hands
"""
from typing import Dict, List, Any, Optional
import re
import logging
from app.stats.position_mapping import get_position_map, get_position_category, get_rfi_stat_for_position
from app.stats.preflop_validators import PreflopOpportunityValidator

logger = logging.getLogger(__name__)

class PreflopStats:
    """Calculate pre-flop statistics from parsed hands"""
    
    def __init__(self, hand_collector=None):
        self.stats = self._initialize_stats()
        self.hand_collector = hand_collector
        self.current_hand_text = None
        self.current_hand_id = None
        # Create centralized validator instance
        self.validator = PreflopOpportunityValidator()
    
    def _initialize_stats(self) -> Dict[str, Dict[str, int]]:
        """Initialize all stat counters"""
        return {
            # RFI (Raise First In)
            "Early RFI": {"opportunities": 0, "attempts": 0},
            "Middle RFI": {"opportunities": 0, "attempts": 0},
            "CO Steal": {"opportunities": 0, "attempts": 0},
            "BTN Steal": {"opportunities": 0, "attempts": 0},
            
            # BvB (Battle of the Blinds)
            "SB UO VPIP": {"opportunities": 0, "attempts": 0},
            "BB fold vs SB steal": {"opportunities": 0, "attempts": 0},
            "BB raise vs SB limp UOP": {"opportunities": 0, "attempts": 0},
            "SB Steal": {"opportunities": 0, "attempts": 0},
            
            
            # Ranges de CC/3Bet IP
            "EP 3bet": {"opportunities": 0, "attempts": 0},
            "EP Cold Call": {"opportunities": 0, "attempts": 0},
            "EP VPIP": {"opportunities": 0, "attempts": 0},
            "MP 3bet": {"opportunities": 0, "attempts": 0},
            "MP Cold Call": {"opportunities": 0, "attempts": 0},
            "MP VPIP": {"opportunities": 0, "attempts": 0},
            "CO 3bet": {"opportunities": 0, "attempts": 0},
            "CO Cold Call": {"opportunities": 0, "attempts": 0},
            "CO VPIP": {"opportunities": 0, "attempts": 0},
            "BTN 3bet": {"opportunities": 0, "attempts": 0},
            "BTN Cold Call": {"opportunities": 0, "attempts": 0},
            "BTN VPIP": {"opportunities": 0, "attempts": 0},
            "BTN fold to CO steal": {"opportunities": 0, "attempts": 0},
            
            # vs 3bet
            "Fold to 3bet IP": {"opportunities": 0, "attempts": 0},
            "Fold to 3bet OOP": {"opportunities": 0, "attempts": 0},
            "Fold to 3bet": {"opportunities": 0, "attempts": 0},
            
            # Squeeze
            "Squeeze": {"opportunities": 0, "attempts": 0},
            "Squeeze vs BTN Raiser": {"opportunities": 0, "attempts": 0},
            
            # Defesa da BB
            "BB fold vs CO steal": {"opportunities": 0, "attempts": 0},
            "BB fold vs BTN steal": {"opportunities": 0, "attempts": 0},
            "BB fold vs SB steal": {"opportunities": 0, "attempts": 0},
            "BB resteal vs BTN steal": {"opportunities": 0, "attempts": 0},
            
            # Defesa da SB
            "SB fold to CO Steal": {"opportunities": 0, "attempts": 0},
            "SB fold to BTN Steal": {"opportunities": 0, "attempts": 0},
            "SB resteal vs BTN": {"opportunities": 0, "attempts": 0},
        }
    
    def analyze_hand(self, hand_text: str) -> None:
        """
        Analyze a single hand and update statistics
        """
        # Store current hand for collector
        self.current_hand_text = hand_text
        
        # Extract hand ID from the text
        self.current_hand_id = self._extract_hand_id(hand_text)
        
        # Detect Hero from "Dealt to" line
        hero_name = self._detect_hero(hand_text)
        
        # Extract positions and actions from the hand
        positions = self._extract_positions(hand_text)
        preflop_actions = self._extract_preflop_actions(hand_text)
        
        # Extract stacks and big blind size
        stacks_chips, bb_size = self._extract_stacks_and_bb(hand_text)
        
        # Convert stacks to big blinds
        stacks_bb = {}
        if bb_size > 0:
            for player, chips in stacks_chips.items():
                stacks_bb[player] = round(chips / bb_size, 2)
        
        # Replace Hero's actual name with "Hero" in positions, actions, and stacks
        if hero_name:
            # Replace in positions if Hero is there
            if hero_name in positions:
                positions["Hero"] = positions.pop(hero_name)
            
            # ALWAYS replace in stacks if Hero is there (even if not in positions)
            if hero_name in stacks_bb:
                stacks_bb["Hero"] = stacks_bb.pop(hero_name)
            
            # Also replace in actions
            for action in preflop_actions:
                if action["player"] == hero_name:
                    action["player"] = "Hero"
        
        # DIAGNOSTIC: Log stack extraction AFTER Hero replacement (INFO level for visibility)
        logger.info(f"[STACK DIAGNOSTIC] Hand {self.current_hand_id}: hero_name={hero_name}, bb_size={bb_size}, stacks_bb keys={list(stacks_bb.keys())}, Hero stack={stacks_bb.get('Hero', 'MISSING')}")
        
        if not positions or not preflop_actions:
            return
        
        # Analyze RFI opportunities
        self._analyze_rfi(hand_text, positions, preflop_actions, stacks_bb)
        
        # Analyze 3bet/cold call opportunities
        self._analyze_3bet_coldcall(hand_text, positions, preflop_actions, stacks_bb)
        
        # Analyze blind vs blind
        self._analyze_bvb(hand_text, positions, preflop_actions, stacks_bb)
        
        # Analyze squeeze opportunities
        self._analyze_squeeze(hand_text, positions, preflop_actions, stacks_bb)
        
        # Analyze blind defense
        self._analyze_blind_defense(hand_text, positions, preflop_actions, stacks_bb)
        
        # Analyze SB defense
        self._analyze_sb_defense(hand_text, positions, preflop_actions, stacks_bb)
        
        # Analyze fold to 3bet situations
        self._analyze_fold_to_3bet(hand_text, positions, preflop_actions, stacks_bb)
        
        # Update VPIP stats based on 3bet and Cold Call data
        self._update_vpip_stats()
    
    def _detect_hero(self, hand_text: str) -> Optional[str]:
        """Detect Hero player name from Dealt to line"""
        import re
        # Look for "Dealt to PlayerName [cards]" - support names with spaces
        dealt_match = re.search(r'Dealt to ([^\[]+?)\s*\[', hand_text)
        if dealt_match:
            return dealt_match.group(1).strip()
        return None
    
    def _check_allin_before_hero(self, actions: List[Dict]) -> bool:
        """
        Check if there was an all-in RAISE/BET before Hero acted.
        All-in calls/limps are allowed (they don't block opportunities).
        Returns True if all-in raise/bet occurred before Hero's turn.
        
        REFACTORED: Now uses centralized PreflopOpportunityValidator
        """
        # Update validator with current hand ID for logging
        self.validator.hand_id = self.current_hand_id
        return self.validator.check_allin_before_hero(actions)
    
    def _extract_hand_id(self, hand_text: str) -> Optional[str]:
        """Extract hand ID from hand text"""
        import re
        # Look for various patterns that indicate a hand ID
        # Try multiple patterns to catch different formats
        patterns = [
            r'Poker Hand\s*#([A-Z0-9]+):',  # PokerStars/GGPoker format with alphanumeric IDs
            r'Poker Hand\s*#(\d+):',        # PokerStars format with numeric IDs
            r'Game\s*#([A-Z0-9]+)',          # Other sites with alphanumeric
            r'Game\s*#(\d+)',                # Other sites with numeric
            r'Hand\s*#([A-Z0-9]+)',          # Generic format with alphanumeric
            r'Hand\s*#(\d+)',                # Generic format with numeric
        ]
        
        for pattern in patterns:
            match = re.search(pattern, hand_text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        # If no ID found, return None
        return None
    
    def _extract_positions(self, hand_text: str) -> Dict[str, str]:
        """
        Extract player positions from hand text
        Returns mapping of player name to position
        """
        positions = {}
        
        # Look for button designation (support both "Seat #X" and "Seat X" formats)
        button_match = re.search(r"Seat #?(\d+) is the button", hand_text)
        button_seat = int(button_match.group(1)) if button_match else None
        
        # Extract all seats with players (excluding those out of hand for PokerStars)
        # Use (.+?) to capture player names with spaces
        # Check if this is a PokerStars hand
        is_pokerstars = hand_text.startswith('PokerStars')
        
        seat_lines = []
        for line in hand_text.split('\n'):
            # 888poker/888.pt format: "Seat 1: Player ( 1500 )" without "in chips"
            # PokerStars format: "Seat 1: Player (1500 in chips)"
            # Accept both formats by checking for "Seat" and "(" but not requiring "in chips"
            if 'Seat ' in line and '(' in line:
                # Only exclude "out of hand" players for PokerStars
                if is_pokerstars and 'out of hand' in line:
                    continue
                # Support both standard format and 888poker format (with extra space and currency symbols)
                # Standard: Seat 1: Player (1500 in chips)
                # 888poker: Seat 1: Player ( 1500 ) or Seat 1: Player ( 1500 in chips)
                # Match the player and seat, but don't capture the stack amount (that's extracted elsewhere)
                match = re.search(r'Seat\s+(\d+):\s+(.+?)\s+\(', line)
                if match:
                    seat_lines.append((match.group(1), match.group(2)))
        
        seats = seat_lines
        
        if not seats or not button_seat:
            return positions
        
        # Create ordered list of active seats
        active_seats = [(int(seat), player) for seat, player in seats]
        active_seats.sort(key=lambda x: x[0])
        
        # Find button index
        button_idx = None
        for i, (seat_num, player) in enumerate(active_seats):
            if seat_num == button_seat:
                button_idx = i
                break
        
        if button_idx is None:
            return positions
        
        # Assign positions based on button
        num_players = len(active_seats)
        
        # Rotate the list so button is at index 0
        # This anchors the position mapping to BTN instead of SB
        rotated_seats = active_seats[button_idx:] + active_seats[:button_idx]
        
        # Use centralized position mapping (GG Poker standard)
        position_map = get_position_map(num_players)
        if not position_map:
            return positions  # Unsupported player count
        
        for i, (seat_num, player) in enumerate(rotated_seats):
            positions[player] = position_map.get(i, "Unknown")
        
        return positions
    
    
    def _normalize_currency_value(self, value: str) -> float:
        """
        Normalize currency string to float, handling all currency symbols and decimal formats.
        Supports: $12.34, €12,50, R$12.34, 1.200,50 (European), 1,200.50 (US), $1,000 (thousands)
        """
        if not value:
            return 0.0
        
        # Remove all currency symbols and spaces
        clean = re.sub(r'[€£$R¥₹\s]', '', value.strip())
        
        if not clean:
            return 0.0
        
        # Find positions of separators
        last_comma = clean.rfind(',')
        last_dot = clean.rfind('.')
        
        # Determine if we have a decimal separator or just thousand separators
        if last_comma > last_dot:
            # Comma is rightmost - check if it's decimal or thousands
            digits_after_comma = len(clean) - last_comma - 1
            if digits_after_comma == 3 and last_dot == -1:
                # Format: 1,000 - comma is thousand separator, no decimal
                clean = clean.replace(',', '')
            else:
                # Format: 1.200,50 or 12,50 - comma is decimal
                clean = clean.replace('.', '').replace(',', '.')
        elif last_dot > last_comma:
            # Dot is rightmost - check if it's decimal or thousands
            digits_after_dot = len(clean) - last_dot - 1
            if digits_after_dot == 3 and last_comma == -1:
                # Format: 1.000 - dot is thousand separator, no decimal
                clean = clean.replace('.', '')
            else:
                # Format: 1,200.50 or 12.50 - dot is decimal
                clean = clean.replace(',', '')
        # else: no separators, already clean
        
        try:
            return float(clean)
        except ValueError:
            return 0.0
    
    def _extract_stacks_and_bb(self, hand_text: str) -> tuple[Dict[str, float], float]:
        """
        Extract player stacks and big blind size from hand text.
        Supports multi-site formats: GG Poker, PokerStars, Winamax, 888poker, WPN
        
        Returns:
            (stacks_dict, bb_size) where stacks_dict maps player_name -> stack_size
        """
        stacks = {}
        bb_size = 0.0
        
        # Pattern for seat lines - support multi-character currencies like R$
        # Works for: GG, Stars, WPN, Winamax, 888, all currencies
        # Seat 1: PlayerName (12345 in chips)
        # Seat 1: PlayerName ($12.34 in chips)
        # Seat 1: PlayerName (€12.34 in chips)
        # Seat 1: PlayerName (R$12.34 in chips)
        # PKO: Seat 1: PlayerName (5000 in chips, $10 bounty)
        seat_pattern = r'Seat \d+: (.+?) \((?:R\$|€|£|\$|¥|₹)?([0-9,.]+) in chips(?:,\s*[^)]+)?\)'
        
        # Pattern for Winamax (no "in chips"):
        # Seat 1: PlayerName (€12345)
        # PKO: Seat 1: PlayerName (20000, 9€ bounty)
        winamax_pattern = r'Seat \d+: (.+?) \((?:R\$|€|£|\$|¥|₹)?([0-9,.]+)(?:,\s*[^)]+)?\)'
        
        # Pattern for 888poker (different format - no "in chips"):
        # Seat 1: PlayerName ( 35.459 )
        # Seat 1: PlayerName ( $12.34 )
        # Note: Must exclude "is the button" lines
        poker888_pattern = r'Seat \d+: (.+?) \(\s*(?:R\$|€|£|\$|¥|₹)?\s*([0-9,.]+)\s*\)(?!\s*is the button)'
        
        # Extract stacks
        for line in hand_text.split('\n'):
            # Try standard format first
            match = re.search(seat_pattern, line)
            if not match:
                # Try Winamax format
                match = re.search(winamax_pattern, line)
            if not match:
                # Try 888poker format
                match = re.search(poker888_pattern, line)
            
            if match:
                player_name = match.group(1).strip()
                stack_value = self._normalize_currency_value(match.group(2))
                if stack_value > 0:
                    stacks[player_name] = stack_value
        
        # Extract big blind size - support all currency symbols including R$
        # Pattern: "posts big blind 400" or "posts big blind $2.00" or "posts big blind [2000]" (888poker)
        bb_pattern = r'posts (?:the )?big blind \[?(?:R\$|€|£|\$|¥|₹)?([0-9,.]+)\]?'
        
        # 888poker also uses "X/Y Blinds" format (e.g., "350/700 Blinds")
        blinds_pattern = r'([0-9,.]+)/([0-9,.]+)\s+Blinds'
        
        for line in hand_text.split('\n'):
            # Try standard "posts big blind" format
            match = re.search(bb_pattern, line, re.IGNORECASE)
            if match:
                bb_size = self._normalize_currency_value(match.group(1))
                if bb_size > 0:
                    break  # Found BB, stop searching
            
            # Try 888poker "X/Y Blinds" format
            match = re.search(blinds_pattern, line)
            if match:
                sb_size = self._normalize_currency_value(match.group(1))
                bb_size = self._normalize_currency_value(match.group(2))
                if bb_size > 0:
                    break  # Found BB, stop searching
        
        return stacks, bb_size
    
    
    def _extract_preflop_actions(self, hand_text: str) -> List[Dict[str, Any]]:
        """
        Extract pre-flop actions from hand text
        Returns list of actions in order
        """
        actions = []
        
        # Find pre-flop section - support both PokerStars and 888 formats
        preflop_start = hand_text.find("*** HOLE CARDS ***")
        if preflop_start == -1:
            # Try 888poker/888.pt format
            preflop_start = hand_text.find("** Dealing down cards **")
        
        flop_start = hand_text.find("*** FLOP ***")
        if flop_start == -1:
            # Try 888poker/888.pt format
            flop_start = hand_text.find("** Dealing Flop **")
        
        if preflop_start == -1:
            return actions
        
        if flop_start == -1:
            # No flop means hand ended pre-flop
            preflop_section = hand_text[preflop_start:]
        else:
            preflop_section = hand_text[preflop_start:flop_start]
        
        # Parse actions - support both formats:
        # PokerStars/GG: "PlayerName: folds"
        # 888poker/888.pt: "PlayerName folds"
        action_pattern_with_colon = r'^([^:]+):\s+(folds|calls|raises|bets|checks|posts|is all-in)'
        action_pattern_without_colon = r'^(\S+)\s+(folds|calls|raises|bets|checks|posts|all-in)'
        
        for line in preflop_section.split('\n'):
            line_stripped = line.strip()
            
            # Try format with colon first (PokerStars/GG)
            match = re.match(action_pattern_with_colon, line_stripped)
            if not match:
                # Try format without colon (888poker/888.pt)
                match = re.match(action_pattern_without_colon, line_stripped)
            
            if match:
                player = match.group(1).strip()
                action = match.group(2).strip()
                
                # Skip blind posts
                if "posts" in action:
                    continue
                
                # Detect all-in from full line (not just action text)
                is_allin = "all-in" in line.lower() or "all in" in line.lower()
                
                actions.append({
                    "player": player,
                    "action": action,
                    "is_raise": "raises" in action or "bets" in action,
                    "is_call": "calls" in action,
                    "is_fold": "folds" in action,
                    "is_allin": is_allin
                })
        
        return actions
    
    def _analyze_rfi(self, hand_text: str, positions: Dict[str, str], actions: List[Dict], stacks_bb: Dict[str, float]) -> None:
        """Analyze RFI (Raise First In) opportunities - when Hero can open the pot"""
        hero_position = positions.get("Hero")
        if not hero_position:
            return
        
        # Track actions before Hero
        raise_before_hero = False
        limp_before_hero = False
        hero_acted = False
        hero_action = None
        
        for action in actions:
            player = action["player"]
            
            # Check if this is Hero's action
            if player == "Hero":
                hero_acted = True
                hero_action = action
                break
            
            # Track if anyone raised before Hero
            if action["is_raise"]:
                raise_before_hero = True
                break  # No RFI opportunity if someone raised
            
            # Track if anyone called/limped before Hero (not SB/BB completing)
            if action["is_call"]:
                player_position = positions.get(player)
                # Only count as limp if not SB/BB completing
                if player_position not in ["SB", "BB"]:
                    limp_before_hero = True
        
        # Count RFI opportunity if:
        # 1. Hero acted (had a chance to act)
        # 2. No one raised before Hero (pot is unopened)
        # 3. No one limped before Hero (everyone folded)
        if hero_acted and not raise_before_hero and not limp_before_hero:
            # Determine which RFI stat this belongs to
            rfi_stat = get_rfi_stat_for_position(hero_position)
            
            if rfi_stat:
                # CENTRALIZED VALIDATION: Use validator to check all stack/all-in rules
                self.validator.hand_id = self.current_hand_id
                is_valid, reason = self.validator.validate_rfi_steal(
                    actions=actions,
                    hero_position=hero_position,
                    stacks_bb=stacks_bb,
                    positions=positions
                )
                
                if not is_valid:
                    logger.info(f"[RFI BLOCKED] Hand {self.current_hand_id} - {rfi_stat}: {reason}")
                else:
                    # Record RFI opportunity
                    self.stats[rfi_stat]["opportunities"] += 1
                    
                    # Collect hand opportunity
                    if self.hand_collector and self.current_hand_text:
                        self.hand_collector.add_hand(rfi_stat, self.current_hand_text, self.current_hand_id)
                    
                    # Check if Hero actually raised (RFI/steal attempt)
                    if hero_action and hero_action["is_raise"]:
                        self.stats[rfi_stat]["attempts"] += 1
    
    def _analyze_3bet_coldcall(self, hand_text: str, positions: Dict[str, str], actions: List[Dict], stacks_bb: Dict[str, float]) -> None:
        """Analyze 3bet and cold call opportunities - only single raised pots without limpers or callers"""
        hero_position = positions.get("Hero")
        
        if not hero_position or hero_position in ["SB", "BB"]:
            return
        
        # Find the raiser before hero acts (for validation)
        raiser_name = None
        for action in actions:
            if action["player"] == "Hero":
                break
            if action["is_raise"]:
                raiser_name = action["player"]
                break
        
        # Track the action sequence before Hero
        raises_before = 0
        limpers_before_raise = 0  # Count limpers/calls before the first raise
        calls_after_raise = 0     # Count calls after the first raise
        co_steal_attempt = False  # Track if CO made a valid steal attempt
        first_raise_occurred = False
        
        # Check if CO made a steal attempt (all folded to CO, then CO raised)
        actions_before_co = []
        co_position_found = False
        
        for action in actions:
            player = action["player"]
            position = positions.get(player)
            
            # Check if this is CO's action
            if position == "CO" and not co_position_found:
                co_position_found = True
                # Check if all non-blind players before CO folded
                non_blind_actions = [a for a in actions_before_co if positions.get(a["player"]) not in ["SB", "BB"]]
                all_folded = all(a.get("is_fold", False) for a in non_blind_actions)
                
                # CO steal attempt = everyone folded to CO and CO raises
                if all_folded and action["is_raise"]:
                    co_steal_attempt = True
            
            # Store actions before we reach Hero
            if player != "Hero":
                actions_before_co.append(action)
            
            # When we reach Hero's action
            if player == "Hero":
                # For 3bet/CC: Need exactly 1 raise, no limpers before raise, no calls after raise
                is_clean_single_raised = (
                    raises_before == 1 and 
                    limpers_before_raise == 0 and 
                    calls_after_raise == 0
                )
                
                if is_clean_single_raised:
                    # Map positions to stat names using centralized categorization
                    # Now correctly maps UTG+2 to EP in 9-max!
                    pos_map = {
                        "UTG": "EP", "UTG+1": "EP", "UTG+2": "EP",  # All Early positions
                        "MP": "MP", "MP+1": "MP", "MP+2": "MP", "HJ": "MP",  # All Middle positions
                        "CO": "CO",
                        "BTN": "BTN"
                    }
                    
                    stat_prefix = pos_map.get(hero_position)
                    if stat_prefix:
                        # CENTRALIZED VALIDATION: Use validator for 3bet/Cold Call
                        self.validator.hand_id = self.current_hand_id
                        is_valid, reason = self.validator.validate_3bet_defense(
                            actions=actions,
                            raiser_name=raiser_name,
                            stacks_bb=stacks_bb
                        )
                        
                        if not is_valid:
                            logger.info(f"[3BET/COLDCALL BLOCKED] Hand {self.current_hand_id} - {stat_prefix}: {reason}")
                        else:
                            # Record opportunity for both 3bet and Cold Call
                            self.stats[f"{stat_prefix} 3bet"]["opportunities"] += 1
                            self.stats[f"{stat_prefix} Cold Call"]["opportunities"] += 1
                            
                            # Collect hand for both opportunities
                            if self.hand_collector and self.current_hand_text:
                                self.hand_collector.add_hand(f"{stat_prefix} 3bet", self.current_hand_text, self.current_hand_id)
                                self.hand_collector.add_hand(f"{stat_prefix} Cold Call", self.current_hand_text, self.current_hand_id)
                            
                            # Check what Hero did
                            if action["is_raise"]:
                                # Hero 3bet
                                self.stats[f"{stat_prefix} 3bet"]["attempts"] += 1
                            elif action["is_call"]:
                                # Hero cold called
                                self.stats[f"{stat_prefix} Cold Call"]["attempts"] += 1
                
                # Check BTN fold to CO steal (only when CO opened the pot)
                if hero_position == "BTN" and co_steal_attempt:
                    # CENTRALIZED VALIDATION: Use validator for BTN fold to CO
                    self.validator.hand_id = self.current_hand_id
                    is_valid, reason = self.validator.validate_3bet_defense(
                        actions=actions,
                        raiser_name=raiser_name,
                        stacks_bb=stacks_bb
                    )
                    
                    if not is_valid:
                        logger.info(f"[BTN FOLD CO BLOCKED] Hand {self.current_hand_id}: {reason}")
                    else:
                        self.stats["BTN fold to CO steal"]["opportunities"] += 1
                        # Collect hand for BTN fold to CO steal opportunity
                        if self.hand_collector and self.current_hand_text:
                            self.hand_collector.add_hand("BTN fold to CO steal", self.current_hand_text, self.current_hand_id)
                        if action["is_fold"]:
                            self.stats["BTN fold to CO steal"]["attempts"] += 1
                
                # Stop processing after Hero acts
                break
            
            # Track action sequence
            if action["is_raise"]:
                raises_before += 1
                first_raise_occurred = True
            elif action["is_call"]:
                # Ignore SB/BB calls (they are blind posts, not limps/calls)
                position_of_caller = positions.get(player)
                if position_of_caller not in ["SB", "BB"]:
                    if not first_raise_occurred:
                        # This is a limper before any raise
                        limpers_before_raise += 1
                    else:
                        # This is a call after the first raise
                        calls_after_raise += 1
        
        # VPIP will be calculated from 3bet + Cold Call stats later
    
    def _analyze_bvb(self, hand_text: str, positions: Dict[str, str], actions: List[Dict], stacks_bb: Dict[str, float]) -> None:
        """Analyze Blind vs Blind situations - only when Hero is involved"""
        # Find SB and BB players
        sb_player = None
        bb_player = None
        hero_position = None
        
        for player, position in positions.items():
            if position == "SB":
                sb_player = player
            elif position == "BB":
                bb_player = player
            
            # Track Hero's position
            if player == "Hero":
                hero_position = position
        
        if not sb_player or not bb_player:
            return
        
        # Only analyze if Hero is in SB or BB
        if hero_position not in ["SB", "BB"]:
            return
        
        # Check if all players folded to SB (BvB situation)
        # IMPORTANT: No raises or limps should have occurred before SB acts
        is_bvb = True
        first_actor = None
        sb_action = None
        bb_action = None
        raises_before_sb = 0
        limps_before_sb = 0  # Track limps/calls before SB
        
        for action in actions:
            player = action["player"]
            
            # If someone other than SB or BB acts (not folding), it's not BvB
            if player not in [sb_player, bb_player]:
                # Count any raises before SB acts
                if action["is_raise"]:
                    raises_before_sb += 1
                # Count any limps/calls before SB acts
                elif action["is_call"]:
                    limps_before_sb += 1
                # If they don't fold, it's not BvB
                if not action["is_fold"]:
                    is_bvb = False
                    break
            else:
                # Track first actor between SB and BB
                if not first_actor:
                    first_actor = player
                
                # Track SB and BB actions
                if player == sb_player and not sb_action:
                    sb_action = action
                elif player == bb_player and not bb_action:
                    bb_action = action
        
        # Not BvB if there were raises or limps before SB, or it's not a true BvB situation
        if not is_bvb or raises_before_sb > 0 or limps_before_sb > 0:
            return
        
        # Now we have a BvB situation with Hero involved - analyze the specific stats
        
        # Determine villain for validation
        villain_player = bb_player if hero_position == "SB" else sb_player
        
        # Stats when Hero is SB
        if hero_position == "SB" and first_actor == sb_player:
            # CENTRALIZED VALIDATION for all SB stats
            self.validator.hand_id = self.current_hand_id
            is_valid, reason = self.validator.validate_bvb(
                actions=actions,
                villain_player=villain_player,
                stacks_bb=stacks_bb
            )
            
            if not is_valid:
                logger.info(f"[BVB SB BLOCKED] Hand {self.current_hand_id}: {reason}")
            else:
                # 1. SB UO VPIP - When Hero SB has opportunity to act first (unopened)
                self.stats["SB UO VPIP"]["opportunities"] += 1
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("SB UO VPIP", self.current_hand_text, self.current_hand_id)
                if sb_action and (sb_action["is_raise"] or sb_action["is_call"]):
                    self.stats["SB UO VPIP"]["attempts"] += 1
                
                # 4. SB Steal - When Hero SB raises in unopened pot
                self.stats["SB Steal"]["opportunities"] += 1
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("SB Steal", self.current_hand_text, self.current_hand_id)
                if sb_action and sb_action["is_raise"]:
                    self.stats["SB Steal"]["attempts"] += 1
        
        # Stats when Hero is BB
        if hero_position == "BB":
            # CENTRALIZED VALIDATION for all BB stats
            self.validator.hand_id = self.current_hand_id
            is_valid, reason = self.validator.validate_bvb(
                actions=actions,
                villain_player=villain_player,
                stacks_bb=stacks_bb
            )
            
            if not is_valid:
                logger.info(f"[BVB BB BLOCKED] Hand {self.current_hand_id}: {reason}")
            else:
                # 2. BB fold vs SB steal - When SB raises and Hero is BB
                if sb_action and sb_action["is_raise"]:
                    self.stats["BB fold vs SB steal"]["opportunities"] += 1
                    if self.hand_collector and self.current_hand_text:
                        self.hand_collector.add_hand("BB fold vs SB steal", self.current_hand_text, self.current_hand_id)
                    if bb_action and bb_action["is_fold"]:
                        self.stats["BB fold vs SB steal"]["attempts"] += 1
                
                # 3. BB raise vs SB limp - When SB limps and Hero is BB
                elif sb_action and sb_action["is_call"]:
                    self.stats["BB raise vs SB limp UOP"]["opportunities"] += 1
                    if self.hand_collector and self.current_hand_text:
                        self.hand_collector.add_hand("BB raise vs SB limp UOP", self.current_hand_text, self.current_hand_id)
                    if bb_action and bb_action["is_raise"]:
                        self.stats["BB raise vs SB limp UOP"]["attempts"] += 1
    
    
    def _update_vpip_stats(self) -> None:
        """Update VPIP stats based on 3bet and Cold Call data"""
        # VPIP = 3bet + Cold Call combined
        for prefix in ["EP", "MP", "CO", "BTN"]:
            three_bet_stat = self.stats.get(f"{prefix} 3bet", {})
            cold_call_stat = self.stats.get(f"{prefix} Cold Call", {})
            vpip_stat = self.stats.get(f"{prefix} VPIP", {})
            
            # VPIP opportunities = max of 3bet or Cold Call opportunities (should be same)
            vpip_stat["opportunities"] = max(
                three_bet_stat.get("opportunities", 0),
                cold_call_stat.get("opportunities", 0)
            )
            
            # VPIP attempts = 3bet attempts + Cold Call attempts
            vpip_stat["attempts"] = (
                three_bet_stat.get("attempts", 0) +
                cold_call_stat.get("attempts", 0)
            )
    
    def _analyze_squeeze(self, hand_text: str, positions: Dict[str, str], actions: List[Dict], stacks_bb: Dict[str, float]) -> None:
        """Analyze squeeze opportunities - only single raised pots"""
        hero_position = positions.get("Hero")
        if not hero_position:
            return
        
        # Find raiser and caller(s) for validation
        raiser_name = None
        caller_names = []
        
        for action in actions:
            if action["player"] == "Hero":
                break
            if action["is_raise"] and not raiser_name:
                raiser_name = action["player"]
            elif action["is_call"] and raiser_name:  # Call after raise
                caller_names.append(action["player"])
        
        # Track raise and call actions before Hero
        raise_count = 0
        first_raiser_position = None
        has_call_after_first_raise = False
        callers_after_raise = []
        hero_acted = False
        hero_action = None
        
        for action in actions:
            player = action["player"]
            position = positions.get(player)
            
            # If Hero already acted, stop tracking
            if hero_acted:
                break
            
            # Check if this is Hero's action
            if player == "Hero":
                hero_acted = True
                hero_action = action
                break
            
            # Track ALL raises
            if action["is_raise"]:
                raise_count += 1
                # Only track the first raiser
                if raise_count == 1:
                    first_raiser_position = position
            
            # Track calls after the FIRST raise (not after re-raises)
            elif action["is_call"] and raise_count == 1:
                has_call_after_first_raise = True
                callers_after_raise.append(position)
        
        # ONLY count squeeze opportunities in single raised pots
        if raise_count != 1:
            return  # Skip if not single raised pot
        
        # General Squeeze opportunity: exactly 1 raise + at least 1 call before Hero
        if has_call_after_first_raise and hero_acted:
            # CENTRALIZED VALIDATION for Squeeze
            self.validator.hand_id = self.current_hand_id
            is_valid, reason = self.validator.validate_squeeze(
                actions=actions,
                raiser_name=raiser_name,
                caller_names=caller_names,
                stacks_bb=stacks_bb
            )
            
            if not is_valid:
                logger.info(f"[SQUEEZE BLOCKED] Hand {self.current_hand_id}: {reason}")
            else:
                self.stats["Squeeze"]["opportunities"] += 1
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("Squeeze", self.current_hand_text, self.current_hand_id)
                if hero_action and hero_action["is_raise"]:
                    self.stats["Squeeze"]["attempts"] += 1
        
        # Squeeze vs BTN Raiser: BTN raises (single raised), SB calls, Hero in BB
        if (hero_position == "BB" and 
            first_raiser_position == "BTN" and 
            "SB" in callers_after_raise and
            hero_acted):
            # CENTRALIZED VALIDATION for Squeeze vs BTN Raiser
            self.validator.hand_id = self.current_hand_id
            is_valid, reason = self.validator.validate_squeeze(
                actions=actions,
                raiser_name=raiser_name,
                caller_names=caller_names,
                stacks_bb=stacks_bb
            )
            
            if not is_valid:
                logger.info(f"[SQUEEZE BTN BLOCKED] Hand {self.current_hand_id}: {reason}")
            else:
                self.stats["Squeeze vs BTN Raiser"]["opportunities"] += 1
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("Squeeze vs BTN Raiser", self.current_hand_text, self.current_hand_id)
                if hero_action and hero_action["is_raise"]:
                    self.stats["Squeeze vs BTN Raiser"]["attempts"] += 1
    
    def _analyze_blind_defense(self, hand_text: str, positions: Dict[str, str], actions: List[Dict], stacks_bb: Dict[str, float]) -> None:
        """Analyze blind defense situations - only single raised pots without callers"""
        hero_position = positions.get("Hero")
        if hero_position != "BB":
            return  # Only analyze when Hero is in BB
        
        logger.debug(f"[BB DEFENSE] Hand {self.current_hand_id}: Hero in BB, analyzing defense...")
        
        # Find the raiser for validation
        raiser_name = None
        for action in actions:
            if action["player"] == "Hero":
                break
            if action["is_raise"]:
                raiser_name = action["player"]
                break
        
        # Track ALL raises, calls and raisers to ensure clean steal situation
        first_raiser_position = None
        raise_count = 0
        calls_after_raise = 0
        sb_folded = False
        hero_acted = False
        hero_action = None
        actions_before_raiser = []
        raiser_found = False
        
        for action in actions:
            player = action["player"]
            position = positions.get(player)
            
            # If Hero already acted, stop tracking 
            if hero_acted:
                break
            
            # Check if this is Hero's action
            if player == "Hero":
                hero_acted = True
                hero_action = action
                break
            
            # Track raises
            if action["is_raise"]:
                raise_count += 1
                # Only track the FIRST raiser
                if raise_count == 1:
                    first_raiser_position = position
                    raiser_found = True
                    # Check if all actions before the raiser were folds
                    all_folded_before = all(a.get("is_fold", False) for a in actions_before_raiser 
                                           if positions.get(a["player"]) not in ["SB", "BB"])
                    if not all_folded_before:
                        return  # Someone acted before the raiser (not a clean steal)
            
            # Track calls after the first raise
            elif action["is_call"] and raiser_found:
                # Don't count SB/BB as callers for blind posting
                if position not in ["SB", "BB"]:
                    calls_after_raise += 1
                # But DO count SB call after a raise as a caller
                elif position == "SB" and raise_count > 0:
                    calls_after_raise += 1
            
            # Track if SB folded (for BB resteal vs BTN scenario)
            if position == "SB" and action["is_fold"]:
                sb_folded = True
            
            # Store actions before raiser
            if not raiser_found:
                actions_before_raiser.append(action)
        
        # ONLY count if it's a single raised pot (exactly 1 raise before Hero acts)
        if raise_count != 1:
            logger.debug(f"[BB DEFENSE] Hand {self.current_hand_id}: SKIP - raise_count={raise_count} (need exactly 1)")
            return  # Skip if not single raised pot
        
        # Only count if there are no calls after the raise (clean steal situation)
        if calls_after_raise > 0:
            logger.debug(f"[BB DEFENSE] Hand {self.current_hand_id}: SKIP - calls_after_raise={calls_after_raise} (need 0)")
            return  # Skip if there are callers
        
        logger.debug(f"[BB DEFENSE] Hand {self.current_hand_id}: Clean steal from {first_raiser_position}, SB folded={sb_folded}")
        
        # BB fold vs CO steal (clean steal from CO, no callers)
        if first_raiser_position == "CO" and hero_acted:
            # CENTRALIZED VALIDATION for BB fold vs CO steal
            self.validator.hand_id = self.current_hand_id
            is_valid, reason = self.validator.validate_3bet_defense(
                actions=actions,
                raiser_name=raiser_name,
                stacks_bb=stacks_bb
            )
            
            if not is_valid:
                logger.info(f"[BB DEFENSE BLOCKED] Hand {self.current_hand_id} - BB fold vs CO steal: {reason}")
            else:
                self.stats["BB fold vs CO steal"]["opportunities"] += 1
                # Collect hand opportunity
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("BB fold vs CO steal", self.current_hand_text, self.current_hand_id)
                if hero_action and hero_action["is_fold"]:
                    self.stats["BB fold vs CO steal"]["attempts"] += 1
        
        # BB fold vs BTN steal (clean steal from BTN, SB must have folded)
        if first_raiser_position == "BTN" and hero_acted and sb_folded:
            # CENTRALIZED VALIDATION for BB fold vs BTN steal
            self.validator.hand_id = self.current_hand_id
            is_valid, reason = self.validator.validate_3bet_defense(
                actions=actions,
                raiser_name=raiser_name,
                stacks_bb=stacks_bb
            )
            
            if not is_valid:
                logger.info(f"[BB DEFENSE BLOCKED] Hand {self.current_hand_id} - BB fold vs BTN steal: {reason}")
            else:
                self.stats["BB fold vs BTN steal"]["opportunities"] += 1
                # Collect hand opportunity
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("BB fold vs BTN steal", self.current_hand_text, self.current_hand_id)
                if hero_action and hero_action["is_fold"]:
                    self.stats["BB fold vs BTN steal"]["attempts"] += 1
            
            # BB resteal vs BTN steal (only when SB folded and single raised)
            # CENTRALIZED VALIDATION for BB resteal vs BTN steal
            self.validator.hand_id = self.current_hand_id
            is_valid_resteal, reason_resteal = self.validator.validate_3bet_defense(
                actions=actions,
                raiser_name=raiser_name,
                stacks_bb=stacks_bb
            )
            
            if not is_valid_resteal:
                logger.info(f"[BB DEFENSE BLOCKED] Hand {self.current_hand_id} - BB resteal vs BTN steal: {reason_resteal}")
            else:
                self.stats["BB resteal vs BTN steal"]["opportunities"] += 1
                # Collect hand opportunity
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("BB resteal vs BTN steal", self.current_hand_text, self.current_hand_id)
                if hero_action and hero_action["is_raise"]:
                    self.stats["BB resteal vs BTN steal"]["attempts"] += 1
        
        # Note: BB fold vs SB steal is already handled in _analyze_bvb
    
    def _analyze_sb_defense(self, hand_text: str, positions: Dict[str, str], actions: List[Dict], stacks_bb: Dict[str, float]) -> None:
        """Analyze SB defense situations - only single raised pots without callers"""
        hero_position = positions.get("Hero")
        if hero_position != "SB":
            return  # Only analyze when Hero is in SB
        
        logger.debug(f"[SB DEFENSE] Hand {self.current_hand_id}: Hero in SB, analyzing defense...")
        
        # Find the raiser for validation
        raiser_name = None
        for action in actions:
            if action["player"] == "Hero":
                break
            if action["is_raise"]:
                raiser_name = action["player"]
                break
        
        # Track ALL raises, calls and raisers to ensure clean steal situation
        first_raiser_position = None
        raise_count = 0
        calls_after_raise = 0
        btn_folded = False
        hero_acted = False
        hero_action = None
        actions_before_raiser = []
        raiser_found = False
        
        for action in actions:
            player = action["player"]
            position = positions.get(player)
            
            # If Hero already acted, stop tracking 
            if hero_acted:
                break
            
            # Check if this is Hero's action
            if player == "Hero":
                hero_acted = True
                hero_action = action
                break
            
            # Track raises
            if action["is_raise"]:
                raise_count += 1
                # Only track the FIRST raiser
                if raise_count == 1:
                    first_raiser_position = position
                    raiser_found = True
                    # Check if all actions before the raiser were folds
                    all_folded_before = all(a.get("is_fold", False) for a in actions_before_raiser 
                                           if positions.get(a["player"]) not in ["SB", "BB"])
                    if not all_folded_before:
                        return  # Someone acted before the raiser (not a clean steal)
            
            # Track calls after the first raise
            elif action["is_call"] and raiser_found:
                # Don't count SB/BB as callers for blind posting
                if position not in ["SB", "BB"]:
                    calls_after_raise += 1
            
            # Track if BTN folded (for SB fold to CO steal scenario)
            if position == "BTN" and action["is_fold"]:
                btn_folded = True
            
            # Store actions before raiser
            if not raiser_found:
                actions_before_raiser.append(action)
        
        # ONLY count if it's a single raised pot (exactly 1 raise before Hero acts)
        if raise_count != 1:
            return  # Skip if not single raised pot
        
        # Only count if there are no calls after the raise (clean steal situation)
        if calls_after_raise > 0:
            return  # Skip if there are callers
        
        # SB fold to CO steal (clean steal from CO, BTN must have folded)
        if first_raiser_position == "CO" and hero_acted and btn_folded:
            # CENTRALIZED VALIDATION for SB fold to CO Steal
            self.validator.hand_id = self.current_hand_id
            is_valid, reason = self.validator.validate_3bet_defense(
                actions=actions,
                raiser_name=raiser_name,
                stacks_bb=stacks_bb
            )
            
            if not is_valid:
                logger.info(f"[SB DEFENSE BLOCKED] Hand {self.current_hand_id} - SB fold to CO Steal: {reason}")
            else:
                self.stats["SB fold to CO Steal"]["opportunities"] += 1
                # Collect hand opportunity
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("SB fold to CO Steal", self.current_hand_text, self.current_hand_id)
                if hero_action and hero_action["is_fold"]:
                    self.stats["SB fold to CO Steal"]["attempts"] += 1
        
        # SB fold to BTN steal (clean steal from BTN, no callers)
        if first_raiser_position == "BTN" and hero_acted:
            # CENTRALIZED VALIDATION for SB fold to BTN Steal
            self.validator.hand_id = self.current_hand_id
            is_valid, reason = self.validator.validate_3bet_defense(
                actions=actions,
                raiser_name=raiser_name,
                stacks_bb=stacks_bb
            )
            
            if not is_valid:
                logger.info(f"[SB DEFENSE BLOCKED] Hand {self.current_hand_id} - SB fold to BTN Steal: {reason}")
            else:
                self.stats["SB fold to BTN Steal"]["opportunities"] += 1
                # Collect hand opportunity
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("SB fold to BTN Steal", self.current_hand_text, self.current_hand_id)
                if hero_action and hero_action["is_fold"]:
                    self.stats["SB fold to BTN Steal"]["attempts"] += 1
            
            # SB resteal vs BTN (when facing BTN steal and re-raising)
            # CENTRALIZED VALIDATION for SB resteal vs BTN
            self.validator.hand_id = self.current_hand_id
            is_valid_resteal, reason_resteal = self.validator.validate_3bet_defense(
                actions=actions,
                raiser_name=raiser_name,
                stacks_bb=stacks_bb
            )
            
            if not is_valid_resteal:
                logger.info(f"[SB DEFENSE BLOCKED] Hand {self.current_hand_id} - SB resteal vs BTN: {reason_resteal}")
            else:
                self.stats["SB resteal vs BTN"]["opportunities"] += 1
                # Collect hand opportunity
                if self.hand_collector and self.current_hand_text:
                    self.hand_collector.add_hand("SB resteal vs BTN", self.current_hand_text, self.current_hand_id)
                if hero_action and hero_action["is_raise"]:
                    self.stats["SB resteal vs BTN"]["attempts"] += 1
    
    def _analyze_fold_to_3bet(self, hand_text: str, positions: Dict[str, str], actions: List[Dict], stacks_bb: Dict[str, float]) -> None:
        """Analyze fold to 3bet situations (IP and OOP) - only clean open raise and 3bet"""
        hero_position = positions.get("Hero")
        if not hero_position:
            return
        
        # Find the 3-bettor for validation
        # We need to find Hero's raise first, then the 3bet after that
        hero_raised = False
        threebetter_name = None
        threebetter_action = None
        
        for i, action in enumerate(actions):
            if action["player"] == "Hero" and action["is_raise"]:
                hero_raised = True
            elif hero_raised and action["is_raise"]:
                threebetter_name = action["player"]
                threebetter_action = action
                break
        
        # Find Hero's open raise
        hero_open_raised = False
        hero_raise_index = -1
        actions_before_hero = []
        
        for i, action in enumerate(actions):
            if action["player"] == "Hero":
                non_blind_actions = [a for a in actions_before_hero if positions.get(a["player"]) not in ["SB", "BB"]]
                all_folded = all(a.get("is_fold", False) for a in non_blind_actions)
                if all_folded and action["is_raise"]:
                    hero_open_raised = True
                    hero_raise_index = i
                break
            actions_before_hero.append(action)
        
        # If Hero didn't open raise, skip
        if not hero_open_raised:
            return
        
        # Now look for exactly one 3bet after Hero's open raise
        raises_after_hero = 0
        calls_after_hero = 0
        threebetter_position = None
        threebetter_index = -1
        
        for i in range(hero_raise_index + 1, len(actions)):
            player = actions[i]["player"]
            
            # Skip if this is Hero's second action
            if player == "Hero":
                break
            
            # Count raises after Hero
            if actions[i]["is_raise"]:
                raises_after_hero += 1
                if raises_after_hero == 1:
                    threebetter_position = positions.get(player)
                    threebetter_index = i
            
            # Count calls after Hero (excluding blind calls)
            if actions[i]["is_call"]:
                caller_position = positions.get(player)
                if caller_position not in ["SB", "BB"]:
                    calls_after_hero += 1
        
        # Only count if exactly 1 raise (3bet) and no calls after Hero's open
        if raises_after_hero != 1 or calls_after_hero > 0:
            return
        
        # Find Hero's response to the 3bet
        for j in range(threebetter_index + 1, len(actions)):
            if actions[j]["player"] == "Hero":
                # Skip if threebetter position not found
                if not threebetter_position:
                    return
                
                # Determine if Hero will be IP or OOP post-flop
                is_ip = self._will_be_in_position(hero_position, threebetter_position)
                
                # CENTRALIZED VALIDATION for general Fold to 3bet
                self.validator.hand_id = self.current_hand_id
                is_valid, reason = self.validator.validate_fold_to_3bet(
                    actions_before_raise=actions[:hero_raise_index + 1],
                    threebetter_name=threebetter_name,
                    threebetter_action=threebetter_action,
                    stacks_bb=stacks_bb
                )
                
                if not is_valid:
                    logger.info(f"[FOLD3BET BLOCKED] Hand {self.current_hand_id} - Fold to 3bet: {reason}")
                else:
                    self.stats["Fold to 3bet"]["opportunities"] += 1
                    if actions[j]["is_fold"]:
                        self.stats["Fold to 3bet"]["attempts"] += 1
                
                # Now handle IP/OOP specific stats (use same validations)
                if is_ip:
                    # CENTRALIZED VALIDATION for Fold to 3bet IP
                    self.validator.hand_id = self.current_hand_id
                    is_valid_ip, reason_ip = self.validator.validate_fold_to_3bet(
                        actions_before_raise=actions[:hero_raise_index + 1],
                        threebetter_name=threebetter_name,
                        threebetter_action=threebetter_action,
                        stacks_bb=stacks_bb
                    )
                    
                    if not is_valid_ip:
                        logger.info(f"[FOLD3BET BLOCKED] Hand {self.current_hand_id} - Fold to 3bet IP: {reason_ip}")
                    else:
                        self.stats["Fold to 3bet IP"]["opportunities"] += 1
                        # Collect hand opportunity
                        if self.hand_collector and self.current_hand_text:
                            self.hand_collector.add_hand("Fold to 3bet IP", self.current_hand_text, self.current_hand_id)
                        if actions[j]["is_fold"]:
                            self.stats["Fold to 3bet IP"]["attempts"] += 1
                else:
                    # CENTRALIZED VALIDATION for Fold to 3bet OOP
                    self.validator.hand_id = self.current_hand_id
                    is_valid_oop, reason_oop = self.validator.validate_fold_to_3bet(
                        actions_before_raise=actions[:hero_raise_index + 1],
                        threebetter_name=threebetter_name,
                        threebetter_action=threebetter_action,
                        stacks_bb=stacks_bb
                    )
                    
                    if not is_valid_oop:
                        logger.info(f"[FOLD3BET BLOCKED] Hand {self.current_hand_id} - Fold to 3bet OOP: {reason_oop}")
                    else:
                        self.stats["Fold to 3bet OOP"]["opportunities"] += 1
                        # Collect hand opportunity
                        if self.hand_collector and self.current_hand_text:
                            self.hand_collector.add_hand("Fold to 3bet OOP", self.current_hand_text, self.current_hand_id)
                        if actions[j]["is_fold"]:
                            self.stats["Fold to 3bet OOP"]["attempts"] += 1
                break
    
    def _will_be_in_position(self, hero_pos: str, villain_pos: str) -> bool:
        """Determine if Hero will be in position post-flop against villain"""
        # Position order (later positions act after earlier ones post-flop)
        position_order = {
            "SB": 0, "BB": 1,  # Blinds act first post-flop
            "UTG": 2, "UTG+1": 3,
            "MP": 4, "MP+1": 5, "HJ": 6,
            "CO": 7, "BTN": 8  # BTN acts last post-flop
        }
        
        hero_order = position_order.get(hero_pos, -1)
        villain_order = position_order.get(villain_pos, -1)
        
        # Hero is IP if acting after villain post-flop (higher order)
        return hero_order > villain_order
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """
        Get formatted statistics summary
        """
        formatted_stats = {}
        
        for stat_name, counts in self.stats.items():
            if counts["opportunities"] > 0:
                percentage = (counts["attempts"] / counts["opportunities"]) * 100
                formatted_stats[stat_name] = {
                    "opportunities": counts["opportunities"],
                    "attempts": counts["attempts"],
                    "percentage": round(percentage, 1)
                }
        
        return formatted_stats
    
    def get_exclusion_counts(self) -> tuple:
        """
        Return exclusion counts for compatibility with pipeline.
        Since we now validate stat-by-stat instead of excluding entire hands,
        we always return (0, 0) for low_effective_stack and allin_contextual.
        """
        return (0, 0)
