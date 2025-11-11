"""
Hand collector to organize hands by statistics.
Saves hands that are opportunities for specific stats into separate files.
"""
import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set
from collections import defaultdict

logger = logging.getLogger(__name__)

class HandCollector:
    """Collects and saves hands organized by statistic opportunities."""
    
    # Map stat names to clean filenames (class attribute for use by aggregator)
    stat_filenames = {
            # RFI
            "Early RFI": "rfi_early.txt",
            "Middle RFI": "rfi_middle.txt",
            "CO Steal": "rfi_co_steal.txt",
            "BTN Steal": "rfi_btn_steal.txt",
            
            # BvB
            "SB UO VPIP": "bvb_sb_uo_vpip.txt",
            "BB fold vs SB steal": "bvb_bb_fold_sb_steal.txt",
            "BB raise vs SB limp UOP": "bvb_bb_raise_sb_limp.txt",
            "SB Steal": "bvb_sb_steal.txt",
            
            # 3bet/CC
            "EP 3bet": "3bet_ep.txt",
            "EP Cold Call": "cc_ep.txt",
            "MP 3bet": "3bet_mp.txt",
            "MP Cold Call": "cc_mp.txt",
            "CO 3bet": "3bet_co.txt",
            "CO Cold Call": "cc_co.txt",
            "BTN 3bet": "3bet_btn.txt",
            "BTN Cold Call": "cc_btn.txt",
            "BTN fold to CO steal": "btn_fold_co_steal.txt",
            
            # vs 3bet
            "Fold to 3bet IP": "vs_3bet_fold_ip.txt",
            "Fold to 3bet OOP": "vs_3bet_fold_oop.txt",
            
            # Squeeze
            "Squeeze": "squeeze.txt",
            "Squeeze vs BTN Raiser": "squeeze_vs_btn.txt",
            
            # BB Defense
            "BB fold vs CO steal": "bb_defense_fold_co.txt",
            "BB fold vs BTN steal": "bb_defense_fold_btn.txt",
            "BB resteal vs BTN steal": "bb_defense_resteal_btn.txt",
            
            # SB Defense
            "SB fold to CO Steal": "sb_defense_fold_co.txt",
            "SB fold to BTN Steal": "sb_defense_fold_btn.txt",
            "SB resteal vs BTN": "sb_defense_resteal_btn.txt",
            
            # Postflop Stats (exact names from PostflopCalculator)
            "Flop CBet IP %": "postflop_flop_cbet_ip.txt",
            "Flop CBet 3BetPot IP": "postflop_flop_cbet_3betpot_ip.txt",
            "Flop CBet OOP%": "postflop_flop_cbet_oop.txt",
            "Flop fold vs Cbet IP": "postflop_flop_fold_cbet_ip.txt",
            "Flop raise Cbet IP": "postflop_flop_raise_cbet_ip.txt",
            "Flop raise Cbet OOP": "postflop_flop_raise_cbet_oop.txt",
            "Fold vs Check Raise": "postflop_fold_vs_check_raise.txt",
            "Flop bet vs missed Cbet SRP": "postflop_flop_bet_missed_srp.txt",
            "Turn CBet IP%": "postflop_turn_cbet_ip.txt",
            "Turn Cbet OOP%": "postflop_turn_cbet_oop.txt",
            "Turn donk bet": "postflop_turn_donk.txt",
            "Turn donk bet SRP vs PFR": "postflop_turn_donk_srp.txt",
            "Bet turn vs Missed Flop Cbet OOP SRP": "postflop_bet_turn_missed_oop.txt",
            "Turn Fold vs CBet OOP": "postflop_turn_fold_cbet_oop.txt",
            "WTSD%": "postflop_wtsd.txt",
            "W$SD%": "postflop_wssd.txt",
            "W$WSF Rating": "postflop_wwsf.txt",
            "River Agg %": "postflop_river_agg.txt",
            "River bet - Single Rsd Pot": "postflop_river_bet_single.txt",
            "W$SD% B River": "postflop_wssd_b_river.txt",
        }
    
    def __init__(self, work_dir: str):
        """
        Initialize hand collector.
        
        Args:
            work_dir: Working directory for this processing session (including format subdirectory)
        """
        self.work_dir = work_dir
        # The work_dir already contains the full path including format subdirectory
        self.stats_dir = work_dir
        os.makedirs(self.stats_dir, exist_ok=True)
        
        # Track hands by stat
        self.hands_by_stat = defaultdict(list)
        
        # Track metadata
        self.metadata = {
            "total_hands_analyzed": 0,
            "hands_per_stat": {},
            "stat_descriptions": {
                "Early RFI": "Oportunidades de RFI nas posições iniciais (UTG, UTG+1)",
                "Middle RFI": "Oportunidades de RFI nas posições médias (MP, HJ)",
                "CO Steal": "Oportunidades de steal do Cutoff",
                "BTN Steal": "Oportunidades de steal do Button",
                "SB UO VPIP": "Situações onde SB pode agir sem raises anteriores",
                "BB fold vs SB steal": "BB enfrenta steal da Small Blind",
                "BB raise vs SB limp UOP": "BB pode isolar limp da SB",
                "SB Steal": "SB pode fazer steal contra BB",
                "EP 3bet": "Oportunidades de 3bet em Early Position",
                "EP Cold Call": "Oportunidades de Cold Call em Early Position",
                "MP 3bet": "Oportunidades de 3bet em Middle Position",
                "MP Cold Call": "Oportunidades de Cold Call em Middle Position",
                "CO 3bet": "Oportunidades de 3bet no Cutoff",
                "CO Cold Call": "Oportunidades de Cold Call no Cutoff",
                "BTN 3bet": "Oportunidades de 3bet no Button",
                "BTN Cold Call": "Oportunidades de Cold Call no Button",
                "BTN fold to CO steal": "Button enfrenta steal do Cutoff",
                "Fold to 3bet IP": "Enfrenta 3bet em posição",
                "Fold to 3bet OOP": "Enfrenta 3bet fora de posição",
                "Squeeze": "Oportunidades de squeeze (raise após call de raise)",
                "Squeeze vs BTN Raiser": "Squeeze quando o raiser original é o Button",
                "BB fold vs CO steal": "BB enfrenta steal do Cutoff",
                "BB fold vs BTN steal": "BB enfrenta steal do Button",
                "BB resteal vs BTN steal": "BB pode fazer resteal contra Button",
                "SB fold to CO Steal": "SB enfrenta steal do Cutoff",
                "SB fold to BTN Steal": "SB enfrenta steal do Button",
                "SB resteal vs BTN": "SB pode fazer resteal contra Button",
                "Flop CBet IP %": "Oportunidades de CBet no flop em posição",
                "Flop CBet OOP %": "Oportunidades de CBet no flop fora de posição",
                "Flop Fold to CBet IP %": "Enfrenta CBet no flop em posição",
                "Flop Fold to CBet OOP %": "Enfrenta CBet no flop fora de posição",
                "Flop Bet vs Missed CBet IP %": "Oportunidades de bet quando adversário não fez CBet no flop (IP)",
                "Flop Bet vs Missed CBet OOP %": "Oportunidades de bet quando adversário não fez CBet no flop (OOP)",
                "Turn CBet IP %": "Oportunidades de CBet no turn em posição",
                "Turn CBet OOP %": "Oportunidades de CBet no turn fora de posição",
                "Turn Fold to CBet IP %": "Enfrenta CBet no turn em posição",
                "Turn Fold to CBet OOP %": "Enfrenta CBet no turn fora de posição",
                "Turn Bet vs Missed CBet IP %": "Oportunidades de bet quando adversário não fez CBet no turn (IP)",
                "Turn Bet vs Missed CBet OOP %": "Oportunidades de bet quando adversário não fez CBet no turn (OOP)"
            }
        }
    
    def add_hand(self, stat_name: str, hand_text: str, hand_id: Optional[str] = None):
        """
        Add a hand to a specific stat collection.
        
        Args:
            stat_name: Name of the statistic
            hand_text: Full text of the hand
            hand_id: Optional unique identifier for the hand
        """
        if stat_name not in HandCollector.stat_filenames:
            logger.warning(f"Unknown stat: {stat_name}")
            return
        
        # Add hand to collection
        hand_entry = {
            "text": hand_text,
            "id": hand_id or f"hand_{len(self.hands_by_stat[stat_name])}"
        }
        self.hands_by_stat[stat_name].append(hand_entry)
    
    def save_all(self):
        """Save all collected hands to their respective files."""
        saved_stats = []
        
        for stat_name, hands in self.hands_by_stat.items():
            if not hands:
                continue
            
            filename = HandCollector.stat_filenames.get(stat_name)
            if not filename:
                continue
            
            filepath = os.path.join(self.stats_dir, filename)
            
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    # Write each hand exactly as it was, separated by double newlines
                    for i, hand_entry in enumerate(hands):
                        # Write the hand text exactly as received
                        hand_text = hand_entry['text'].strip()
                        f.write(hand_text)
                        
                        # Add double newline separator between hands
                        # (but not after the last hand)
                        if i < len(hands) - 1:
                            f.write("\n\n\n")  # 3 newlines = 2 blank lines between hands
                        else:
                            f.write("\n")  # Just one newline at the end of file
                
                # Update metadata
                self.metadata['hands_per_stat'][stat_name] = len(hands)
                
                # Save hand IDs for this stat
                if 'hand_ids' not in self.metadata:
                    self.metadata['hand_ids'] = {}
                self.metadata['hand_ids'][stat_name] = [h['id'] for h in hands]
                
                saved_stats.append(stat_name)
                
                logger.info(f"Saved {len(hands)} hands for {stat_name} to {filename}")
                
            except Exception as e:
                logger.error(f"Error saving hands for {stat_name}: {e}")
        
        # Save metadata
        metadata_path = os.path.join(self.stats_dir, "metadata.json")
        try:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved metadata to {metadata_path}")
        except Exception as e:
            logger.error(f"Error saving metadata: {e}")
        
        return saved_stats
    
    def get_stats_with_hands(self) -> List[Dict]:
        """
        Get list of stats that have collected hands.
        
        Returns:
            List of dicts with stat info
        """
        stats_info = []
        
        for stat_name, hands in self.hands_by_stat.items():
            if hands:
                filename = HandCollector.stat_filenames.get(stat_name)
                if filename:
                    stats_info.append({
                        "name": stat_name,
                        "filename": filename,
                        "count": len(hands),
                        "description": self.metadata['stat_descriptions'].get(stat_name, '')
                    })
        
        return sorted(stats_info, key=lambda x: x['name'])
    
    def get_hands_by_stat(self) -> Dict[str, List[str]]:
        """
        Get the collected hand texts organized by stat name.
        
        Returns:
            Dictionary mapping stat names to list of hand texts
        """
        result = {}
        for stat_name, hands in self.hands_by_stat.items():
            if hands:
                # Extract just the text from each hand entry
                result[stat_name] = [hand_entry['text'] for hand_entry in hands]
        return result