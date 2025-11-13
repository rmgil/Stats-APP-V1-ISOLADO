#!/usr/bin/env python3
"""Test validation with audit logging for 888poker and 888.pt"""

import sys
from pathlib import Path

# Add workspace to path (like wsgi.py does)
sys.path.insert(0, str(Path(__file__).parent))

import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

from app.parse.site_parsers.site_detector import detect_poker_site
from app.stats.preflop_stats import PreflopStats

def test_file(filepath, label):
    """Test a single file with audit logging."""
    print('\n' + '='*80)
    print(f'{label} VALIDATION AUDIT')
    print('='*80)
    print(f'File: {filepath}\n')
    
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    site = detect_poker_site(content)
    print(f'Site detected: {site}\n')
    
    hands = content.split('\n\n\n')
    valid_hands = [h for h in hands if h.strip() and len(h) > 100]
    print(f'Total hands: {len(valid_hands)}\n')
    
    print('='*80)
    print('PROCESSING FIRST 5 HANDS:')
    print('='*80)
    
    for i, hand_text in enumerate(valid_hands[:5], 1):
        print(f'\n--- HAND #{i} ---')
        stats_calc = PreflopStats()
        stats_calc.analyze_hand(hand_text)
        stats = stats_calc.get_stats_summary()
        
        opps = []
        for stat_name, stat_data in stats.items():
            if stat_data.get('opportunities', 0) > 0:
                opps.append(f'{stat_name}={stat_data["opportunities"]}')
        
        if opps:
            print(f'✅ Opportunities: {", ".join(opps)}')
        else:
            print('⚠️  No opportunities counted')
    
    # Summary
    print(f'\n{"="*80}')
    print('SUMMARY - ALL HANDS:')
    print('='*80)
    
    total_calc = PreflopStats()
    for hand_text in valid_hands:
        try:
            total_calc.analyze_hand(hand_text)
        except Exception as e:
            print(f'⚠️  Hand parse error: {e}')
    
    final_stats = total_calc.get_stats_summary()
    
    print(f'\nTotal opportunities found:')
    for stat_name, stat_data in sorted(final_stats.items()):
        if stat_data.get('opportunities', 0) > 0:
            print(f'  {stat_name}: {stat_data["opportunities"]}')

if __name__ == '__main__':
    import glob
    
    # Find test files
    poker_files = glob.glob('attached_assets/888poker*PKO*.txt')
    pt_files = glob.glob('attached_assets/888.pt*.txt')
    
    if poker_files:
        test_file(poker_files[0], '888POKER PKO')
    else:
        print("⚠️ No 888poker files found")
    
    if pt_files:
        test_file(pt_files[0], '888.PT MYSTERY BOUNTY')
    else:
        print("⚠️ No 888.pt files found")
