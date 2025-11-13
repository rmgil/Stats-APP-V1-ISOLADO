"""
Tests for Phase 3 derivation: positions, preflop, IP/OOP, and stacks.
"""
import pytest
from app.parse.schemas import Hand, StreetInfo, Action, Player
from app.derive.positions import assign_positions, group_buckets
from app.derive.preflop import (
    is_unopened_pot, has_limper_before, first_raiser,
    actor_is_first_raiser, who_3bet_4bet, hero_faced_3bet,
    detect_squeeze, detect_resteal_vs_btn, classify_pot_type,
    detect_freeplay_bb, hero_vpip, limper_exists
)
from app.derive.ip import derive_ip
from app.derive.stacks import eff_stack_bb_srp, eff_stack_bb_vs_3bettor


class TestPositions:
    def test_6max_5handed(self):
        """Test 6-max 5-handed: MP, CO, BTN, SB, BB."""
        hand = Hand(
            site='pokerstars',
            file_id='test.txt',
            button_seat=3,
            table_max=6,
            players=[
                Player(seat=1, name='MP', stack_chips=100),
                Player(seat=2, name='CO', stack_chips=100),
                Player(seat=3, name='BTN', stack_chips=100),
                Player(seat=4, name='SB', stack_chips=100),
                Player(seat=5, name='BB', stack_chips=100),
            ],
            streets={'preflop': StreetInfo(actions=[])}
        )
        
        positions = assign_positions(hand)
        groups = group_buckets(positions, 6)
        
        assert positions['MP'] == 'MP'
        assert positions['CO'] == 'CO'
        assert positions['BTN'] == 'BTN'
        assert positions['SB'] == 'SB'
        assert positions['BB'] == 'BB'
        
        assert groups['MP'] == 'MP'
        assert groups['CO'] == 'LP'
        assert groups['BTN'] == 'LP'
        assert groups['SB'] == 'BLINDS'
        assert groups['BB'] == 'BLINDS'
    
    def test_9max_8handed(self):
        """Test 9-max 8-handed: EP2 removed, CO/BTN/SB/BB preserved."""
        hand = Hand(
            site='pokerstars',
            file_id='test.txt',
            button_seat=6,
            table_max=9,
            players=[
                Player(seat=1, name='EP', stack_chips=100),  # EP3
                Player(seat=2, name='MP1', stack_chips=100),
                Player(seat=3, name='MP2', stack_chips=100),
                Player(seat=4, name='CO', stack_chips=100),
                Player(seat=5, name='BTN_PLAYER', stack_chips=100),
                Player(seat=6, name='BTN', stack_chips=100),  # Actual BTN
                Player(seat=7, name='SB', stack_chips=100),
                Player(seat=8, name='BB', stack_chips=100),
            ],
            streets={'preflop': StreetInfo(actions=[])}
        )
        
        positions = assign_positions(hand)
        groups = group_buckets(positions, 9)
        
        # With 8-handed, EP2 is removed (leftmost position)
        assert positions['BTN'] == 'BTN'
        assert positions['SB'] == 'SB'
        assert positions['BB'] == 'BB'
        assert 'CO' in positions.values()
        
        # Groups
        assert groups['BTN'] == 'LP'
        assert groups['SB'] == 'BLINDS'
        assert groups['BB'] == 'BLINDS'


class TestPreflop:
    def test_rfi_vs_iso(self):
        """Test RFI (no limpers) vs ISO (with limpers)."""
        # RFI: No limpers before hero raise
        actions_rfi = [
            Action(type='POST_SB', actor='SB', amount=1),
            Action(type='POST_BB', actor='BB', amount=2),
            Action(type='FOLD', actor='UTG'),
            Action(type='RAISE', actor='Hero', amount=6),
        ]
        
        assert is_unopened_pot(actions_rfi, until_actor='Hero') == True
        assert limper_exists(actions_rfi) == False
        assert actor_is_first_raiser(actions_rfi, 'Hero') == True
        
        # ISO: Limpers before hero raise
        actions_iso = [
            Action(type='POST_SB', actor='SB', amount=1),
            Action(type='POST_BB', actor='BB', amount=2),
            Action(type='CALL', actor='UTG', amount=2),  # Limper
            Action(type='RAISE', actor='Hero', amount=8),  # ISO
        ]
        
        assert limper_exists(actions_iso) == True
        assert actor_is_first_raiser(actions_iso, 'Hero') == True
        # Hero is isolating the limper
        
    def test_3bet_4bet(self):
        """Test 3-bet and 4-bet detection."""
        actions = [
            Action(type='POST_SB', actor='SB', amount=1),
            Action(type='POST_BB', actor='BB', amount=2),
            Action(type='RAISE', actor='UTG', amount=6),  # Open
            Action(type='RERAISE', actor='MP', amount=18),  # 3-bet
            Action(type='RERAISE', actor='CO', amount=45),  # 4-bet
        ]
        
        three_bettor, four_bettor = who_3bet_4bet(actions)
        assert three_bettor == 'MP'
        assert four_bettor == 'CO'
        
        pot_type = classify_pot_type(actions)
        assert pot_type == '4bet'
    
    def test_squeeze(self):
        """Test squeeze: raise + call + 3bet from hero."""
        hand = Hand(
            site='pokerstars',
            file_id='test.txt',
            button_seat=3,
            hero='Hero',
            players=[
                Player(seat=1, name='UTG', stack_chips=100),
                Player(seat=2, name='MP', stack_chips=100),
                Player(seat=3, name='BTN', stack_chips=100),
                Player(seat=4, name='Hero', stack_chips=100),  # SB
                Player(seat=5, name='BB', stack_chips=100),
            ],
            streets={'preflop': StreetInfo(actions=[])}
        )
        
        actions = [
            Action(type='POST_SB', actor='Hero', amount=1),
            Action(type='POST_BB', actor='BB', amount=2),
            Action(type='RAISE', actor='UTG', amount=6),  # Open
            Action(type='CALL', actor='MP', amount=6),  # Caller
            Action(type='FOLD', actor='BTN'),
            Action(type='RERAISE', actor='Hero', amount=24),  # Squeeze
        ]
        
        is_squeeze = detect_squeeze(actions, 'Hero')
        assert is_squeeze == True
    
    def test_resteal_vs_btn(self):
        """Test resteal vs BTN: BTN opens, hero in blinds 3-bets."""
        hand = Hand(
            site='pokerstars',
            file_id='test.txt',
            button_seat=1,
            hero='Hero',
            players=[
                Player(seat=1, name='BTN', stack_chips=100),
                Player(seat=2, name='Hero', stack_chips=100),  # SB
                Player(seat=3, name='BB', stack_chips=100),
            ],
            streets={'preflop': StreetInfo(actions=[])}
        )
        
        actions = [
            Action(type='POST_SB', actor='Hero', amount=1),
            Action(type='POST_BB', actor='BB', amount=2),
            Action(type='RAISE', actor='BTN', amount=5),  # BTN steal
            Action(type='RERAISE', actor='Hero', amount=15),  # Resteal
        ]
        
        positions = assign_positions(hand)
        opener = first_raiser(actions)
        is_resteal = detect_resteal_vs_btn(actions, 'Hero', hand, opener)
        
        assert opener == 'BTN'
        assert positions[opener] == 'BTN'
        assert positions['Hero'] == 'SB'
        assert is_resteal == True


class TestIPandHU:
    def test_hu_flop_and_ip(self):
        """Test heads-up flop detection and IP/OOP."""
        # Hero BTN (IP) vs BB (OOP)
        hand_ip = Hand(
            site='pokerstars',
            file_id='test.txt',
            button_seat=1,
            hero='Hero',
            players=[
                Player(seat=1, name='Hero', stack_chips=100),  # BTN
                Player(seat=2, name='SB', stack_chips=100),
                Player(seat=3, name='BB', stack_chips=100),
            ],
            streets={
                'preflop': StreetInfo(actions=[
                    Action(type='POST_SB', actor='SB', amount=1),
                    Action(type='POST_BB', actor='BB', amount=2),
                    Action(type='RAISE', actor='Hero', amount=6),
                    Action(type='FOLD', actor='SB'),
                    Action(type='CALL', actor='BB', amount=4),
                ]),
                'flop': StreetInfo(actions=[
                    Action(type='CHECK', actor='BB'),  # BB acts first (OOP)
                    Action(type='BET', actor='Hero', amount=8),  # Hero acts second (IP)
                ]),
                'turn': StreetInfo(actions=[]),
                'river': StreetInfo(actions=[])
            }
        )
        
        result = derive_ip(hand_ip)
        assert result['heads_up_flop'] == True
        assert result['hero_ip_flop'] == True  # Hero is in position
        assert result['players_to_flop'] == 2
        
        # Hero BB (OOP) vs BTN (IP)
        hand_oop = Hand(
            site='pokerstars',
            file_id='test.txt',
            button_seat=1,
            hero='Hero',
            players=[
                Player(seat=1, name='BTN', stack_chips=100),
                Player(seat=2, name='SB', stack_chips=100),
                Player(seat=3, name='Hero', stack_chips=100),  # BB
            ],
            streets={
                'preflop': StreetInfo(actions=[
                    Action(type='POST_SB', actor='SB', amount=1),
                    Action(type='POST_BB', actor='Hero', amount=2),
                    Action(type='RAISE', actor='BTN', amount=6),
                    Action(type='FOLD', actor='SB'),
                    Action(type='CALL', actor='Hero', amount=4),
                ]),
                'flop': StreetInfo(actions=[
                    Action(type='CHECK', actor='Hero'),  # Hero acts first (OOP)
                    Action(type='BET', actor='BTN', amount=8),
                ]),
                'turn': StreetInfo(actions=[]),
                'river': StreetInfo(actions=[])
            }
        )
        
        result = derive_ip(hand_oop)
        assert result['heads_up_flop'] == True
        assert result['hero_ip_flop'] == False  # Hero is out of position
        assert result['players_to_flop'] == 2
    
    def test_multiway(self):
        """Test multiway pot detection."""
        hand = Hand(
            site='pokerstars',
            file_id='test.txt',
            button_seat=1,
            hero='Hero',
            players=[
                Player(seat=1, name='BTN', stack_chips=100),
                Player(seat=2, name='SB', stack_chips=100),
                Player(seat=3, name='BB', stack_chips=100),
                Player(seat=4, name='Hero', stack_chips=100),
            ],
            streets={
                'preflop': StreetInfo(actions=[
                    Action(type='POST_SB', actor='SB', amount=1),
                    Action(type='POST_BB', actor='BB', amount=2),
                    Action(type='RAISE', actor='Hero', amount=6),
                    Action(type='CALL', actor='BTN', amount=6),
                    Action(type='CALL', actor='SB', amount=5),
                    Action(type='FOLD', actor='BB'),
                ]),
                'flop': StreetInfo(actions=[]),
                'turn': StreetInfo(actions=[]),
                'river': StreetInfo(actions=[])
            }
        )
        
        result = derive_ip(hand)
        assert result['heads_up_flop'] == False
        assert result['players_to_flop'] == 3
        assert result['hero_ip_flop'] == None  # Not HU, so no IP/OOP


class TestEffectiveStacks:
    def test_eff_stack_srp(self):
        """Test effective stack calculation for SRP."""
        hand = Hand(
            site='pokerstars',
            file_id='test.txt',
            hero='Hero',
            blinds={'sb': 0.5, 'bb': 1},
            players=[
                Player(seat=1, name='Hero', stack_chips=100),  # 100 BB
                Player(seat=2, name='Villain', stack_chips=50),  # 50 BB
            ],
            streets={
                'preflop': StreetInfo(actions=[
                    Action(type='POST_SB', actor='Hero', amount=0.5),
                    Action(type='POST_BB', actor='Villain', amount=1),
                    Action(type='RAISE', actor='Hero', amount=3),
                    Action(type='CALL', actor='Villain', amount=2),
                ]),
                'flop': StreetInfo(actions=[]),
                'turn': StreetInfo(actions=[]),
                'river': StreetInfo(actions=[])
            }
        )
        
        eff_stack = eff_stack_bb_srp(hand, 'Hero')
        assert eff_stack == 50.0  # Min of 100 and 50 BB
    
    def test_eff_stack_vs_3bettor(self):
        """Test effective stack vs 3-bettor."""
        hand = Hand(
            site='pokerstars',
            file_id='test.txt',
            hero='Hero',
            blinds={'sb': 1, 'bb': 2},
            players=[
                Player(seat=1, name='Hero', stack_chips=200),  # 100 BB
                Player(seat=2, name='Villain', stack_chips=160),  # 80 BB
            ],
            streets={
                'preflop': StreetInfo(actions=[
                    Action(type='POST_SB', actor='Hero', amount=1),
                    Action(type='POST_BB', actor='Villain', amount=2),
                    Action(type='RAISE', actor='Hero', amount=6),
                    Action(type='RERAISE', actor='Villain', amount=18),  # 3-bet
                ]),
                'flop': StreetInfo(actions=[]),
                'turn': StreetInfo(actions=[]),
                'river': StreetInfo(actions=[])
            }
        )
        
        eff_stack = eff_stack_bb_vs_3bettor(hand, 'Hero', 'Villain')
        assert eff_stack == 80.0  # Min of 100 and 80 BB


def test_integration():
    """Test full integration of all modules."""
    from app.derive.runner import enrich_hands
    import tempfile
    import json
    import os
    
    # Create test hand
    hand_data = {
        'site': 'pokerstars',
        'file_id': 'test.txt',
        'tournament_id': '123',
        'button_seat': 3,
        'hero': 'Hero',
        'table_max': 6,
        'blinds': {'sb': 10, 'bb': 20},
        'players': [
            {'seat': 1, 'name': 'UTG', 'stack_chips': 1000},
            {'seat': 2, 'name': 'MP', 'stack_chips': 1500},
            {'seat': 3, 'name': 'Hero', 'stack_chips': 2000},  # BTN
            {'seat': 4, 'name': 'SB', 'stack_chips': 1000},
            {'seat': 5, 'name': 'BB', 'stack_chips': 800}
        ],
        'players_dealt_in': ['UTG', 'MP', 'Hero', 'SB', 'BB'],
        'streets': {
            'preflop': {
                'actions': [
                    {'type': 'POST_SB', 'actor': 'SB', 'amount': 10},
                    {'type': 'POST_BB', 'actor': 'BB', 'amount': 20},
                    {'type': 'FOLD', 'actor': 'UTG'},
                    {'type': 'CALL', 'actor': 'MP', 'amount': 20},  # Limp
                    {'type': 'RAISE', 'actor': 'Hero', 'amount': 80},  # ISO
                    {'type': 'RERAISE', 'actor': 'SB', 'amount': 240},  # 3-bet
                    {'type': 'FOLD', 'actor': 'BB'},
                    {'type': 'FOLD', 'actor': 'MP'},
                    {'type': 'CALL', 'actor': 'Hero', 'amount': 160}
                ]
            },
            'flop': {
                'actions': [
                    {'type': 'CHECK', 'actor': 'SB'},
                    {'type': 'BET', 'actor': 'Hero', 'amount': 300}
                ]
            },
            'turn': {'actions': []},
            'river': {'actions': []}
        }
    }
    
    # Write to temp file
    temp_dir = tempfile.mkdtemp()
    in_file = os.path.join(temp_dir, 'test.jsonl')
    out_file = os.path.join(temp_dir, 'enriched.jsonl')
    
    with open(in_file, 'w') as f:
        f.write(json.dumps(hand_data) + '\n')
    
    # Run enrichment
    result = enrich_hands(in_file, out_file)
    assert result['hands'] == 1
    
    # Check enriched data
    with open(out_file, 'r') as f:
        enriched = json.loads(f.readline())
    
    derived = enriched['derived']
    
    # Check positions
    assert derived['positions']['abs_positions']['Hero'] == 'BTN'
    assert derived['positions']['pos_group']['Hero'] == 'LP'
    
    # Check preflop
    assert derived['preflop']['hero_position'] == 'BTN'
    assert derived['preflop']['is_isoraiser'] == True  # There was a limper
    assert derived['preflop']['three_bettor'] == 'SB'
    assert derived['preflop']['faced_3bet'] == True
    assert derived['preflop']['pot_type'] == '3bet'
    
    # Check IP
    assert derived['ip']['heads_up_flop'] == True
    assert derived['ip']['hero_ip_flop'] == True  # Hero BTN vs SB
    
    # Check stacks
    assert derived['stacks']['eff_stack_bb_srp'] == 50.0  # 1000/20
    assert derived['stacks']['eff_stack_bb_vs_3bettor'] == 50.0  # Min(2000,1000)/20
    
    print("✅ All integration tests passed!")