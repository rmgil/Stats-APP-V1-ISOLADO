"""
Tests for Stats RFI functionality - components and integration.
"""
import json
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from app.stats.engine import build_context, eval_clause, pass_filters, run_stats
from app.stats.dsl import load_catalog, parse_stat_definition


class TestStatsComponents(unittest.TestCase):
    """Test individual components of the stats engine."""
    
    def test_build_context_complete(self):
        """Test building context from enriched hand."""
        hand = {
            'hero': 'TestHero',
            'timestamp_utc': '2025-06-15T10:00:00Z',
            'file_id': 'non-ko/test.txt',
            'site': 'pokerstars',
            'tournament_id': '123',
            'button_seat': 3,
            'players': [{'name': 'TestHero'}],
            'derived': {
                'positions': {
                    'pos_group': {'TestHero': 'EP'},
                    'abs_positions': {'TestHero': 'EP1'}
                },
                'preflop': {
                    'unopened_pot': True,
                    'hero_raised_first_in': True,
                    'pot_type': 'SRP'
                },
                'ip': {
                    'heads_up_flop': True,
                    'players_to_flop': 2
                },
                'stacks': {
                    'eff_stack_bb_srp': 25.5
                }
            }
        }
        
        ctx = build_context(hand)
        
        # Verify hero position data
        self.assertEqual(ctx['hero_pos_group'], 'EP')
        self.assertEqual(ctx['hero_position'], 'EP1')
        
        # Verify preflop data
        self.assertTrue(ctx['unopened_pot'])
        self.assertTrue(ctx['hero_raised_first_in'])
        self.assertEqual(ctx['pot_type'], 'SRP')
        
        # Verify IP data
        self.assertTrue(ctx['heads_up_flop'])
        self.assertEqual(ctx['players_to_flop'], 2)
        
        # Verify stacks
        self.assertEqual(ctx['eff_stack_srp'], 25.5)
        
        # Verify meta
        self.assertEqual(ctx['month'], '2025-06')
        self.assertEqual(len(ctx['hand_id']), 16)  # 16-char hash
    
    def test_eval_clause_operators(self):
        """Test all eval_clause operators."""
        ctx = {
            'hero_pos_group': 'EP',
            'unopened_pot': True,
            'eff_stack_srp': 20,
            'pot_type': 'SRP'
        }
        
        # Test eq operator
        self.assertTrue(eval_clause({'eq': ['hero_pos_group', 'EP']}, ctx))
        self.assertFalse(eval_clause({'eq': ['hero_pos_group', 'MP']}, ctx))
        
        # Test is_true operator
        self.assertTrue(eval_clause({'is_true': 'unopened_pot'}, ctx))
        self.assertFalse(eval_clause({'is_true': 'faced_3bet'}, ctx))
        
        # Test gte operator
        self.assertTrue(eval_clause({'gte': ['eff_stack_srp', 16]}, ctx))
        self.assertFalse(eval_clause({'gte': ['eff_stack_srp', 25]}, ctx))
        
        # Test in operator
        self.assertTrue(eval_clause({'in': ['pot_type', ['SRP', '3BP']]}, ctx))
        self.assertFalse(eval_clause({'in': ['pot_type', ['4BP', '5BP']]}, ctx))
        
        # Test all operator (list of conditions)
        self.assertTrue(eval_clause({
            'all': [
                {'eq': ['hero_pos_group', 'EP']},
                {'is_true': 'unopened_pot'}
            ]
        }, ctx))
        
        # Test any operator
        self.assertTrue(eval_clause({
            'any': [
                {'eq': ['hero_pos_group', 'MP']},  # False
                {'is_true': 'unopened_pot'}  # True
            ]
        }, ctx))
    
    def test_pass_filters(self):
        """Test filter application."""
        stat = {
            'filters': {
                'heads_up_only': True,
                'pot_type': ['SRP'],
                'eff_stack_min_bb': 16,
                'exclude_allin_preflop': True
            }
        }
        
        # Valid context - passes all filters
        ctx_valid = {
            'heads_up_flop': True,
            'pot_type': 'SRP',
            'eff_stack_srp': 20,
            'any_allin_preflop': False
        }
        self.assertTrue(pass_filters(stat, ctx_valid))
        
        # Not heads up - fails
        ctx_mw = ctx_valid.copy()
        ctx_mw['heads_up_flop'] = False
        self.assertFalse(pass_filters(stat, ctx_mw))
        
        # Wrong pot type - fails
        ctx_3bet = ctx_valid.copy()
        ctx_3bet['pot_type'] = '3BP'
        self.assertFalse(pass_filters(stat, ctx_3bet))
        
        # Stack too small - fails
        ctx_short = ctx_valid.copy()
        ctx_short['eff_stack_srp'] = 12
        self.assertFalse(pass_filters(stat, ctx_short))
        
        # All-in preflop - fails
        ctx_allin = ctx_valid.copy()
        ctx_allin['any_allin_preflop'] = True
        self.assertFalse(pass_filters(stat, ctx_allin))
    
    def test_dsl_catalog_loading(self):
        """Test loading and parsing DSL catalog."""
        catalog = load_catalog()
        
        # Verify catalog structure
        self.assertIn('version', catalog)
        self.assertIn('stats', catalog)
        self.assertIsInstance(catalog['stats'], list)
        
        # Find RFI stats
        rfi_stats = [s for s in catalog['stats'] if s.get('family') == 'RFI']
        self.assertEqual(len(rfi_stats), 4)  # RFI_EARLY, RFI_MIDDLE, RFI_CO_STEAL, RFI_BTN_STEAL
        
        # Parse and validate RFI_EARLY
        rfi_early = next(s for s in rfi_stats if s['id'] == 'RFI_EARLY')
        parsed = parse_stat_definition(rfi_early)
        
        self.assertEqual(parsed['id'], 'RFI_EARLY')
        self.assertEqual(parsed['label'], 'Early RFI')
        self.assertEqual(parsed['scope'], 'preflop')
        self.assertIn('nonko_9max_pref', parsed['applies_to_groups'])
        self.assertTrue(parsed['filters']['heads_up_only'])
        self.assertEqual(parsed['filters']['eff_stack_min_bb'], 16)


class TestStatsIntegration(unittest.TestCase):
    """Test full integration of stats calculation."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_jsonl = os.path.join(self.temp_dir, 'test_hands.jsonl')
        self.out_dir = os.path.join(self.temp_dir, 'stats_output')
        self.dsl_path = 'app/stats/dsl/stats.yml'
    
    def test_rfi_calculation_with_mock(self):
        """Test RFI calculation with mocked groups_for_hand."""
        # Create test hands: 2 opportunities, 1 attempt
        test_hands = [
            # Opportunity 1: RFI successful (attempt)
            {
                'hero': 'TestHero',
                'timestamp_utc': '2025-06-15T10:00:00Z',
                'site': 'pokerstars',
                'tournament_id': '001',
                'file_id': 'non-ko/test1.txt',
                'table_max': 9,
                'button_seat': 5,
                'players': [{'name': 'TestHero'}, {'name': 'Villain'}],
                'derived': {
                    'positions': {
                        'table_max_resolved': 9,
                        'pos_group': {'TestHero': 'EP'},
                        'abs_positions': {'TestHero': 'EP1'}
                    },
                    'preflop': {
                        'unopened_pot': True,
                        'hero_raised_first_in': True,  # Attempt
                        'pot_type': 'SRP'
                    },
                    'ip': {
                        'heads_up_flop': True,
                        'players_to_flop': 2
                    },
                    'stacks': {
                        'eff_stack_bb_srp': 25.0
                    }
                }
            },
            # Opportunity 2: RFI fold (no attempt)
            {
                'hero': 'TestHero',
                'timestamp_utc': '2025-06-20T14:00:00Z',
                'site': 'pokerstars',
                'tournament_id': '002',
                'file_id': 'non-ko/test2.txt',
                'table_max': 9,
                'button_seat': 3,
                'players': [{'name': 'TestHero'}, {'name': 'Villain2'}],
                'derived': {
                    'positions': {
                        'table_max_resolved': 9,
                        'pos_group': {'TestHero': 'EP'},
                        'abs_positions': {'TestHero': 'EP2'}
                    },
                    'preflop': {
                        'unopened_pot': True,
                        'hero_raised_first_in': False,  # No attempt (fold)
                        'pot_type': 'SRP'  # Still SRP context even if hero folded
                    },
                    'ip': {
                        'heads_up_flop': True,
                        'players_to_flop': 2  # Simulating that others played
                    },
                    'stacks': {
                        'eff_stack_bb_srp': 20.0
                    }
                }
            }
        ]
        
        # Write test data
        with open(self.test_jsonl, 'w') as f:
            for hand in test_hands:
                f.write(json.dumps(hand) + '\n')
        
        # Mock groups_for_hand to always return ["nonko_9max_pref"]
        with patch('app.stats.engine.groups_for_hand') as mock_groups:
            mock_groups.return_value = ["nonko_9max_pref"]
            
            # Run stats calculation
            result = run_stats(self.test_jsonl, self.dsl_path, self.out_dir)
        
        # Verify processing results
        self.assertEqual(result['hands_processed'], 2)
        self.assertEqual(result['stats_computed'], 4)  # 4 RFI stats
        self.assertEqual(result['errors'], 0)
        
        # Load and verify stat_counts.json
        with open(result['output_path'], 'r') as f:
            manifest = json.load(f)
        
        # Verify manifest structure
        self.assertEqual(manifest['hands_processed'], 2)
        self.assertIn('2025-06', manifest['counts'])
        
        # Verify RFI_EARLY stats: 2 opportunities, 1 attempt, 50%
        june_stats = manifest['counts']['2025-06']['nonko_9max_pref']
        self.assertIn('RFI_EARLY', june_stats)
        
        rfi_early = june_stats['RFI_EARLY']
        self.assertEqual(rfi_early['opportunities'], 2, "Should have 2 RFI opportunities")
        self.assertEqual(rfi_early['attempts'], 1, "Should have 1 RFI attempt")
        self.assertEqual(rfi_early['percentage'], 50.0, "Should be 50% (1/2)")
        
        # Verify index files exist
        index_dir = result['index_dir']
        self.assertTrue(os.path.exists(index_dir))
        
        # Check opportunity IDs file
        opp_file = os.path.join(index_dir, '2025-06__nonko_9max_pref__RFI_EARLY__opps.ids')
        self.assertTrue(os.path.exists(opp_file))
        with open(opp_file, 'r') as f:
            opp_ids = [line.strip() for line in f]
        self.assertEqual(len(opp_ids), 2, "Should have 2 hand IDs in opportunities file")
        
        # Check attempt IDs file
        att_file = os.path.join(index_dir, '2025-06__nonko_9max_pref__RFI_EARLY__attempts.ids')
        self.assertTrue(os.path.exists(att_file))
        with open(att_file, 'r') as f:
            att_ids = [line.strip() for line in f]
        self.assertEqual(len(att_ids), 1, "Should have 1 hand ID in attempts file")
    
    def test_multimonth_stats(self):
        """Test stats calculation across multiple months."""
        # Create hands in different months
        test_hands = [
            # June: 1 opportunity, 1 attempt
            {
                'hero': 'TestHero',
                'timestamp_utc': '2025-06-15T10:00:00Z',
                'site': 'pokerstars',
                'tournament_id': '101',
                'file_id': 'non-ko/june.txt',
                'table_max': 6,
                'button_seat': 4,
                'players': [{'name': 'TestHero'}],
                'derived': {
                    'positions': {
                        'table_max_resolved': 6,
                        'pos_group': {'TestHero': 'LP'},
                        'abs_positions': {'TestHero': 'CO'}
                    },
                    'preflop': {
                        'unopened_pot': True,
                        'hero_raised_first_in': True,
                        'pot_type': 'SRP'
                    },
                    'ip': {
                        'heads_up_flop': True,
                        'players_to_flop': 2
                    },
                    'stacks': {
                        'eff_stack_bb_srp': 22.0
                    }
                }
            },
            # July: 1 opportunity, 0 attempts
            {
                'hero': 'TestHero',
                'timestamp_utc': '2025-07-20T14:00:00Z',
                'site': 'pokerstars',
                'tournament_id': '102',
                'file_id': 'non-ko/july.txt',
                'table_max': 6,
                'button_seat': 1,
                'players': [{'name': 'TestHero'}],
                'derived': {
                    'positions': {
                        'table_max_resolved': 6,
                        'pos_group': {'TestHero': 'LP'},
                        'abs_positions': {'TestHero': 'BTN'}
                    },
                    'preflop': {
                        'unopened_pot': True,
                        'hero_raised_first_in': False,  # Fold
                        'pot_type': 'SRP'  # Still SRP context
                    },
                    'ip': {
                        'heads_up_flop': True,
                        'players_to_flop': 2  # Others played
                    },
                    'stacks': {
                        'eff_stack_bb_srp': 18.0
                    }
                }
            }
        ]
        
        # Write test data
        with open(self.test_jsonl, 'w') as f:
            for hand in test_hands:
                f.write(json.dumps(hand) + '\n')
        
        # Mock groups_for_hand
        with patch('app.stats.engine.groups_for_hand') as mock_groups:
            mock_groups.return_value = ["nonko_6max_pref"]
            
            # Run stats
            result = run_stats(self.test_jsonl, self.dsl_path, self.out_dir)
        
        # Verify we have 2 months
        self.assertEqual(result['months_generated'], 2)
        
        # Load manifest
        with open(result['output_path'], 'r') as f:
            manifest = json.load(f)
        
        # Verify June: CO steal 100%
        june = manifest['counts']['2025-06']['nonko_6max_pref']
        self.assertEqual(june['RFI_CO_STEAL']['percentage'], 100.0)
        
        # Verify July: BTN steal 0%
        july = manifest['counts']['2025-07']['nonko_6max_pref']
        self.assertEqual(july['RFI_BTN_STEAL']['percentage'], 0.0)


if __name__ == '__main__':
    unittest.main()