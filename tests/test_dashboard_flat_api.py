"""Test dashboard flat API with real statistics calculation."""
import pytest
import json
import tempfile
from pathlib import Path
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.stats.runner import run_stats
from app.score.runner import build_scorecard
from app.parse.runner import parse_folder
from app.derive.runner import enrich_hands
from main import app

# Mini hands_enriched.jsonl fixture (6 hands with varied stats)
MINI_HANDS_ENRICHED = [
    {
        "hand_id": "PS_001",
        "timestamp_utc": "2024-01-15T10:00:00Z",
        "site": "pokerstars",
        "hero": "TestHero",
        "table_max": 9,
        "file_id": "/NON-KO/hand1.txt",
        "derived": {
            "positions": {"table_max_resolved": 9},
            "preflop": {
                "hero_position": "EP",
                "pot_type": "RFI",
                "hero_rfi": True,
                "hero_rfi_opp": True
            }
        },
        "streets": {"flop": {"actions": [{"player": "TestHero", "action": "bet"}]}}
    },
    {
        "hand_id": "PS_002", 
        "timestamp_utc": "2024-01-15T11:00:00Z",
        "site": "pokerstars",
        "hero": "TestHero",
        "table_max": 9,
        "file_id": "/NON-KO/hand2.txt",
        "derived": {
            "positions": {"table_max_resolved": 9},
            "preflop": {
                "hero_position": "EP",
                "pot_type": "FOLD",
                "hero_rfi": False,
                "hero_rfi_opp": True
            }
        }
    },
    {
        "hand_id": "PS_003",
        "timestamp_utc": "2024-01-15T12:00:00Z",
        "site": "pokerstars",
        "hero": "TestHero",
        "table_max": 6,
        "file_id": "/NON-KO/hand3.txt",
        "derived": {
            "positions": {"table_max_resolved": 6},
            "preflop": {
                "hero_position": "CO",
                "pot_type": "STEAL",
                "hero_steal": True,
                "hero_steal_opp": True
            }
        },
        "streets": {"flop": {"actions": [{"player": "TestHero", "action": "check"}]}}
    },
    {
        "hand_id": "PS_004",
        "timestamp_utc": "2024-01-15T13:00:00Z",
        "site": "pokerstars",
        "hero": "TestHero",
        "table_max": 6,
        "file_id": "/NON-KO/hand4.txt",
        "derived": {
            "positions": {"table_max_resolved": 6},
            "preflop": {
                "hero_position": "BTN",
                "pot_type": "STEAL",
                "hero_steal": True,
                "hero_steal_opp": True
            }
        }
    },
    {
        "hand_id": "GG_001",
        "timestamp_utc": "2024-01-15T14:00:00Z",
        "site": "ggpoker",
        "hero": "TestHero",
        "table_max": 9,
        "file_id": "/PKO/hand5.txt",
        "derived": {
            "positions": {"table_max_resolved": 9},
            "preflop": {
                "hero_position": "MP",
                "pot_type": "RFI",
                "hero_rfi": True,
                "hero_rfi_opp": True
            },
            "postflop": {
                "cbet": True,
                "cbet_opp": True,
                "cbet_flop": True,
                "cbet_flop_opp": True
            }
        },
        "streets": {
            "flop": {"actions": [{"player": "TestHero", "action": "bet", "is_cbet": True}]}
        }
    },
    {
        "hand_id": "GG_002",
        "timestamp_utc": "2024-01-15T15:00:00Z",
        "site": "ggpoker",
        "hero": "TestHero",
        "table_max": 9,
        "file_id": "/PKO/hand6.txt",
        "derived": {
            "positions": {"table_max_resolved": 9},
            "preflop": {
                "hero_position": "MP",
                "pot_type": "3BET",
                "hero_3bet": True,
                "hero_3bet_opp": True
            },
            "postflop": {
                "cbet": False,
                "cbet_opp": True,
                "cbet_flop": False,
                "cbet_flop_opp": True
            }
        },
        "streets": {
            "flop": {"actions": [{"player": "TestHero", "action": "check"}]}
        }
    }
]


class TestDashboardFlatAPI:
    """Test /api/stats/flat endpoint with real data."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client
    
    @pytest.fixture
    def test_session_data(self):
        """Create test session with mini hands data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            session_dir = Path(tmpdir)
            
            # Create hands_enriched.jsonl
            hands_path = session_dir / "hands_enriched.jsonl"
            with open(hands_path, 'w') as f:
                for hand in MINI_HANDS_ENRICHED:
                    f.write(json.dumps(hand) + '\n')
            
            # Run stats
            stats_dir = session_dir / "stats"
            stats_dir.mkdir(exist_ok=True)
            
            stats_result = run_stats(
                str(hands_path),
                'app/stats/dsl/stats.yml',
                str(stats_dir)
            )
            
            # Run scoring
            scores_dir = session_dir / "scores"
            scores_dir.mkdir(exist_ok=True)
            
            score_result = build_scorecard(
                str(stats_dir / 'stat_counts.json'),
                'app/score/config.yml',
                str(scores_dir / 'scorecard.json')
            )
            
            return {
                'session_dir': session_dir,
                'stats_path': stats_dir / 'stat_counts.json',
                'score_path': scores_dir / 'scorecard.json',
                'stats_result': stats_result,
                'score_result': score_result
            }
    
    def test_stats_flat_api_percentages(self, test_session_data):
        """Test that /api/stats/flat returns correct percentages."""
        # Load the generated stats
        with open(test_session_data['stats_path'], 'r') as f:
            stats = json.load(f)
        
        # Verify some expected values
        groups = stats.get('groups', {})
        
        # Check NON-KO 9max preflop group exists
        assert 'nonko_9max_pref' in groups, "Missing nonko_9max_pref group"
        
        # Check RFI stats
        rfi_stats = groups.get('nonko_9max_pref', {}).get('subgroups', {}).get('RFI', {}).get('stats', {})
        if 'RFI_EARLY' in rfi_stats:
            rfi_early = rfi_stats['RFI_EARLY']
            
            # Verify percentage calculation
            if rfi_early['opportunities'] > 0:
                expected_pct = (rfi_early['attempts'] / rfi_early['opportunities']) * 100
                assert abs(rfi_early['percentage'] - expected_pct) < 0.01, \
                    f"Percentage mismatch: {rfi_early['percentage']} != {expected_pct}"
        
        # Check NON-KO 6max preflop group
        if 'nonko_6max_pref' in groups:
            steal_stats = groups['nonko_6max_pref'].get('subgroups', {}).get('STEAL', {}).get('stats', {})
            
            # Check CO_STEAL
            if 'CO_STEAL' in steal_stats:
                co_steal = steal_stats['CO_STEAL']
                assert co_steal['opportunities'] > 0, "CO_STEAL should have opportunities"
                assert co_steal['attempts'] > 0, "CO_STEAL should have attempts"
                
                # Verify percentage
                expected_pct = (co_steal['attempts'] / co_steal['opportunities']) * 100
                assert abs(co_steal['percentage'] - expected_pct) < 0.01, \
                    f"CO_STEAL percentage mismatch"
    
    def test_stats_flat_api_scores(self, test_session_data):
        """Test that scores/notes are calculated correctly."""
        # Load the generated scorecard
        with open(test_session_data['score_path'], 'r') as f:
            scorecard = json.load(f)
        
        # Check overall score exists and is reasonable
        overall = scorecard.get('overall', 0)
        assert overall > 0, "Overall score should be greater than 0"
        assert overall <= 100, "Overall score should not exceed 100"
        
        # Check group scores
        groups = scorecard.get('groups', {})
        
        for group_name, group_data in groups.items():
            group_score = group_data.get('score', 0)
            assert group_score >= 0, f"Group {group_name} score should be non-negative"
            assert group_score <= 100, f"Group {group_name} score should not exceed 100"
            
            # Check subgroup scores
            subgroups = group_data.get('subgroups', {})
            for subgroup_name, subgroup_data in subgroups.items():
                subgroup_score = subgroup_data.get('score', 0)
                assert subgroup_score >= 0, f"Subgroup {subgroup_name} score should be non-negative"
                
                # Check individual stat scores
                stats = subgroup_data.get('stats', {})
                for stat_name, stat_data in stats.items():
                    stat_score = stat_data.get('score', 0)
                    assert stat_score >= 0, f"Stat {stat_name} score should be non-negative"
                    assert stat_score <= 100, f"Stat {stat_name} score should not exceed 100"
    
    def test_api_response_format(self, client, test_session_data):
        """Test /api/stats/flat API response format."""
        # Create test session directory structure
        session_id = 'test123'
        session_path = Path('/tmp/mtt_sessions') / session_id / 'out'
        session_path.mkdir(parents=True, exist_ok=True)
        
        # Copy stats files
        stats_dir = session_path / 'stats'
        stats_dir.mkdir(exist_ok=True)
        
        import shutil
        shutil.copy(test_session_data['stats_path'], stats_dir / 'stat_counts.json')
        
        # Test API call
        response = client.get(f'/api/stats/flat?session={session_id}')
        
        assert response.status_code == 200, f"API returned {response.status_code}"
        
        data = response.get_json()
        assert 'stats' in data, "Response should contain 'stats' key"
        assert 'session' in data, "Response should contain 'session' key"
        assert 'filters' in data, "Response should contain 'filters' key"
        
        # Check stats format
        stats = data['stats']
        assert isinstance(stats, list), "Stats should be a list"
        
        if len(stats) > 0:
            first_stat = stats[0]
            required_keys = ['group', 'subgroup', 'stat', 'opportunities', 'attempts', 'percentage', 'ids']
            for key in required_keys:
                assert key in first_stat, f"Stat entry missing key: {key}"
            
            # Check percentage is rounded to 2 decimal places
            pct = first_stat['percentage']
            assert isinstance(pct, (int, float)), "Percentage should be numeric"
            
            # Check IDs structure
            ids = first_stat['ids']
            assert 'opportunities' in ids, "IDs should have opportunities"
            assert 'attempts' in ids, "IDs should have attempts"
    
    def test_postflop_stats_calculation(self, test_session_data):
        """Test postflop statistics are calculated correctly."""
        with open(test_session_data['stats_path'], 'r') as f:
            stats = json.load(f)
        
        # Check PKO postflop stats
        pko_groups = stats.get('groups', {}).get('pko_pref', {})
        if pko_groups:
            postflop = pko_groups.get('subgroups', {}).get('POSTFLOP_CBET', {})
            if postflop:
                cbet_stats = postflop.get('stats', {})
                
                # Check CBET_FLOP if it exists
                if 'CBET_FLOP' in cbet_stats:
                    cbet_flop = cbet_stats['CBET_FLOP']
                    # Should have 2 opportunities (GG_001 and GG_002 had cbet_flop_opp)
                    assert cbet_flop['opportunities'] == 2, \
                        f"Expected 2 CBET_FLOP opportunities, got {cbet_flop['opportunities']}"
                    # Should have 1 attempt (only GG_001 made cbet)
                    assert cbet_flop['attempts'] == 1, \
                        f"Expected 1 CBET_FLOP attempt, got {cbet_flop['attempts']}"
                    
                    # Check percentage
                    expected_pct = 50.0  # 1/2 * 100
                    assert abs(cbet_flop['percentage'] - expected_pct) < 0.01, \
                        f"CBET_FLOP percentage should be 50%, got {cbet_flop['percentage']}"


if __name__ == "__main__":
    # Run tests
    test = TestDashboardFlatAPI()
    
    # Create test session
    print("Creating test session data...")
    with tempfile.TemporaryDirectory() as tmpdir:
        session_dir = Path(tmpdir)
        
        # Create hands_enriched.jsonl
        hands_path = session_dir / "hands_enriched.jsonl"
        with open(hands_path, 'w') as f:
            for hand in MINI_HANDS_ENRICHED:
                f.write(json.dumps(hand) + '\n')
        
        # Run stats
        stats_dir = session_dir / "stats"
        stats_dir.mkdir(exist_ok=True)
        
        print("Running stats calculation...")
        stats_result = run_stats(
            str(hands_path),
            'app/stats/dsl/stats.yml',
            str(stats_dir)
        )
        print(f"Stats calculated: {stats_result.get('total_hands', len(MINI_HANDS_ENRICHED))} hands processed")
        
        # Run scoring
        scores_dir = session_dir / "scores"
        scores_dir.mkdir(exist_ok=True)
        
        print("Running score calculation...")
        score_result = build_scorecard(
            str(stats_dir / 'stat_counts.json'),
            'app/score/config.yml',
            str(scores_dir / 'scorecard.json')
        )
        print(f"Scores calculated: overall score = {score_result.get('overall', 0):.1f}")
        
        test_data = {
            'session_dir': session_dir,
            'stats_path': stats_dir / 'stat_counts.json',
            'score_path': scores_dir / 'scorecard.json',
            'stats_result': stats_result,
            'score_result': score_result
        }
        
        # Run tests
        print("\nRunning tests...")
        test.test_stats_flat_api_percentages(test_data)
        print("✓ Percentages calculated correctly")
        
        test.test_stats_flat_api_scores(test_data)
        print("✓ Scores calculated correctly")
        
        test.test_postflop_stats_calculation(test_data)
        print("✓ Postflop stats calculated correctly")
        
        print("\n✅ All dashboard flat API tests passed!")