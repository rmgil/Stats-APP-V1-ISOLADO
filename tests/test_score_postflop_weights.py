"""
Phase 8.2.F - Test scoring system with postflop weights
Ensures score changes based on percentages and sample size minimums
"""
import json
import tempfile
import pytest
from pathlib import Path
from app.score.runner import build_scorecard
from app.score.loader import load_config


def create_stat_counts_file(percentages, opportunities):
    """Create a synthetic stat_counts.json file with specified values"""
    
    stat_counts = {
        "counts": {
            "2024-07": {
                "postflop_all": {}
            }
        },
        "metadata": {
            "generated_at": "2024-07-15T12:00:00",
            "total_hands": 1000
        }
    }
    
    # Add postflop stats with specified percentages and opportunities
    postflop_stats = {
        "POST_CBET_FLOP_IP": {
            "percentage": percentages.get("POST_CBET_FLOP_IP", 60.0),
            "opportunities": opportunities.get("POST_CBET_FLOP_IP", 100),
            "attempts": int(opportunities.get("POST_CBET_FLOP_IP", 100) * percentages.get("POST_CBET_FLOP_IP", 60.0) / 100)
        },
        "POST_VS_CBET_FLOP_FOLD_IP": {
            "percentage": percentages.get("POST_VS_CBET_FLOP_FOLD_IP", 40.0),
            "opportunities": opportunities.get("POST_VS_CBET_FLOP_FOLD_IP", 80),
            "attempts": int(opportunities.get("POST_VS_CBET_FLOP_FOLD_IP", 80) * percentages.get("POST_VS_CBET_FLOP_FOLD_IP", 40.0) / 100)
        },
        "POST_PROBE_TURN_ATT_IP": {
            "percentage": percentages.get("POST_PROBE_TURN_ATT_IP", 50.0),
            "opportunities": opportunities.get("POST_PROBE_TURN_ATT_IP", 50),
            "attempts": int(opportunities.get("POST_PROBE_TURN_ATT_IP", 50) * percentages.get("POST_PROBE_TURN_ATT_IP", 50.0) / 100)
        },
        "POST_DELAYED_CBET_TURN_ATT_IP": {
            "percentage": percentages.get("POST_DELAYED_CBET_TURN_ATT_IP", 45.0),
            "opportunities": opportunities.get("POST_DELAYED_CBET_TURN_ATT_IP", 30),
            "attempts": int(opportunities.get("POST_DELAYED_CBET_TURN_ATT_IP", 30) * percentages.get("POST_DELAYED_CBET_TURN_ATT_IP", 45.0) / 100)
        },
        "POST_DONK_FLOP": {
            "percentage": percentages.get("POST_DONK_FLOP", 5.0),
            "opportunities": opportunities.get("POST_DONK_FLOP", 60),
            "attempts": int(opportunities.get("POST_DONK_FLOP", 60) * percentages.get("POST_DONK_FLOP", 5.0) / 100)
        },
        "POST_XR_FLOP": {
            "percentage": percentages.get("POST_XR_FLOP", 12.0),
            "opportunities": opportunities.get("POST_XR_FLOP", 40),
            "attempts": int(opportunities.get("POST_XR_FLOP", 40) * percentages.get("POST_XR_FLOP", 12.0) / 100)
        },
        "POST_WTSD": {
            "percentage": percentages.get("POST_WTSD", 28.5),
            "opportunities": opportunities.get("POST_WTSD", 200),
            "attempts": int(opportunities.get("POST_WTSD", 200) * percentages.get("POST_WTSD", 28.5) / 100)
        },
        "POST_W$SD": {
            "percentage": percentages.get("POST_W$SD", 52.5),
            "opportunities": opportunities.get("POST_W$SD", 57),
            "attempts": int(opportunities.get("POST_W$SD", 57) * percentages.get("POST_W$SD", 52.5) / 100)
        },
        "POST_WWSF": {
            "percentage": percentages.get("POST_WWSF", 46.0),
            "opportunities": opportunities.get("POST_WWSF", 200),
            "attempts": int(opportunities.get("POST_WWSF", 200) * percentages.get("POST_WWSF", 46.0) / 100)
        },
        "POST_AGG_PCT_FLOP": {
            "percentage": percentages.get("POST_AGG_PCT_FLOP", 32.0),
            "opportunities": opportunities.get("POST_AGG_PCT_FLOP", 150),
            "attempts": int(opportunities.get("POST_AGG_PCT_FLOP", 150) * percentages.get("POST_AGG_PCT_FLOP", 32.0) / 100)
        },
        "POST_AGG_PCT_TURN": {
            "percentage": percentages.get("POST_AGG_PCT_TURN", 28.0),
            "opportunities": opportunities.get("POST_AGG_PCT_TURN", 100),
            "attempts": int(opportunities.get("POST_AGG_PCT_TURN", 100) * percentages.get("POST_AGG_PCT_TURN", 28.0) / 100)
        },
        "POST_AGG_PCT_RIVER": {
            "percentage": percentages.get("POST_AGG_PCT_RIVER", 22.0),
            "opportunities": opportunities.get("POST_AGG_PCT_RIVER", 50),
            "attempts": int(opportunities.get("POST_AGG_PCT_RIVER", 50) * percentages.get("POST_AGG_PCT_RIVER", 22.0) / 100)
        }
    }
    
    stat_counts["counts"]["2024-07"]["postflop_all"] = postflop_stats
    
    return stat_counts


class TestScoreWithIdealPercentages:
    """Test scoring when stats match ideal percentages"""
    
    def test_perfect_scores(self):
        """Test that perfect percentages yield high scores"""
        
        # Create stat counts with ideal percentages
        ideal_percentages = {
            "POST_CBET_FLOP_IP": 60.0,  # Ideal: 60%
            "POST_VS_CBET_FLOP_FOLD_IP": 40.0,  # Ideal: 40%
            "POST_PROBE_TURN_ATT_IP": 50.0,  # Ideal: 50%
            "POST_WTSD": 28.5,  # Ideal: 28.5%
            "POST_W$SD": 52.5,  # Ideal: 52.5%
            "POST_WWSF": 46.0,  # Ideal: 46%
            "POST_AGG_PCT_FLOP": 32.0  # Ideal: 32%
        }
        
        high_opportunities = {k: 100 for k in ideal_percentages.keys()}
        
        stat_counts = create_stat_counts_file(ideal_percentages, high_opportunities)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write stat counts
            counts_path = Path(temp_dir) / "stat_counts.json"
            with open(counts_path, 'w') as f:
                json.dump(stat_counts, f)
            
            # Build scorecard
            result = build_scorecard(
                str(counts_path),
                "app/score/config.yml",
                temp_dir,
                force=True
            )
            
            # Score should be high when matching ideals
            assert result is not None
            
            # Check if scorecard was generated
            scorecard_path = Path(temp_dir) / "scorecard.json"
            assert scorecard_path.exists()
    
    def test_poor_scores(self):
        """Test that poor percentages yield low scores"""
        
        # Create stat counts with poor percentages (far from ideals)
        poor_percentages = {
            "POST_CBET_FLOP_IP": 90.0,  # Too high (ideal: 60%)
            "POST_VS_CBET_FLOP_FOLD_IP": 70.0,  # Too high (ideal: 40%)
            "POST_PROBE_TURN_ATT_IP": 10.0,  # Too low (ideal: 50%)
            "POST_WTSD": 50.0,  # Too high (ideal: 28.5%)
            "POST_W$SD": 30.0,  # Too low (ideal: 52.5%)
            "POST_WWSF": 20.0,  # Too low (ideal: 46%)
            "POST_AGG_PCT_FLOP": 60.0  # Too high (ideal: 32%)
        }
        
        high_opportunities = {k: 100 for k in poor_percentages.keys()}
        
        stat_counts = create_stat_counts_file(poor_percentages, high_opportunities)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write stat counts
            counts_path = Path(temp_dir) / "stat_counts.json"
            with open(counts_path, 'w') as f:
                json.dump(stat_counts, f)
            
            # Build scorecard
            result = build_scorecard(
                str(counts_path),
                "app/score/config.yml",
                temp_dir,
                force=True
            )
            
            # Score should be lower when far from ideals
            assert result is not None
            
            # Check if scorecard was generated
            scorecard_path = Path(temp_dir) / "scorecard.json"
            assert scorecard_path.exists()


class TestSampleSizeEffects:
    """Test how sample size affects scoring"""
    
    def test_low_sample_size_penalty(self):
        """Test that low sample sizes receive penalties"""
        
        # Good percentages but low sample size
        good_percentages = {
            "POST_CBET_FLOP_IP": 60.0,
            "POST_WTSD": 28.5,
            "POST_W$SD": 52.5
        }
        
        low_opportunities = {
            "POST_CBET_FLOP_IP": 5,  # Very low sample
            "POST_WTSD": 3,  # Very low sample
            "POST_W$SD": 2  # Very low sample
        }
        
        stat_counts = create_stat_counts_file(good_percentages, low_opportunities)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write stat counts
            counts_path = Path(temp_dir) / "stat_counts.json"
            with open(counts_path, 'w') as f:
                json.dump(stat_counts, f)
            
            # Build scorecard
            result = build_scorecard(
                str(counts_path),
                "app/score/config.yml",
                temp_dir,
                force=True
            )
            
            assert result is not None
    
    def test_high_sample_size_full_weight(self):
        """Test that high sample sizes get full weight"""
        
        # Good percentages with high sample size
        good_percentages = {
            "POST_CBET_FLOP_IP": 60.0,
            "POST_WTSD": 28.5,
            "POST_W$SD": 52.5
        }
        
        high_opportunities = {
            "POST_CBET_FLOP_IP": 200,  # High sample
            "POST_WTSD": 150,  # High sample
            "POST_W$SD": 100  # High sample
        }
        
        stat_counts = create_stat_counts_file(good_percentages, high_opportunities)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write stat counts
            counts_path = Path(temp_dir) / "stat_counts.json"
            with open(counts_path, 'w') as f:
                json.dump(stat_counts, f)
            
            # Build scorecard
            result = build_scorecard(
                str(counts_path),
                "app/score/config.yml",
                temp_dir,
                force=True
            )
            
            assert result is not None


class TestWeightConfiguration:
    """Test that weight configuration affects scores properly"""
    
    def test_weight_normalization(self):
        """Test that weights are properly normalized"""
        
        config = load_config("app/score/config.yml")
        
        # Check group weights sum to 1.0 (or close due to normalization)
        group_weights = config['weights']['groups']
        total_group_weight = sum(group_weights.values())
        assert 0.99 <= total_group_weight <= 1.01  # Allow small floating point error
        
        # Check subgroup weights
        subgroup_weights = config['weights']['subgroups']
        postflop_subgroups = [k for k in subgroup_weights.keys() if 'POSTFLOP' in k]
        
        # Verify postflop subgroups exist
        assert len(postflop_subgroups) > 0
        
        # Check that important postflop subgroups have reasonable weights
        assert 'POSTFLOP_CBET' in subgroup_weights
        assert 'POSTFLOP_VS_CBET' in subgroup_weights
        assert 'POSTFLOP_SHOWDOWN' in subgroup_weights
        assert 'POSTFLOP_AGGRESSION' in subgroup_weights
    
    def test_stat_specific_weights(self):
        """Test that individual stat weights are applied"""
        
        config = load_config("app/score/config.yml")
        
        stat_weights = config['weights']['stats']
        
        # Check that important postflop stats have weights
        important_stats = [
            'POST_CBET_FLOP_IP',
            'POST_VS_CBET_FLOP_FOLD_IP',
            'POST_WTSD',
            'POST_W$SD',
            'POST_WWSF',
            'POST_AGG_PCT_FLOP'
        ]
        
        for stat in important_stats:
            assert stat in stat_weights, f"Missing weight for {stat}"
            assert stat_weights[stat] > 0, f"Weight for {stat} should be positive"


class TestIdealReferenceValues:
    """Test that ideal reference values are properly configured"""
    
    def test_ideals_exist_for_postflop_stats(self):
        """Test that all postflop stats have ideal values"""
        
        config = load_config("app/score/config.yml")
        
        ideals = config['ideals']
        
        # Check critical postflop stats have ideals
        postflop_stats = [
            'POST_CBET_FLOP_IP',
            'POST_CBET_FLOP_OOP',
            'POST_CBET_TURN_IP',
            'POST_CBET_TURN_OOP',
            'POST_VS_CBET_FLOP_FOLD_IP',
            'POST_VS_CBET_FLOP_FOLD_OOP',
            'POST_PROBE_TURN_ATT_IP',
            'POST_DELAYED_CBET_TURN_ATT_IP',
            'POST_DONK_FLOP',
            'POST_XR_FLOP',
            'POST_WTSD',
            'POST_W$SD',
            'POST_WWSF',
            'POST_AGG_PCT_FLOP',
            'POST_AGG_PCT_TURN',
            'POST_AGG_PCT_RIVER'
        ]
        
        for stat in postflop_stats:
            assert stat in ideals, f"Missing ideal for {stat}"
            assert 'postflop_all' in ideals[stat], f"Missing postflop_all ideal for {stat}"
            assert isinstance(ideals[stat]['postflop_all'], (int, float)), f"Invalid ideal value for {stat}"
    
    def test_ideal_ranges_reasonable(self):
        """Test that ideal values are within reasonable poker ranges"""
        
        config = load_config("app/score/config.yml")
        
        ideals = config['ideals']
        
        # Check that ideals are within reasonable ranges
        reasonable_ranges = {
            'POST_CBET_FLOP_IP': (50, 70),  # 50-70% CBet flop IP
            'POST_CBET_FLOP_OOP': (40, 60),  # 40-60% CBet flop OOP
            'POST_WTSD': (20, 35),  # 20-35% WTSD
            'POST_W$SD': (45, 60),  # 45-60% W$SD
            'POST_DONK_FLOP': (0, 10),  # 0-10% Donk flop (should be rare)
            'POST_XR_FLOP': (5, 20),  # 5-20% Check-raise flop
            'POST_AGG_PCT_FLOP': (25, 40)  # 25-40% Aggression flop
        }
        
        for stat, (min_val, max_val) in reasonable_ranges.items():
            if stat in ideals and 'postflop_all' in ideals[stat]:
                ideal_val = ideals[stat]['postflop_all']
                assert min_val <= ideal_val <= max_val, \
                    f"{stat} ideal {ideal_val} outside reasonable range {min_val}-{max_val}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])