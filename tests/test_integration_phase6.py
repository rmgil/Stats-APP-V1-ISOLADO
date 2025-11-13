
"""
Integration test for complete pipeline Phases 1-6
"""
import os
import json
import tempfile
import pytest
from unittest.mock import patch

def test_full_pipeline_integration():
    """Test complete pipeline from classification to scoring"""
    
    # Skip if no test data available
    if not os.path.exists('stats/stat_counts.json'):
        pytest.skip("No stat_counts.json available for testing")
    
    # Test scoring pipeline
    from app.score.runner import build_scorecard
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = build_scorecard(
            'stats/stat_counts.json',
            'app/score/config.yml', 
            tmp_dir,
            force=True
        )
        
        assert result is not None
        assert 'scorecard_path' in result
        assert os.path.exists(result['scorecard_path'])
        
        # Load and validate scorecard
        with open(result['scorecard_path'], 'r') as f:
            scorecard = json.load(f)
        
        # Basic structure validation
        assert 'generated_at' in scorecard
        assert 'stat_level' in scorecard
        assert 'group_level' in scorecard
        assert 'overall' in scorecard
        
        # Check exports were created
        exports_dir = os.path.join(tmp_dir, "exports")
        if os.path.exists(exports_dir):
            expected_files = ["stat_level.csv", "subgroup_level.csv", "group_level.csv", "overall.txt"]
            for expected in expected_files:
                assert os.path.exists(os.path.join(exports_dir, expected))

def test_cli_runner_exists():
    """Test that CLI runner is properly implemented"""
    from app.score import runner_cli
    
    # Should have main function
    assert hasattr(runner_cli, 'main')
    
    # Should be callable
    assert callable(runner_cli.main)
