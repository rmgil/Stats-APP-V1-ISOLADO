"""Test consolidated dashboard payload builder"""

import json
import tempfile
from pathlib import Path
from app.api_dashboard import build_dashboard_payload


def test_build_dashboard_payload_no_token():
    """Test payload builder without token (current directory)"""
    payload = build_dashboard_payload(None)
    
    assert payload["run"]["token"] is None
    assert payload["run"]["base"] == "."
    assert "overall" in payload
    assert "group_level" in payload
    assert "samples" in payload
    assert "months" in payload
    assert "counts" in payload


def test_build_dashboard_payload_with_token():
    """Test payload builder with token"""
    token = "test123"
    payload = build_dashboard_payload(token)
    
    assert payload["run"]["token"] == token
    assert payload["run"]["base"] == f"runs/{token}"
    assert "overall" in payload
    assert "group_level" in payload
    assert "samples" in payload
    assert "months" in payload
    assert "counts" in payload


def test_build_dashboard_payload_with_data():
    """Test payload builder with actual data files"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data structure
        base = Path(tmpdir)
        stats_dir = base / "stats"
        scores_dir = base / "scores"
        stats_dir.mkdir()
        scores_dir.mkdir()
        
        # Create test stats file
        stats_data = {
            "2024-01": {
                "preflop": {
                    "VPIP": {
                        "opportunities": 100,
                        "attempts": 25,
                        "percentage": 25.0,
                        "index_files": ["file1.jsonl"]
                    }
                }
            }
        }
        with open(stats_dir / "stat_counts.json", "w") as f:
            json.dump(stats_data, f)
        
        # Create test score file  
        score_data = {
            "overall": 85.5,
            "group_level": {
                "preflop": "Bom"
            },
            "samples": {
                "6max": 1000,
                "9max": 500
            }
        }
        with open(scores_dir / "scorecard.json", "w") as f:
            json.dump(score_data, f)
        
        # Mock the base path resolution
        import app.api_dashboard
        original_path = Path
        
        def mock_path(path_str):
            if path_str == ".":
                return base
            elif path_str == "stats":
                return stats_dir
            elif path_str == "scores":
                return scores_dir
            return original_path(path_str)
        
        app.api_dashboard.Path = mock_path
        
        try:
            # Build payload
            payload = build_dashboard_payload(None)
            
            # Verify data was loaded
            assert payload["overall"]["score"] == 85.5
            assert payload["group_level"]["preflop"] == "Bom"
            assert payload["samples"]["6max"] == 1000
            assert payload["months"] == ["2024-01"]
            assert "preflop" in payload["counts"]["2024-01"]
            assert payload["counts"]["2024-01"]["preflop"]["VPIP"]["percentage"] == 25.0
        finally:
            # Restore original Path
            app.api_dashboard.Path = original_path


def test_build_dashboard_payload_normalizes_score_keys():
    """Test that payload builder normalizes different score structures"""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        scores_dir = base / "scores"
        scores_dir.mkdir()
        
        # Test with nested data structure
        score_data = {
            "data": {
                "overall": 75.0,
                "group_level": {"postflop": "OK"}
            },
            "sample": {"total": 1500}  # Note: 'sample' not 'samples'
        }
        with open(scores_dir / "scorecard.json", "w") as f:
            json.dump(score_data, f)
        
        import app.api_dashboard
        original_path = Path
        
        def mock_path(path_str):
            if path_str == ".":
                return base
            elif path_str == "scores":
                return scores_dir
            return original_path(path_str)
        
        app.api_dashboard.Path = mock_path
        
        try:
            payload = build_dashboard_payload(None)
            
            # Verify normalization worked
            assert payload["overall"]["score"] == 75.0
            assert payload["group_level"]["postflop"] == "OK"
            assert payload["samples"]["total"] == 1500
        finally:
            app.api_dashboard.Path = original_path


def test_build_dashboard_payload_uses_pipeline_counts(monkeypatch):
    """Ensure monthly payload uses pipeline_result totals and groups."""

    token = "abc123def456"
    month = "2025-07"

    pipeline_result = {
        "status": "completed",
        "combined": {
            "nonko_9max": {
                "hand_count": 10,
                "postflop_hands_count": 4,
                "stats": {
                    "Early RFI": {"opportunities": 10, "attempts": 5},
                    "Flop CBet IP %": {"opportunities": 4, "attempts": 2},
                },
                "postflop_stats": {
                    "Flop CBet IP %": {"opportunities": 4, "attempts": 2}
                },
                "scores": {
                    "rfi": {
                        "stats": {
                            "Early RFI": {"score": 80, "ideal": 60, "weight": 1}
                        },
                        "overall_score": 80,
                    },
                    "bvb": {"overall_score": 70},
                    "threbet_cc": {"overall_score": 75},
                    "vs_3bet": {"overall_score": 70},
                    "squeeze": {"overall_score": 65},
                    "bb_defense": {"overall_score": 68},
                    "sb_defense": {"overall_score": 66},
                },
                "overall_score": 75,
            },
            "nonko_6max": {
                "hand_count": 0,
                "postflop_hands_count": 0,
                "stats": {},
                "postflop_stats": {},
                "scores": {},
                "overall_score": 0,
            },
            "pko": {
                "hand_count": 0,
                "postflop_hands_count": 0,
                "stats": {},
                "postflop_stats": {},
                "scores": {},
                "overall_score": 0,
            },
        },
        "sites": {
            "pokerstars": {
                "nonko_9max": {
                    "hand_count": 10,
                    "postflop_hands_count": 4,
                }
            }
        },
        "valid_hands": 10,
        "total_hands": 12,
        "aggregated_discards": {"mystery": 1, "other": 1, "total": 2},
        "classification": {
            "discarded_hands": {"mystery": 1, "other": 1, "total": 2},
            "total_hands": 12,
            "valid_hands": 10,
        },
    }

    months_manifest = {"months": [{"month": month}]}

    class StubStorage:
        def get_pipeline_result(self, token_arg, month=None):
            assert token_arg == token
            return pipeline_result

        def get_months_manifest(self, token_arg):
            assert token_arg == token
            return months_manifest

    monkeypatch.setattr("app.api_dashboard.get_result_storage", lambda: StubStorage())

    payload = build_dashboard_payload(token, month=month)

    assert payload["valid_hands"] == 10
    assert payload["total_hands"] == 12
    assert payload["discard_stats"]["mystery"] == 1
    assert payload["groups"]["nonko_9max"]["hands_count"] == 10
