"""
Test API flat data contract with minimal workspace
"""
import os
import json
import tempfile
import shutil
from pathlib import Path
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.api.flat import build_flat

def test_flat_api_minimal():
    """Test flat API with minimal stats and scorecard"""
    
    # Create temporary workspace
    with tempfile.TemporaryDirectory() as tmpdir:
        token = "test_token"
        work_dir = Path(tmpdir) / "work" / token
        stats_dir = work_dir / "stats"
        scores_dir = work_dir / "scores"
        
        # Create directories
        stats_dir.mkdir(parents=True, exist_ok=True)
        scores_dir.mkdir(parents=True, exist_ok=True)
        
        # Create minimal stat_counts.json
        stat_counts = {
            "total_hands": 1000,
            "months": ["2024-12", "2024-11"],
            "groups": {
                "nonko_9max": {
                    "subgroups": {
                        "PREFLOP_RFI": {
                            "stats": {
                                "RFI_EARLY": {
                                    "opportunities": 250,
                                    "attempts": 35,
                                    "opps": {"ids": []},
                                    "attempts_ids": []
                                }
                            }
                        }
                    }
                }
            }
        }
        
        with open(stats_dir / "stat_counts.json", 'w') as f:
            json.dump(stat_counts, f)
        
        # Create minimal scorecard.json
        scorecard = {
            "config": {
                "groups": {
                    "nonko_9max": {
                        "display_name": "NON-KO 9-Max",
                        "weight": 1.0
                    }
                },
                "subgroups": {
                    "PREFLOP_RFI": {
                        "display_name": "Pre-Flop RFI",
                        "weight": 0.8
                    }
                },
                "stats": {
                    "RFI_EARLY": {
                        "display_name": "RFI Early Position",
                        "ideal_min": 12,
                        "ideal_max": 18,
                        "sample_min": 100,
                        "weight": 0.5
                    }
                }
            },
            "scoring": {
                "overall": {"score": 82.5},
                "groups": {
                    "nonko_9max": {
                        "score": 82.5,
                        "subgroups": {
                            "PREFLOP_RFI": {
                                "score": 82.5,
                                "stats": {
                                    "RFI_EARLY": {
                                        "score": 82.5
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        with open(scores_dir / "scorecard.json", 'w') as f:
            json.dump(scorecard, f)
        
        # Change to temp directory for build_flat to work
        original_cwd = os.getcwd()
        os.chdir(tmpdir)
        
        try:
            # Build flat data
            result = build_flat(token)
            
            # Assertions
            assert result["token"] == token
            assert result["month_latest"] == "2024-12"
            assert result["overall_score"] == 82.5
            assert result["sample"]["hands"] == 1000
            assert result["sample"]["opportunities_total"] == 250
            
            # Check groups
            assert len(result["groups"]) == 1
            group = result["groups"][0]
            assert group["key"] == "nonko_9max"
            assert group["label"] == "NON-KO 9-Max"
            assert group["weight"] == 1.0
            assert group["score"] == 82.5
            
            # Check subgroups
            assert len(group["subgroups"]) == 1
            subgroup = group["subgroups"][0]
            assert subgroup["key"] == "PREFLOP_RFI"
            assert subgroup["label"] == "Pre-Flop RFI"
            assert subgroup["weight"] == 0.8
            assert subgroup["score"] == 82.5
            
            # Check stats
            assert len(subgroup["rows"]) == 1
            stat = subgroup["rows"][0]
            assert stat["code"] == "RFI_EARLY"
            assert stat["label"] == "RFI Early Position"
            assert stat["opps"] == 250
            assert stat["att"] == 35
            assert stat["pct"] == 14.0  # 35/250 * 100
            assert stat["score"] == 82.5
            assert stat["ideal_min"] == 12
            assert stat["ideal_max"] == 18
            assert stat["weight_stat"] == 0.5
            assert stat["weight_subgroup"] == 0.8
            assert stat["weight_group"] == 1.0
            assert "dentro do ideal" in stat["note"].lower()
            
            print("✅ All flat API tests passed!")
            
        finally:
            os.chdir(original_cwd)

def test_flat_api_sample_min():
    """Test note generation with sample_min"""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        token = "test_sample"
        work_dir = Path(tmpdir) / "work" / token
        stats_dir = work_dir / "stats"
        scores_dir = work_dir / "scores"
        
        stats_dir.mkdir(parents=True, exist_ok=True)
        scores_dir.mkdir(parents=True, exist_ok=True)
        
        # Create stats with low opportunities
        stat_counts = {
            "total_hands": 50,
            "months": ["2024-12"],
            "groups": {
                "nonko_9max": {
                    "subgroups": {
                        "PREFLOP_RFI": {
                            "stats": {
                                "RFI_EARLY": {
                                    "opportunities": 20,  # Below sample_min
                                    "attempts": 4
                                }
                            }
                        }
                    }
                }
            }
        }
        
        with open(stats_dir / "stat_counts.json", 'w') as f:
            json.dump(stat_counts, f)
        
        # Scorecard with sample_min
        scorecard = {
            "config": {
                "groups": {"nonko_9max": {"weight": 1.0}},
                "subgroups": {"PREFLOP_RFI": {"weight": 1.0}},
                "stats": {
                    "RFI_EARLY": {
                        "ideal_min": 12,
                        "ideal_max": 18,
                        "sample_min": 100,  # Minimum sample size
                        "weight": 1.0
                    }
                }
            },
            "scoring": {
                "overall": {"score": 50.0},
                "groups": {
                    "nonko_9max": {
                        "score": 50.0,
                        "subgroups": {
                            "PREFLOP_RFI": {
                                "score": 50.0,
                                "stats": {
                                    "RFI_EARLY": {"score": 50.0}
                                }
                            }
                        }
                    }
                }
            }
        }
        
        with open(scores_dir / "scorecard.json", 'w') as f:
            json.dump(scorecard, f)
        
        original_cwd = os.getcwd()
        os.chdir(tmpdir)
        
        try:
            result = build_flat(token)
            
            # Check note includes "amostra baixa"
            stat = result["groups"][0]["subgroups"][0]["rows"][0]
            assert stat["opps"] == 20
            assert stat["pct"] == 20.0  # 4/20 * 100
            assert "amostra baixa" in stat["note"]
            assert "+2.00pp acima do ideal" in stat["note"]  # 20% vs 18% max
            
            print("✅ Sample min test passed!")
            
        finally:
            os.chdir(original_cwd)

if __name__ == "__main__":
    test_flat_api_minimal()
    test_flat_api_sample_min()
    print("\n✅ All tests passed successfully!")