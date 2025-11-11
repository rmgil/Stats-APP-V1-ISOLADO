"""Test artifact storage in runs directory"""

import json
import tempfile
from pathlib import Path
import shutil
from app.pipeline_orchestrator import build_all


def test_artifacts_copied_to_runs_directory():
    """Test that artifacts are copied to runs/{token} directory"""
    
    # Create test directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a job directory structure like /tmp/jobs/{token}/out
        token = "test_abc123"
        job_dir = Path(tmpdir) / "jobs" / token
        out_dir = job_dir / "out"
        out_dir.mkdir(parents=True)
        
        # Create manifest
        manifest_path = job_dir / "classification_manifest.json"
        manifest = {
            "inputs": {"NON-KO": ["test1.txt", "test2.txt"]},
            "outputs": {"NON-KO": ["nonko/test1.txt", "nonko/test2.txt"]},
            "timestamp": "2024-01-01T00:00:00"
        }
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)
        
        # Create parsed directory with empty hands.jsonl
        parsed_dir = out_dir / "parsed"
        parsed_dir.mkdir(parents=True)
        (parsed_dir / "hands.jsonl").touch()
        (parsed_dir / "hands_enriched.jsonl").touch()
        
        # Create mock stat_counts.json
        stats_dir = out_dir / "stats"
        stats_dir.mkdir(parents=True)
        stat_counts = {"2024-01": {"preflop": {"VPIP": {"opportunities": 100, "attempts": 25}}}}
        with open(stats_dir / "stat_counts.json", "w") as f:
            json.dump(stat_counts, f)
        
        # Create mock scorecard.json
        scores_dir = out_dir / "scores"
        scores_dir.mkdir(parents=True)
        scorecard = {"overall": 85.5, "group_level": {"preflop": "Bom"}}
        with open(scores_dir / "scorecard.json", "w") as f:
            json.dump(scorecard, f)
        
        # Run the pipeline
        result = build_all(str(manifest_path), str(out_dir))
        
        # Check that artifacts were copied to runs/{token}
        run_dir = Path("runs") / token
        
        # Check stats file
        stats_file = run_dir / "stats" / "stat_counts.json"
        assert stats_file.exists(), f"Stats file not found at {stats_file}"
        
        with open(stats_file, "r") as f:
            copied_stats = json.load(f)
        assert copied_stats == stat_counts
        
        # Check scorecard file
        score_file = run_dir / "scores" / "scorecard.json"
        assert score_file.exists(), f"Scorecard file not found at {score_file}"
        
        with open(score_file, "r") as f:
            copied_score = json.load(f)
        assert copied_score == scorecard
        
        # Cleanup
        if run_dir.exists():
            shutil.rmtree(run_dir)
        
        print(f"✓ Artifacts successfully copied to runs/{token}")
        return True