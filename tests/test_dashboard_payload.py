"""Test dashboard payload hierarchical structure"""

import json
from pathlib import Path


def test_dashboard_payload_structure(client, app_context):
    """Test that payload has hierarchical groups -> subgroups -> stats structure"""
    # Create minimal test data structure
    test_dir = Path('runs/test_token')
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # Create stats directory and minimal stat_counts.json
    stats_dir = test_dir / 'stats'
    stats_dir.mkdir(exist_ok=True)
    
    stat_counts = {
        "counts": {
            "2024-07": {
                "nonko_9max_pref": {
                    "RFI_EARLY": {
                        "opportunities": 10,
                        "attempts": 3,
                        "percentage": 30.0
                    },
                    "RFI_MIDDLE": {
                        "opportunities": 8,
                        "attempts": 2,
                        "percentage": 25.0
                    }
                }
            }
        }
    }
    
    with open(stats_dir / 'stat_counts.json', 'w') as f:
        json.dump(stat_counts, f)
    
    # Create scores directory with scorecard.json
    scores_dir = test_dir / 'scores'
    scores_dir.mkdir(exist_ok=True)
    
    scorecard = {
        "overall": 65,
        "group_level": {
            "nonko_9max_pref": 70
        },
        "stat_level": {
            "nonko_9max_pref": {
                "RFI_EARLY": 85,
                "RFI_MIDDLE": 75
            }
        }
    }
    
    with open(scores_dir / 'scorecard.json', 'w') as f:
        json.dump(scorecard, f)
    
    # Create manifest.json for ingest info
    manifest = {
        "files_total": 100,
        "files_pko": 30,
        "files_mystery": 20,
        "files_nonko": 50,
        "timestamp": "2024-07-01T10:00:00",
        "source_zip": "test.zip"
    }
    
    with open(test_dir / 'manifest.json', 'w') as f:
        json.dump(manifest, f)
    
    # Call the endpoint
    response = client.get('/api/dashboard/payload?token=test_token')
    assert response.status_code == 200
    
    payload = response.get_json()
    
    # Test basic structure
    assert 'overall' in payload
    assert 'groups' in payload
    assert 'ingest' in payload
    
    # Test ingest data
    assert payload['ingest']['files_total'] == 100
    assert payload['ingest']['files_pko'] == 30
    assert payload['ingest']['files_mystery'] == 20
    assert payload['ingest']['files_nonko'] == 50
    
    # Test hierarchical structure: groups -> subgroups -> stats
    groups = payload['groups']
    assert 'nonko_9max_pref' in groups
    
    group = groups['nonko_9max_pref']
    assert 'weight' in group
    assert 'subgroups' in group
    
    # Check that RFI stats are in RFI subgroup  
    subgroups = group['subgroups']
    assert 'RFI' in subgroups
    
    rfi_subgroup = subgroups['RFI']
    assert 'weight' in rfi_subgroup
    assert 'stats' in rfi_subgroup
    
    # Check specific stat
    stats = rfi_subgroup['stats']
    assert 'RFI_EARLY' in stats
    
    rfi_early = stats['RFI_EARLY']
    assert rfi_early['opps'] == 10
    assert rfi_early['att'] == 3
    assert rfi_early['pct'] == 30.0
    assert 'weight' in rfi_early
    assert 'score' in rfi_early
    # Score might be None if not computed or 85 if loaded from scorecard
    assert rfi_early['score'] in [None, 85]
    
    # Cleanup
    import shutil
    if test_dir.exists():
        shutil.rmtree(test_dir)


def test_payload_with_empty_data(client, app_context):
    """Test payload structure with no stats data"""
    # Create minimal test directory
    test_dir = Path('runs/empty_token')
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # Call the endpoint
    response = client.get('/api/dashboard/payload?token=empty_token')
    assert response.status_code == 200
    
    payload = response.get_json()
    
    # Should still have structure but empty/default values
    assert 'overall' in payload
    assert payload['overall'] in [0, None]  # Can be 0 or None when empty
    assert 'groups' in payload
    assert payload['groups'] == {}
    assert 'ingest' in payload
    assert payload['ingest'] == {}
    
    # Cleanup
    import shutil
    if test_dir.exists():
        shutil.rmtree(test_dir)