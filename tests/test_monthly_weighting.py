from typing import Dict, Any

from app.api_dashboard import compute_weighted_scores_for_month_selection


class StubResultStorage:
    def __init__(self, manifest: Dict[str, Any], results: Dict[str, Dict[str, Any]]):
        self._manifest = manifest
        self._results = results

    def get_months_manifest(self, token: str) -> Dict[str, Any]:
        return self._manifest

    def get_pipeline_result(self, token: str, month: str):
        return self._results[month]


def make_pipeline_result(overall_score: float) -> Dict[str, Any]:
    group = {
        'hand_count': 100,
        'overall_score': overall_score,
        'scores': {
            'rfi': {'overall_score': overall_score}
        }
    }

    pko = {
        'hand_count': 50,
        'overall_score': overall_score - 5,
        'scores': {
            'rfi': {'overall_score': overall_score - 5}
        }
    }

    return {
        'status': 'completed',
        'combined': {
            'nonko_9max': group,
            'nonko_6max': group,
            'pko': pko
        },
        'valid_hands': 200,
        'total_hands': 200
    }


def test_compute_weighted_scores_for_month_selection():
    manifest = {
        'months': [
            {'month': '2023-12'},
            {'month': '2024-01'},
            {'month': '2024-02'}
        ]
    }

    results = {
        '2023-12': make_pipeline_result(60),
        '2024-01': make_pipeline_result(80),
        '2024-02': make_pipeline_result(90)
    }

    storage = StubResultStorage(manifest, results)

    weighting = compute_weighted_scores_for_month_selection(
        token='token',
        selected_month='2024-02',
        result_storage=storage,
        ideals={},
        stat_weights={}
    )

    assert weighting is not None
    weighted_scores = weighting['weighted_scores']
    nonko_score = weighted_scores['nonko']['group_score']

    # Expected: 0.5*90 + 0.3*80 + 0.2*60 = 81
    assert abs(nonko_score - 81) < 0.5

    months_used = weighting['months_used']
    assert len(months_used) == 3
    weights = {entry['month']: entry['normalized_weight'] for entry in months_used}
    assert abs(weights['2024-02'] - 0.5) < 0.01
    assert abs(weights['2024-01'] - 0.3) < 0.01
    assert abs(weights['2023-12'] - 0.2) < 0.01
