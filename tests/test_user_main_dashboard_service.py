import importlib
import json
import sys
import types

import pytest

try:  # pragma: no cover - import real dependency when available
    importlib.import_module("yaml")
except ImportError:  # pragma: no cover - fallback stub
    sys.modules["yaml"] = types.SimpleNamespace(safe_load=lambda stream: json.loads(stream))

try:  # pragma: no cover - import real dependency when available
    importlib.import_module("pydantic")
except ImportError:  # pragma: no cover - fallback stub
    class _BaseModel:  # noqa: D401 - minimal stub
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    sys.modules["pydantic"] = types.SimpleNamespace(BaseModel=_BaseModel)

try:  # pragma: no cover - import real dependency when available
    importlib.import_module("app.services.user_months_service")
except ImportError:  # pragma: no cover - fallback stub
    user_months_stub = types.ModuleType("app.services.user_months_service")

    class _UserMonthsServiceStub:
        def get_user_months_map(self, _user_id: str):
            return {}

    user_months_stub.UserMonthsService = _UserMonthsServiceStub
    sys.modules["app.services.user_months_service"] = user_months_stub

try:  # pragma: no cover - import real dependency when available
    importlib.import_module("app.api_dashboard")
except ImportError:  # pragma: no cover - fallback stub
    api_dashboard_stub = types.ModuleType("app.api_dashboard")

    def _unconfigured_build_user_month_dashboard_payload(*_args, **_kwargs):
        raise RuntimeError("build_user_month_dashboard_payload stub should be patched in tests")

    api_dashboard_stub.build_user_month_dashboard_payload = _unconfigured_build_user_month_dashboard_payload
    sys.modules["app.api_dashboard"] = api_dashboard_stub

from app.services import user_main_dashboard_service as service

MONTHS = ["2024-02", "2024-01", "2023-12"]
MONTH_INDEX = {month: idx for idx, month in enumerate(MONTHS)}


def _value_for_month(base: int, step: int, month: str) -> int:
    return base - step * MONTH_INDEX[month]


def _build_main_stat(opportunities: int, attempts: int, score: float) -> dict:
    pct = round((attempts / opportunities) * 100, 2) if opportunities else None
    return {
        "opportunities": opportunities,
        "attempts": attempts,
        "percentage": pct,
        "score": score,
        "ideal": [40, 50],
    }


def _build_postflop_stat(opportunities: int, attempts: int, score: float) -> dict:
    pct = round((attempts / opportunities) * 100, 2) if opportunities else None
    return {
        "opps": opportunities,
        "att": attempts,
        "pct": pct,
        "score": score,
        "ideal": [55, 65],
        "weight": 0.25,
    }


def _build_preflop_group(label: str, month: str, *, hands_base: int, hands_step: int,
                         opp_base: int, opp_step: int, att_base: int, att_step: int,
                         score_base: float) -> dict:
    hands = _value_for_month(hands_base, hands_step, month)
    opportunities = _value_for_month(opp_base, opp_step, month)
    attempts = _value_for_month(att_base, att_step, month)
    score = score_base - MONTH_INDEX[month]

    return {
        "label": label,
        "hands_count": hands,
        "stats": {
            "Early RFI": _build_main_stat(opportunities, attempts, score)
        },
        "subgroups": {
            "RFI": {"score": score, "weight": 0.2}
        },
        "overall_score": score,
    }


def _build_postflop_group(month: str) -> dict:
    idx = MONTH_INDEX[month]
    flop_opps = _value_for_month(60, 10, month)
    flop_attempts = _value_for_month(36, 6, month)
    vs_opps = _value_for_month(50, 10, month)
    vs_attempts = _value_for_month(15, 3, month)

    return {
        "label": "POSTFLOP",
        "hands_count": _value_for_month(140, 20, month),
        "stats": {},
        "subgroups": {
            "Flop Cbet": {
                "weight": 0.2,
                "score": 70 - idx,
                "stats": {
                    "POST_CBET_FLOP_IP": _build_postflop_stat(flop_opps, flop_attempts, 72 - idx)
                },
            },
            "Vs Cbet": {
                "weight": 0.2,
                "score": 64 - idx,
                "stats": {
                    "POST_FOLD_VS_CBET": _build_postflop_stat(vs_opps, vs_attempts, 62 - idx)
                },
            },
        },
        "overall_score": 66 - idx,
    }


def _build_sample_month_payload(month: str) -> dict:
    return {
        "groups": {
            "nonko_9max": _build_preflop_group(
                "NON-KO 9-max",
                month,
                hands_base=200,
                hands_step=30,
                opp_base=150,
                opp_step=20,
                att_base=45,
                att_step=6,
                score_base=82,
            ),
            "nonko_6max": _build_preflop_group(
                "NON-KO 6-max",
                month,
                hands_base=140,
                hands_step=20,
                opp_base=100,
                opp_step=15,
                att_base=40,
                att_step=6,
                score_base=78,
            ),
            "pko": _build_preflop_group(
                "PKO",
                month,
                hands_base=110,
                hands_step=15,
                opp_base=70,
                opp_step=10,
                att_base=21,
                att_step=3,
                score_base=75,
            ),
            "postflop_all": _build_postflop_group(month),
        },
        "weighted_scores": {
            "nonko": {"group_score": 80 - MONTH_INDEX[month]},
            "pko": {"group_score": 74 - MONTH_INDEX[month]},
            "postflop": {"group_score": 70 - MONTH_INDEX[month]},
            "overall": 77 - MONTH_INDEX[month],
        },
    }


@pytest.fixture
def sample_monthly_payloads(monkeypatch):
    monthly_payloads = {month: _build_sample_month_payload(month) for month in MONTHS}
    months_map = {month: [f"token_{month}"] for month in MONTHS}

    class StubMonthsService:
        def get_user_months_map(self, user_id):
            return months_map

    monkeypatch.setattr(service, "UserMonthsService", lambda: StubMonthsService())

    def fake_month_payload(user_id: str, month: str) -> dict:
        return monthly_payloads[month]

    monkeypatch.setattr(service, "build_user_month_dashboard_payload", fake_month_payload)
    return monthly_payloads


def test_main_dashboard_separates_table_formats(sample_monthly_payloads):
    payload = service.build_user_main_dashboard_payload("user-test")
    groups = payload["groups"]

    nonko9 = groups["nonko_9max"]
    nonko6 = groups["nonko_6max"]

    expected_nonko9_hands = sum(sample_monthly_payloads[m]["groups"]["nonko_9max"]["hands_count"] for m in MONTHS)
    expected_nonko6_hands = sum(sample_monthly_payloads[m]["groups"]["nonko_6max"]["hands_count"] for m in MONTHS)

    assert nonko9["hands_count"] == expected_nonko9_hands
    assert nonko6["hands_count"] == expected_nonko6_hands
    assert nonko9["hands_count"] != nonko6["hands_count"]

    expected_rfi_9max = sum(
        sample_monthly_payloads[m]["groups"]["nonko_9max"]["stats"]["Early RFI"]["opportunities"]
        for m in MONTHS
    )
    expected_rfi_6max = sum(
        sample_monthly_payloads[m]["groups"]["nonko_6max"]["stats"]["Early RFI"]["opportunities"]
        for m in MONTHS
    )

    assert nonko9["stats"]["Early RFI"]["opportunities"] == expected_rfi_9max
    assert nonko6["stats"]["Early RFI"]["opportunities"] == expected_rfi_6max
    assert len(nonko9["stats"]["Early RFI"]["frequencies_by_month"]) == len(MONTHS)
    assert len(nonko6["stats"]["Early RFI"]["frequencies_by_month"]) == len(MONTHS)


def test_postflop_tree_is_preserved(sample_monthly_payloads):
    payload = service.build_user_main_dashboard_payload("user-test")
    postflop = payload["groups"]["postflop_all"]

    flop_subgroup = postflop["subgroups"].get("Flop Cbet")
    assert flop_subgroup is not None
    assert "stats" in flop_subgroup
    assert "POST_CBET_FLOP_IP" in flop_subgroup["stats"]

    expected_frequencies = len(MONTHS)
    expected_opps = sum(
        sample_monthly_payloads[m]["groups"]["postflop_all"]["subgroups"]["Flop Cbet"]["stats"]["POST_CBET_FLOP_IP"]["opps"]
        for m in MONTHS
    )

    aggregated_stat = flop_subgroup["stats"]["POST_CBET_FLOP_IP"]
    assert aggregated_stat["opps"] == expected_opps
    assert len(aggregated_stat["frequencies_by_month"]) == expected_frequencies


def test_pko_opportunities_respect_volume(sample_monthly_payloads):
    payload = service.build_user_main_dashboard_payload("user-test")
    pko_group = payload["groups"]["pko"]

    total_hands = pko_group["hands_count"]
    expected_opps = sum(
        sample_monthly_payloads[m]["groups"]["pko"]["stats"]["Early RFI"]["opportunities"]
        for m in MONTHS
    )

    aggregated_stat = pko_group["stats"]["Early RFI"]
    assert aggregated_stat["opportunities"] == expected_opps
    assert aggregated_stat["opportunities"] <= total_hands
