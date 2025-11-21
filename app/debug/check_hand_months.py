"""Utilities to inspect month aggregation and hand timestamps for a token."""
from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

from app.partition.months import month_bucket, parse_hand_datetime
from app.services.result_storage import get_result_storage

logger = logging.getLogger(__name__)


def inspect_months_for_token(token: str) -> None:
    """Print hands_per_month and manifest information for a token."""

    rs = get_result_storage()
    result = rs.get_pipeline_result(token)

    if not result:
        print(f"Nenhum pipeline_result encontrado para token={token}")
        return

    print("=== AGGREGATE pipeline_result keys:", list(result.keys()))
    counts = result.get("hands_per_month") or result.get("counts", {}).get("hands_per_month") or {}

    print("=== hands_per_month (do pipeline_result) ===")
    for month in sorted(counts.keys()):
        print(f"{month}: {counts[month]}")

    print("=== RAW hands_per_month JSON ===")
    print(json.dumps(counts, indent=2, ensure_ascii=False))

    base = Path("runs") / token
    manifest_path = base / "manifest.json"
    if manifest_path.exists():
        print("=== manifest.json encontrado em", manifest_path, "===")
        try:
            manifest = json.load(manifest_path.open("r", encoding="utf-8"))
            months = manifest.get("months") or []
            print("Meses no manifest:")
            for entry in months:
                print(entry)
        except Exception as exc:  # noqa: BLE001 - debug helper
            print("Erro a ler manifest.json:", exc)


def _iter_hands_from_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except Exception as exc:  # noqa: BLE001 - best-effort debug
                logger.debug("Falha a ler linha em %s: %s", path, exc)


def _candidate_hand_files(token: str, months: Optional[Iterable[str]] = None) -> List[Path]:
    rs = get_result_storage()
    token_dir = getattr(rs, "_normalize_token", lambda x: x)(token)

    candidates: List[Path] = []
    possible_roots = [
        Path("runs") / token,
        Path("results") / token,
        Path("results") / token_dir,
        Path("work") / token,
        Path("work") / token_dir,
    ]

    for root in possible_roots:
        for fname in (
            "hands_enriched.jsonl",
            "hands.jsonl",
            "parsed/hands_enriched.jsonl",
            "parsed/hands.jsonl",
        ):
            candidate = root / fname
            if candidate.exists():
                candidates.append(candidate)

        if months:
            for month in months:
                candidate = root / "months" / month / "hands_enriched.jsonl"
                if candidate.exists():
                    candidates.append(candidate)

    # Remove duplicates while preserving order
    seen: set[Path] = set()
    ordered: List[Path] = []
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        ordered.append(path)
    return ordered


def _iter_raw_text_hands(raw_dir: Path) -> Iterator[Dict[str, Any]]:
    txt_files = sorted(raw_dir.rglob("*.txt"))
    hand_splitter = re.compile(r"\n{2,}")
    for txt_file in txt_files:
        try:
            content = txt_file.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:  # noqa: BLE001 - debug helper
            logger.debug("Não foi possível ler %s: %s", txt_file, exc)
            continue

        for idx, chunk in enumerate(hand_splitter.split(content)):
            snippet = chunk.strip()
            if len(snippet) < 10:
                continue

            yield {
                "raw_text": snippet,
                "file_id": str(txt_file),
                "hand_index": idx,
            }


def _load_hand_records(token: str, months: Optional[Iterable[str]] = None) -> Iterator[Dict[str, Any]]:
    hand_files = _candidate_hand_files(token, months=months)
    for path in hand_files:
        print(f"=== A ler mãos de {path} ===")
        yield from _iter_hands_from_jsonl(path)

    raw_dir = Path("runs") / token / "raw"
    if raw_dir.exists():
        print(f"=== A ler mãos cruas de {raw_dir} ===")
        yield from _iter_raw_text_hands(raw_dir)


def sample_hand_dates_for_token(token: str, limit: int = 20) -> None:
    """Print sample hands with timestamp and month_key for a token."""

    rs = get_result_storage()
    aggregate = rs.get_pipeline_result(token) or {}
    months = list((aggregate.get("hands_per_month") or {}).keys())

    print(f"=== Amostras de mãos para token={token} (máx {limit}) ===")
    count = 0
    months_seen: set[str] = set()

    for hand in _load_hand_records(token, months=months):
        ts = hand.get("timestamp_utc") or hand.get("ts") or hand.get("datetime")
        if not ts:
            ts = parse_hand_datetime(hand) or ""
        month_key = hand.get("month") or hand.get("month_key")
        if not month_key:
            month_key = month_bucket(ts, debug_context="debug-sample")
        raw = hand.get("raw_line") or hand.get("raw") or hand.get("raw_text") or hand.get("hand_id")

        print("-" * 60)
        print("RAW:", repr(raw)[:200])
        print("timestamp_utc:", ts)
        print("month_key:", month_key)

        if month_key:
            months_seen.add(month_key)

        count += 1
        if count >= limit:
            break

    print("=== Meses vistos nestas amostras:", sorted(months_seen), "===")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("token", help="Upload token (runs/<token>)")
    parser.add_argument(
        "--samples",
        action="store_true",
        help="Imprimir amostras de mãos com timestamp e month_key",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Número de mãos a mostrar no modo de amostras",
    )
    args = parser.parse_args()

    inspect_months_for_token(args.token)

    if args.samples:
        sample_hand_dates_for_token(args.token, limit=args.limit)


if __name__ == "__main__":
    main()
