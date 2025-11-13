from pathlib import Path

from pathlib import Path

from app.parse.runner import ParserRunner
from app.pipeline.month_bucketizer import build_month_buckets


SAMPLE_HAND_TEMPLATE = """PokerStars Hand #{hand_id}: Tournament #{tournament_id}, $10+$1 USD Hold'em No Limit - Level I (10/20) - {timestamp} ET
Table '12345 1' 9-max Seat #1 is the button
Seat 1: Player1 (1500 in chips)
Seat 2: Player2 (1500 in chips)
*** HOLE CARDS ***
Dealt to Player1 [Ah As]
Player1: posts small blind 10
Player2: posts big blind 20
Player1: raises 40 to 50
Player2: folds
Uncalled bet (30) returned to Player1
Player1 collected 40 from pot
*** SUMMARY ***
Total pot 40 | Rake 0
"""


def test_build_month_buckets_groups_by_first_hand_date(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    work_root = tmp_path / "work"

    january_text = SAMPLE_HAND_TEMPLATE.format(
        hand_id="1",
        tournament_id="999",
        timestamp="2024/01/15 12:34:56"
    )

    february_text = SAMPLE_HAND_TEMPLATE.format(
        hand_id="2",
        tournament_id="888",
        timestamp="2024/02/20 18:00:00"
    )

    # Write tournament files (one per tournament)
    (input_dir / "january.txt").write_text(january_text, encoding="utf-8")
    (input_dir / "january_duplicate.txt").write_text(january_text, encoding="utf-8")
    (input_dir / "february.txt").write_text(february_text, encoding="utf-8")

    parser = ParserRunner()
    buckets = build_month_buckets(
        "token123",
        str(input_dir),
        str(work_root),
        metadata_resolver=parser.extract_tournament_metadata,
    )

    assert len(buckets) == 2
    months = sorted(bucket.month for bucket in buckets)
    assert months == ["2024-01", "2024-02"]

    jan_bucket = next(b for b in buckets if b.month == "2024-01")
    feb_bucket = next(b for b in buckets if b.month == "2024-02")

    assert len(jan_bucket.files) == 1  # duplicate tournament deduped
    assert len(feb_bucket.files) == 1

    jan_contents = "\n".join(Path(file).read_text(encoding="utf-8") for file in jan_bucket.files)
    feb_contents = "\n".join(Path(file).read_text(encoding="utf-8") for file in feb_bucket.files)

    assert "2024/01/15" in jan_contents
    assert "2024/02/20" in feb_contents

    # Duplicate tournament should only be kept once
    assert jan_contents.count("PokerStars Hand #1") == 1
