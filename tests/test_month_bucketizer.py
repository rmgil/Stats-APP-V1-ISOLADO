from pathlib import Path

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


def write_sample_file(path: Path, hands: list[str]):
    content = "\n\n".join(hands)
    path.write_text(content, encoding="utf-8")


def test_build_month_buckets_groups_by_hand_dates(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    work_root = tmp_path / "work"

    hand_january = SAMPLE_HAND_TEMPLATE.format(
        hand_id="1",
        tournament_id="999",
        timestamp="2024/01/15 12:34:56"
    )

    hand_february = SAMPLE_HAND_TEMPLATE.format(
        hand_id="2",
        tournament_id="888",
        timestamp="2024/02/20 18:00:00"
    )

    duplicate_january = SAMPLE_HAND_TEMPLATE.format(
        hand_id="1",
        tournament_id="999",
        timestamp="2024/01/15 12:34:56"
    )

    sample_file = input_dir / "sample.txt"
    write_sample_file(sample_file, [hand_january, hand_february, duplicate_january])

    buckets = build_month_buckets("token123", str(input_dir), str(work_root))

    assert len(buckets) == 2
    months = sorted(bucket.month for bucket in buckets)
    assert months == ["2024-01", "2024-02"]

    jan_bucket = next(b for b in buckets if b.month == "2024-01")
    feb_bucket = next(b for b in buckets if b.month == "2024-02")

    # Ensure files were written
    jan_bucket.finalize()
    feb_bucket.finalize()

    jan_contents = "\n".join(Path(file).read_text(encoding="utf-8") for file in jan_bucket.files)
    feb_contents = "\n".join(Path(file).read_text(encoding="utf-8") for file in feb_bucket.files)

    assert "2024/01/15" in jan_contents
    assert "2024/02/20" in feb_contents

    # Duplicate hand should only appear once
    assert jan_contents.count("PokerStars Hand #1") == 1
