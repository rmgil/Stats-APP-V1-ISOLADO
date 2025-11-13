"""
CLI entry point for partition module.

Usage:
    python -m app.partition --input hands.jsonl --output partitions/ --type month
    python -m app.partition --input hands.jsonl --output partitions/ --type group --group-by hero
    python -m app.partition --input hands.jsonl --output partitions/ --type group --group-fields hero site pot_type
"""
from app.partition.runner import main

if __name__ == '__main__':
    main()