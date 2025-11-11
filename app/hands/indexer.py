# app/hands/indexer.py
import os, json, logging
from typing import Dict

logger = logging.getLogger("hands.indexer")

def build_index(hands_jsonl: str, out_path: str) -> Dict:
    """
    Lê parsed/hands_enriched.jsonl e gera um índice:
      hand_id -> { offset, file_id, tournament_id, site }
    """
    index = {"meta": {"input": hands_jsonl, "count": 0}, "map": {}}
    with open(hands_jsonl, "rb") as f:
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                obj = json.loads(line.decode("utf-8"))
                hid = obj.get("hand_id") or obj.get("id")
                if not hid:
                    continue
                index["map"][hid] = {
                    "offset": pos,
                    "file_id": obj.get("file_id"),
                    "tournament_id": obj.get("tournament_id"),
                    "site": obj.get("site"),
                }
                index["meta"]["count"] += 1
            except Exception as e:
                logger.warning(f"Erro a indexar @ {pos}: {e}")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fo:
        json.dump(index, fo, ensure_ascii=False, indent=2)
    return index

def fetch_by_id(hands_jsonl: str, index: Dict, hand_id: str) -> dict:
    entry = (index or {}).get("map", {}).get(hand_id)
    if not entry:
        return {}
    with open(hands_jsonl, "rb") as f:
        f.seek(entry["offset"])
        line = f.readline()
    return json.loads(line.decode("utf-8"))