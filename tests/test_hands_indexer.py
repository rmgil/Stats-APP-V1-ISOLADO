import json, tempfile, os
from app.hands.indexer import build_index, fetch_by_id

def test_build_and_fetch_index(tmp_path):
    # JSONL sintético
    p = tmp_path/"hands.jsonl"
    items = [
        {"hand_id":"H1","file_id":"A.txt","site":"ps","tournament_id":"T1"},
        {"hand_id":"H2","file_id":"B.txt","site":"gg","tournament_id":"T2"},
    ]
    with open(p,"w",encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it)+"\n")

    idx_path = tmp_path/"idx.json"
    idx = build_index(str(p), str(idx_path))
    assert idx["meta"]["count"] == 2

    got = fetch_by_id(str(p), idx, "H2")
    assert got["site"] == "gg"