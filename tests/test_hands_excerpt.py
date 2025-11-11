import os, json, tempfile
from app.hands.service import build_excerpt, SAFE_ROOTS

def test_excerpt_basic(tmp_path, monkeypatch):
    # cria HH fake
    hh = "line0\nHAND START\nHero raises to 2bb\nVillain folds\nHAND END\n"
    src = tmp_path/"NON-KO"/"file1.txt"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text(hh, encoding="utf-8")

    # força root seguro para o tmp
    monkeypatch.setitem(globals(),"SAFE_ROOTS",[str(tmp_path)])

    # offsets: "HAND START" começa no índice após "line0\n" (6)
    hand = {
        "file_id": os.path.relpath(str(src), start=str(tmp_path)),
        "hero": "Hero",
        "raw_offsets": {"hand_start": 6, "hand_end": len(hh)-1}
    }
    ex = build_excerpt(hand, context_chars=10)
    assert "HERO:Hero" in ex["snippet"] or "[HERO:Hero]" in ex["snippet"]