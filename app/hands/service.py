# app/hands/service.py
import os, json, logging, chardet
from typing import Optional

logger = logging.getLogger("hands.service")

# Ajusta se necessário conforme a tua árvore de deploy
SAFE_ROOTS = [
    "CLASSIFIED",
    "parsed",
    "/tmp",
]

def is_safe_path(p: str) -> bool:
    p = os.path.abspath(p)
    for root in SAFE_ROOTS:
        if os.path.exists(root) and os.path.abspath(p).startswith(os.path.abspath(root)):
            return True
    return False

def _read_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def read_text_auto(path: str) -> str:
    raw = _read_bytes(path)
    det = chardet.detect(raw) or {}
    enc = det.get("encoding") or "utf-8"
    try:
        return raw.decode(enc, errors="replace")
    except Exception:
        return raw.decode("utf-8", errors="replace")

def slice_by_offsets(text: str, start: int, end: int, context: int = 200) -> str:
    s = max(0, start - context)
    e = min(len(text), end + context)
    return text[s:e]

def find_source_path(file_id: Optional[str]) -> Optional[str]:
    if not file_id:
        return None
    if os.path.isabs(file_id) and os.path.exists(file_id):
        return file_id
    # tenta combinar com roots seguras
    for root in SAFE_ROOTS:
        guess = os.path.join(root, file_id)
        if os.path.exists(guess):
            return guess
    # fallback por nome
    fname = os.path.basename(file_id)
    for root in SAFE_ROOTS:
        for base, _, files in os.walk(root):
            if fname in files:
                return os.path.join(base, fname)
    return None

def build_excerpt(hand: dict, context_chars: int = 200) -> dict:
    file_id = hand.get("file_id")
    raw_off = (hand.get("raw_offsets") or {})
    hstart = int(raw_off.get("hand_start", -1))
    hend   = int(raw_off.get("hand_end", -1))

    if hstart < 0 or hend < 0:
        return {"error": "Offsets indisponíveis"}

    src = find_source_path(file_id)
    if not src or not is_safe_path(src):
        return {"error": "Ficheiro original não encontrado/fora de raiz segura"}

    text = read_text_auto(src)
    snippet = slice_by_offsets(text, hstart, hend, context=context_chars)

    hero = hand.get("hero")
    if hero:
        snippet = snippet.replace(hero, f"[HERO:{hero}]")

    return {
        "file": src,
        "from": hstart, "to": hend,
        "length": max(0, hend - hstart),
        "snippet": snippet
    }