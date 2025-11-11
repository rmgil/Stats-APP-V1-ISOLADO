# app/hands/api.py
import os, json
from flask import Blueprint, request, jsonify
from app.hands.indexer import build_index, fetch_by_id

bp = Blueprint("hands_api", __name__)

INDEX_PATH   = "parsed/hands_index.json"
HANDS_JSONL  = "parsed/hands_enriched.jsonl"

_INDEX_CACHE = None

def _ensure_index():
    global _INDEX_CACHE
    if _INDEX_CACHE is None:
        if not os.path.exists(INDEX_PATH):
            build_index(HANDS_JSONL, INDEX_PATH)
        _INDEX_CACHE = json.load(open(INDEX_PATH, "r", encoding="utf-8"))
    return _INDEX_CACHE

@bp.route("/api/hh/reindex", methods=["POST"])
def api_reindex():
    data = request.get_json(silent=True) or {}
    in_jsonl = data.get("in_jsonl", HANDS_JSONL)
    out_idx  = data.get("out_index", INDEX_PATH)
    res = build_index(in_jsonl, out_idx)
    global _INDEX_CACHE
    _INDEX_CACHE = res
    return jsonify({"success": True, "indexed": res["meta"]["count"], "index": out_idx})

@bp.route("/api/hh", methods=["GET"])
def api_get_hand():
    hand_id = request.args.get("id")
    if not hand_id:
        return jsonify({"error": "id em falta"}), 400
    idx = _ensure_index()
    if hand_id not in idx.get("map", {}):
        return jsonify({"error": "hand_id desconhecido"}), 404
    obj = fetch_by_id(HANDS_JSONL, idx, hand_id)
    keep = {k: obj.get(k) for k in [
        "hand_id","site","tournament_id","file_id","hero",
        "button_seat","table_max","blinds","derived","timestamp_utc","raw_offsets"
    ]}
    return jsonify(keep)

@bp.route("/api/hh/excerpt", methods=["GET"])
def api_get_excerpt():
    from app.hands.indexer import fetch_by_id
    from app.hands.service import build_excerpt

    hand_id = request.args.get("id")
    ctx = int(request.args.get("context", "200"))
    if not hand_id:
        return jsonify({"error": "id em falta"}), 400
    idx = _ensure_index()
    if hand_id not in idx.get("map", {}):
        return jsonify({"error": "hand_id desconhecido"}), 404
    obj = fetch_by_id(HANDS_JSONL, idx, hand_id)
    ex = build_excerpt(obj, context_chars=ctx)
    if "error" in ex:
        return jsonify(ex), 404
    return jsonify(ex)