"""Consolidated dashboard payload builder"""

# =============================================================================
# 🧭  Resumo da arquitetura final do dashboard (/dashboard/<token>)
#
# • Pipeline único: run_multi_site_pipeline (app/pipeline/multi_site_runner.py)
#   extrai o ZIP enviado, detecta as salas, agrupa por mês quando necessário e
#   chama classify_into_final_groups → process_files_hand_by_hand para gerar
#   todas as mãos válidas. A mesma base de “hands” alimenta o payload global
#   e os buckets mensais gravados em /results/<token>/months/<mes>/.
# • Agregação partilhada: MultiSiteAggregator (app/stats/aggregate.py) produz o
#   dicionário combined com nonko_9max/nonko_6max/pko e escreve os
#   postflop_stats. build_dashboard_payload consome exatamente esta estrutura
#   tanto no global como no mensal: carregamos pipeline_result(token) para o
#   agregado e pipeline_result(token, month=YYYY-MM) para o filtro mensal, mas
#   ambos passam por aggregate_postflop_stats + calculate_weighted_scores_from_groups
#   para obter NON-KO, PKO, POSTFLOP e overall_score com as mesmas regras.
# • Persistência: o pipeline grava work/<token>/pipeline_result_global.json e
#   publica /results/<token>/pipeline_result.json (global) +
#   /results/<token>/months/<mes>/pipeline_result.json (mensal). As amostras de
#   mãos ficam em hands_by_stat/<grupo>/<stat>.txt com metadata.json, partilhadas
#   entre os dois modos.
# • Inputs auxiliares: stat_counts.json (contagens por stat) e
#   scores/scorecard.json (notas e scores) são combinados com o pipeline_result
#   para gerar groups, weighted_scores, downloads, etc.
#
# Fluxo global detalhado
# ----------------------
# 1. Upload → fila: o endpoint de upload grava o ZIP na JobQueueService e o
#    SimpleBackgroundWorker (_process_job em
#    app/services/simple_background_worker.py) reclama o job. Ele prepara a
#    pasta /tmp/processing_<token>, atualiza progresso e invoca
#    run_multi_site_pipeline.
# 2. Extração + deteção de salas: run_multi_site_pipeline extrai o ZIP, usa
#    ParserRunner para extrair metadados, constrói buckets mensais quando
#    existem múltiplos meses e detecta as salas presentes
#    (detect_sites_in_directory).
# 3. Parsers e filtros: para cada sala/mês, classify_into_final_groups
#    (app/classify/group_classifier.py) chama process_files_hand_by_hand, que
#    usa split_into_hands_with_stats + classify_hand_format para gerar “hands”.
#    São descartadas mystery, cash games, mãos <4 jogadores, resumos de
#    torneio e formatos inválidos
#    (app/classify/hand_by_hand_classifier.py).
# 4. Cálculo de stats por sala: para cada grupo final (nonko_9max,
#    nonko_6max, pko) o pipeline agrega as mãos válidas e corre
#    PreflopStats + PostflopCalculatorV3, guardando oportunidades/tentativas e
#    cada hand em HandCollector (app/stats/preflop_stats.py e
#    app/stats/postflop_calculator_v3.py).
# 5. Agregação multi-sala/mês: MultiSiteAggregator junta os stats de todas as
#    salas, calcula percentage/score por stat, escreve as mãos combinadas e
#    devolve {'overall_score', 'stats', 'hand_count', 'postflop_hands_count',
#    'scores', 'sites_included'} por grupo. O resultado global acumula todos os
#    meses em result_data['combined']; cada pipeline mensal grava a mesma
#    agregação aplicada ao seu subconjunto de mãos.
# 6. Persistência: o pipeline grava pipeline_result_global.json (global) e, se
#    houver buckets mensais, pipeline_result.json dentro de months/<mes>/.
#
# Estruturas-chave manipuladas nesta camada
# -----------------------------------------
# • “hand”: string com a hand history crua; guardada por stat no HandCollector
#   e exposta ao frontend via hands_by_stat/*.txt (mesma base global/mensal).
# • “stat”: dicionário com opportunities/attempts/percentage e, após scoring,
#   score/ideal/weight. No pipeline_result vive em
#   result['combined'][grupo]['stats'][nome_do_stat].
# • “group”: chave lógica (nonko_9max, nonko_6max, pko, postflop_all) que
#   agrega stats, contadores e scores para um formato específico.
#
# Onde está o payload global e como é usado
# -----------------------------------------
# • ResultStorageService lê pipeline_result.json (global) ou o equivalente
#   mensal. Ambos trazem classification.discarded_hands, aggregated_discards,
#   valid_hands/total_hands, sites e combined.
# • build_dashboard_payload funde esse JSON com stat_counts.json e
#   scores/scorecard.json.
#   - Header: usa classification/aggregated_discards para Total encontradas,
#     Válidas (= total − descartadas), Mystery e <4 jogadores (chaves
#     'mystery' e 'less_than_4_players').
#   - Separação NON-KO/PKO/Postflop: vem dos grupos de combined; NON-KO é a
#     soma ponderada de nonko_9max e nonko_6max.
#   - 6-max/9-max: herdado de classify_hand_format → group_classifier, que
#     define em hands_per_group quantas mãos pertencem a cada variante.
#   - Postflop: aggregate_postflop_stats junta todos os postflop_stats dos
#     grupos para construir postflop_all com a mesma base global/mensal.
#
# Como os scores e notas são obtidos
# ----------------------------------
# • overall_score global: calculate_weighted_scores_from_groups mistura os
#   scores NON-KO/PKO/Postflop com pesos 80/20. compute_weighted_scores_for_month_selection
#   aplica time-decay (50/30/20) mas reutiliza o mesmo cálculo de grupos.
# • Scores por grupo (NON-KO, PKO, POSTFLOP): vêm de
#   MultiSiteAggregator.aggregate_stats → overall_score e das categorias de
#   scores (rfi/bvb/3bet/…).
# • Notas por stat: build_scorecard (app/score/runner.py) chama
#   explain_stat/score_to_note e grava em scores/scorecard.json.
#
# Downloads de amostras
# ----------------------
# • Cada HandCollector grava metadata.json com contagem e IDs das mãos.
#   MultiSiteAggregator.write_combined_outputs escreve mãos agregadas em
#   work/<token>/hands_by_stat/<grupo>/<ficheiro>. Os endpoints
#   /api/download/hands_by_stat/<token>/... servem os mesmos ficheiros tanto
#   para o dashboard global como para filtragem mensal.
# =============================================================================

import json
import logging
import re
import time
import yaml
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from fastapi import APIRouter, Depends

from app.api.auth_dependencies import get_current_user
from app.models.user import User
from app.parse.site_parsers.site_detector import detect_poker_site
from app.score.scoring import score_step
from app.services.result_storage import ResultStorageService
from app.services.upload_service import UploadService
from app.services.user_months_service import LATEST_UPLOAD_KEY, UserMonthsService
from app.stats.stat_categories import CATEGORY_LABELS, CATEGORY_WEIGHTS

MONTH_KEY_PATTERN = re.compile(r"^\d{4}-\d{2}$")

EXPECTED_GROUP_KEYS = ['nonko_9max', 'nonko_6max', 'pko', 'postflop_all']
DEFAULT_GROUP_LABELS = {
    'nonko_9max': 'NON-KO 9-MAX',
    'nonko_6max': 'NON-KO 6-MAX',
    'pko': 'PKO',
    'postflop_all': 'POSTFLOP',
}

logger = logging.getLogger(__name__)

router = APIRouter()

result_storage = ResultStorageService()
user_months_service = UserMonthsService()
uploads_service = UploadService()


def ensure_group_defaults(groups: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure expected groups exist and include has_data flags."""

    if not isinstance(groups, dict):
        groups = {}

    for key in EXPECTED_GROUP_KEYS:
        group = groups.get(key)
        if not isinstance(group, dict):
            group = {}
            groups[key] = group

        group.setdefault('label', DEFAULT_GROUP_LABELS.get(key, key))
        group.setdefault('file_count', 0)
        group.setdefault('hands_count', 0)
        group.setdefault('postflop_hands_count', group.get('postflop_hands_count', 0))
        group.setdefault('stats', {})
        group.setdefault('postflop_stats', group.get('postflop_stats', {}))
        group.setdefault('overall_score', group.get('overall_score', 0) or 0)
        group.setdefault('subgroups', group.get('subgroups', {}))
        group.setdefault('scores', group.get('scores', {}))

        has_data = bool(group.get('hands_count'))
        has_data = has_data or bool(group.get('stats')) or bool(group.get('postflop_stats'))
        has_data = has_data or bool(group.get('postflop_hands_count'))
        group['has_data'] = has_data

    for key, value in groups.items():
        if isinstance(value, dict) and 'has_data' not in value:
            has_data = bool(value.get('hands_count'))
            has_data = has_data or bool(value.get('stats')) or bool(value.get('postflop_stats'))
            value['has_data'] = has_data

    return groups


# NOTE: This endpoint is intentionally excluded from the generic NotFound ->
# "Erro no processamento do ficheiro" mapping so we can inspect raw missing-data
# causes when debugging dashboard issues.
@router.get("/api/debug/user_main_state")
async def api_debug_user_main_state(current_user: User = Depends(get_current_user)):
    """
    INTERNAL DEBUG ENDPOINT.

    Returns a snapshot of the current user's dashboard-related state:
    uploads, pipeline results, dashboard cache, and months detected.
    This is meant for development/debugging, not for end users.
    """

    try:
        from app.services import user_main_dashboard_service

        user_id = current_user.get_id() if hasattr(current_user, "get_id") else None
        user_id = user_id or getattr(current_user, "id", None)
        snapshot = user_main_dashboard_service.get_user_main_debug_snapshot(
            user_id=str(user_id),
            result_storage=result_storage,
            user_months_service=user_months_service,
            uploads_repo=uploads_service,
        )
        return {"success": True, "data": snapshot}
    except Exception as exc:  # noqa: BLE001 - return debug friendly error
        logger.exception("Error in api_debug_user_main_state for user %s", getattr(current_user, "id", None))
        return {
            "success": False,
            "error": "internal_error",
            "detail": str(exc),
            "type": "debug_internal_error",
        }


@router.get("/api/debug/global-stats/{token}")
async def api_debug_global_stats(token: str):
    """Return raw debug counters for a given upload token."""

    try:
        pipeline_result = result_storage.get_pipeline_result(token)
    except FileNotFoundError:
        return {
            "success": False,
            "error": "not_found",
            "detail": f"pipeline_result not found for {token}",
        }
    except Exception as exc:  # noqa: BLE001 - expose debug-friendly detail
        logger.exception("[DEBUG] Failed to load pipeline_result for %s", token)
        return {
            "success": False,
            "error": "internal_error",
            "detail": str(exc),
        }

    classification = pipeline_result.get("classification") if isinstance(pipeline_result, dict) else {}
    discard_stats = (
        pipeline_result.get("aggregated_discards")
        or (classification or {}).get("discarded_hands")
        or pipeline_result.get("discarded_hands")
        or {}
    )

    debug_payload = pipeline_result.get("debug") if isinstance(pipeline_result, dict) else {}
    rooms = pipeline_result.get("rooms") if isinstance(pipeline_result, dict) else {}
    parsed_hands = debug_payload.get("parsed_hands") if isinstance(debug_payload, dict) else None

    totals = {
        "raw_lines": debug_payload.get("raw_lines") if isinstance(debug_payload, dict) else None,
        "parsed_hands": parsed_hands if isinstance(parsed_hands, (int, float)) else pipeline_result.get("total_hands", 0),
        "valid_hands": debug_payload.get("valid_hands") if isinstance(debug_payload, dict) else pipeline_result.get("valid_hands", 0),
        "mystery_hands": debug_payload.get("mystery_hands") if isinstance(debug_payload, dict) else discard_stats.get("mystery", 0),
        "lt4_hands": debug_payload.get("lt4_hands") if isinstance(debug_payload, dict) else discard_stats.get("less_than_4_players", 0),
        "discarded_no_reason": debug_payload.get("discarded_no_reason") if isinstance(debug_payload, dict) else 0,
    }

    try:
        parsed_int = int(totals.get("parsed_hands") or 0)
    except Exception:
        parsed_int = 0

    discard_total = discard_stats.get("total")
    if not isinstance(discard_total, (int, float)):
        discard_total = sum(int(v or 0) for k, v in discard_stats.items() if k != "total")

    missing = parsed_int - (int(totals.get("valid_hands") or 0) + int(discard_total or 0))
    if missing > (totals.get("discarded_no_reason") or 0):
        totals["discarded_no_reason"] = missing

    totals["raw_lines"] = totals.get("raw_lines") or parsed_int

    return {
        "success": True,
        "token": token,
        "raw_lines": int(totals.get("raw_lines") or 0),
        "parsed_hands": parsed_int,
        "valid_hands": int(totals.get("valid_hands") or 0),
        "mystery_hands": int(totals.get("mystery_hands") or 0),
        "lt4_hands": int(totals.get("lt4_hands") or 0),
        "discarded_no_reason": int(totals.get("discarded_no_reason") or 0),
        "by_room": rooms or (debug_payload.get("by_room") if isinstance(debug_payload, dict) else {}),
    }


def reset_groups_for_missing_data(groups: Dict[str, Any]) -> Dict[str, Any]:
    """Reset expected groups to an empty state with has_data=False."""

    groups = ensure_group_defaults(groups)
    for key in EXPECTED_GROUP_KEYS:
        group = groups.get(key, {})
        if not isinstance(group, dict):
            continue

        group['file_count'] = 0
        group['hands_count'] = 0
        group['postflop_hands_count'] = 0
        group['stats'] = {}
        group['postflop_stats'] = {}
        group['overall_score'] = 0
        group['subgroups'] = {}
        group['scores'] = {}
        group['has_data'] = False

    return groups


def detect_sites_from_hands(token: str, pipeline_data: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
    """Detect poker sites from pipeline_result['sites'] structure and aggregate hand counts"""
    import logging
    logger = logging.getLogger(__name__)

    hands_by_site = {}

    if pipeline_data and not isinstance(pipeline_data, dict):
        pipeline_sites = pipeline_data
        pipeline_data = {}
    else:
        pipeline_sites = (pipeline_data or {}).get('sites')

    room_summary = {}
    if isinstance(pipeline_data, dict):
        room_summary = pipeline_data.get('rooms') or (pipeline_data.get('debug') or {}).get('by_room', {})

    # Prefer explicit room summaries when available to keep the "MÃOS VÁLIDAS" card accurate
    if room_summary:
        for room_name, counts in room_summary.items():
            try:
                total_valid = int(counts.get('valid_hands') or 0)
            except Exception:
                total_valid = 0

            if total_valid <= 0:
                total_valid = int(counts.get('total_hands') or 0)

            if total_valid:
                hands_by_site[room_name] = total_valid
                logger.info(f"[SITE_DETECT] {room_name}: {total_valid} hands (rooms summary)")

    if hands_by_site:
        return hands_by_site

    if not pipeline_sites:
        logger.warning("[SITE_DETECT] No pipeline_sites provided")
        return hands_by_site
    
    # Iterate through sites (ggpoker, pokerstars, etc.)
    for site_name, site_data in pipeline_sites.items():
        if not isinstance(site_data, dict):
            continue
        
        logger.info(f"[SITE_DETECT] Processing site: {site_name}")
        
        # Iterate through groups (nonko_9max, nonko_6max, pko)
        total_hands = 0
        for group_name, group_data in site_data.items():
            if isinstance(group_data, dict) and 'hand_count' in group_data:
                hand_count = group_data['hand_count']
                total_hands += hand_count
                logger.info(f"[SITE_DETECT] {site_name}.{group_name}: {hand_count} hands")
        
        if total_hands > 0:
            hands_by_site[site_name] = total_hands
            logger.info(f"[SITE_DETECT] Total for {site_name}: {total_hands} hands")
    
    logger.info(f"[SITE_DETECT] Final hands_by_site: {hands_by_site}")
    return hands_by_site


def transform_group_for_frontend(group_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform pipeline_result group format to frontend expected format"""
    
    # Get base stats and scores
    base_stats = group_data.get('stats', {})
    scores_data = group_data.get('scores', {})
    
    # Merge scores into stats
    enriched_stats = {}
    for stat_name, stat_data in base_stats.items():
        # Copy base stat data
        enriched_stat = stat_data.copy()
        
        # Find score in score categories (rfi, bvb, threbet_cc, vs_3bet, squeeze, bb_defense, sb_defense)
        score_found = False
        for category_name, category_scores in scores_data.items():
            if not isinstance(category_scores, dict):
                continue
                
            # Check if category has .stats (like RFI)
            if 'stats' in category_scores and isinstance(category_scores['stats'], dict):
                # Search in .stats
                if stat_name in category_scores['stats']:
                    score_info = category_scores['stats'][stat_name]
                    if isinstance(score_info, dict):
                        enriched_stat['score'] = score_info.get('score')
                        enriched_stat['ideal'] = score_info.get('ideal')
                        enriched_stat['weight'] = score_info.get('weight')
                        score_found = True
                        break
            # Check if category has .details (like sb_defense, bb_defense)
            elif 'details' in category_scores and isinstance(category_scores['details'], dict):
                # Search in .details
                if stat_name in category_scores['details']:
                    score_info = category_scores['details'][stat_name]
                    if isinstance(score_info, dict):
                        enriched_stat['score'] = score_info.get('score')
                        enriched_stat['ideal'] = score_info.get('ideal')
                        enriched_stat['weight'] = score_info.get('weight')
                        score_found = True
                        break
            # Otherwise search directly in category
            elif stat_name in category_scores:
                score_info = category_scores[stat_name]
                if isinstance(score_info, dict):
                    enriched_stat['score'] = score_info.get('score')
                    enriched_stat['ideal'] = score_info.get('ideal')
                    enriched_stat['weight'] = score_info.get('weight')
                    score_found = True
                    break
        
        enriched_stats[stat_name] = enriched_stat
    
    # Build transformed structure
    transformed = {
        'hands_count': group_data.get('hand_count', group_data.get('total_hands', 0)),
        'stats': enriched_stats,
        'postflop_stats': group_data.get('postflop_stats', {}),
        'postflop_hands_count': group_data.get('postflop_hands_count', 0),
        'overall_score': group_data.get('overall_score', 0),
        'label': group_data.get('label', ''),
        'file_count': group_data.get('file_count', 0),
        'subgroups': {},
        'scores': scores_data,  # Preserve full scores structure
    }

    transformed['has_data'] = bool(transformed['hands_count']) or bool(enriched_stats) or bool(
        transformed.get('postflop_stats')
    ) or bool(transformed.get('postflop_hands_count'))

    return transformed


def calculate_weighted_scores_from_groups(pipeline_groups: Dict[str, Any], postflop_all: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Calculate weighted_scores for GERAL tab from pipeline groups"""
    
    weighted_scores = {}
    
    # Aggregate NON-KO (9max + 6max weighted by hands)
    nonko_9max = pipeline_groups.get('nonko_9max', {})
    nonko_6max = pipeline_groups.get('nonko_6max', {})
    
    hands_9max = nonko_9max.get('hand_count', 0)
    hands_6max = nonko_6max.get('hand_count', 0)
    total_nonko_hands = hands_9max + hands_6max
    
    if total_nonko_hands > 0:
        weight_9max = hands_9max / total_nonko_hands
        weight_6max = hands_6max / total_nonko_hands
        
        # Use overall_score directly from each group (calculated by MultiSiteAggregator)
        score_9max = nonko_9max.get('overall_score', 0)
        score_6max = nonko_6max.get('overall_score', 0)
        
        # Weighted average of 9max and 6max overall scores
        nonko_score = score_9max * weight_9max + score_6max * weight_6max
        
        # Build subgroups for display (optional, for breakdown)
        nonko_subgroups = {}
        for category, weight in CATEGORY_WEIGHTS.items():
            category_score_9max = nonko_9max.get('scores', {}).get(category, {}).get('overall_score') or \
                                 nonko_9max.get('scores', {}).get(category, {}).get('_weighted_average') or \
                                 nonko_9max.get('scores', {}).get(category, {}).get('score') or 0

            category_score_6max = nonko_6max.get('scores', {}).get(category, {}).get('overall_score') or \
                                 nonko_6max.get('scores', {}).get(category, {}).get('_weighted_average') or \
                                 nonko_6max.get('scores', {}).get(category, {}).get('score') or 0

            category_score = (category_score_9max * weight_9max + category_score_6max * weight_6max) if (category_score_9max or category_score_6max) else 0

            label = CATEGORY_LABELS.get(category)

            if label:
                nonko_subgroups[label] = {
                    'score': category_score,
                    'weight': weight
                }
        
        weighted_scores['nonko'] = {
            'group_score': round(nonko_score, 1),
            'subgroups': nonko_subgroups,
            'weight_9max': int(weight_9max * 100),
            'weight_6max': int(weight_6max * 100)
        }
    
    # PKO scores
    pko = pipeline_groups.get('pko', {})
    if pko.get('hand_count', 0) > 0:
        # Use overall_score directly from PKO group (calculated by MultiSiteAggregator)
        pko_score = pko.get('overall_score', 0)
        
        # Build subgroups for display (optional, for breakdown)
        pko_subgroups = {}
        for category, weight in CATEGORY_WEIGHTS.items():
            score = pko.get('scores', {}).get(category, {}).get('overall_score') or \
                   pko.get('scores', {}).get(category, {}).get('_weighted_average') or \
                   pko.get('scores', {}).get(category, {}).get('score') or 0

            label = CATEGORY_LABELS.get(category)

            if label:
                pko_subgroups[label] = {
                    'score': score,
                    'weight': weight
                }
        
        weighted_scores['pko'] = {
            'group_score': round(pko_score, 1),
            'subgroups': pko_subgroups
        }
    
    # Add POSTFLOP score to weighted_scores if provided
    if postflop_all and 'overall_score' in postflop_all:
        weighted_scores['postflop'] = {
            'group_score': postflop_all['overall_score'],
            'subgroups': {}  # Subgroups already in postflop_all
        }
    
    # Calculate overall weighted score with new formula:
    # POSTFLOP = 20% fixed, remaining 80% distributed proportionally between NON-KO and PKO
    if weighted_scores:
        total_hands = total_nonko_hands + pko.get('hand_count', 0)
        if total_hands > 0:
            # Calculate proportional weights for the 80% remaining (after POSTFLOP's 20%)
            nonko_proportion = total_nonko_hands / total_hands
            pko_proportion = pko.get('hand_count', 0) / total_hands
            
            # Apply 80% to proportional weights
            nonko_weight = nonko_proportion * 0.80
            pko_weight = pko_proportion * 0.80
            postflop_weight = 0.20  # Fixed 20%
            
            overall_score = 0
            if 'nonko' in weighted_scores:
                overall_score += weighted_scores['nonko']['group_score'] * nonko_weight
            if 'pko' in weighted_scores:
                overall_score += weighted_scores['pko']['group_score'] * pko_weight
            if 'postflop' in weighted_scores:
                overall_score += weighted_scores['postflop']['group_score'] * postflop_weight
            
            weighted_scores['overall'] = overall_score
            
            # Debug log for overall calculation
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"[OVERALL CALC] NON-KO: {weighted_scores.get('nonko', {}).get('group_score', 0):.1f} × {nonko_weight:.3f} = {weighted_scores.get('nonko', {}).get('group_score', 0) * nonko_weight:.1f}")
            logger.info(f"[OVERALL CALC] PKO: {weighted_scores.get('pko', {}).get('group_score', 0):.1f} × {pko_weight:.3f} = {weighted_scores.get('pko', {}).get('group_score', 0) * pko_weight:.1f}")
            logger.info(f"[OVERALL CALC] POSTFLOP: {weighted_scores.get('postflop', {}).get('group_score', 0):.1f} × {postflop_weight:.3f} = {weighted_scores.get('postflop', {}).get('group_score', 0) * postflop_weight:.1f}")
            logger.info(f"[OVERALL CALC] TOTAL: {overall_score:.1f}")
    
    return weighted_scores


def _sort_months(months: List[str]) -> List[str]:
    """Sort month identifiers (YYYY-MM) in ascending order, unknown last."""

    def key(month: str) -> Tuple[str, str]:
        if month == 'unknown':
            return ('9999-99', month)
        return (month, '')

    return sorted(months, key=key)


def compute_weighted_scores_for_month_selection(
    token: str,
    selected_month: str,
    result_storage: ResultStorageService,
    ideals: Dict[str, Any],
    stat_weights: Dict[str, float],
) -> Optional[Dict[str, Any]]:
    """Compute weighted scores blending up to 3 months for a selected month."""

    try:
        manifest = result_storage.get_months_manifest(token)
    except Exception:
        manifest = None

    if not manifest or not manifest.get('months'):
        return None

    available_months = [m['month'] for m in manifest['months'] if m.get('month')]
    if not available_months:
        return None

    available_months = _sort_months(available_months)

    if selected_month not in available_months:
        return None

    month_index = available_months.index(selected_month)
    months_to_use = available_months[max(0, month_index - 2): month_index + 1]

    months_to_use = _sort_months(months_to_use)

    if not months_to_use:
        return None

    weight_map = {}
    if len(months_to_use) == 1:
        weight_map[months_to_use[-1]] = 1.0
    elif len(months_to_use) == 2:
        # Selected month (latest) 50%, previous month 50%
        weight_map[months_to_use[-1]] = 0.5
        weight_map[months_to_use[-2]] = 0.5
    else:
        # Selected month 50%, immediate previous 30%, oldest 20%
        weight_map[months_to_use[-1]] = 0.50
        weight_map[months_to_use[-2]] = 0.30
        weight_map[months_to_use[-3]] = 0.20

    per_month_scores: Dict[str, Dict[str, Any]] = {}
    months_used: List[Dict[str, Any]] = []

    total_weight = 0.0
    for month in reversed(months_to_use):
        try:
            month_result = result_storage.get_pipeline_result(token, month=month)
        except FileNotFoundError:
            continue

        if not month_result or month_result.get('status') == 'failed':
            continue

        combined = month_result.get('combined', {})
        if not combined:
            continue

        postflop_all = aggregate_postflop_stats(combined, ideals, stat_weights)
        month_scores = calculate_weighted_scores_from_groups(combined, postflop_all)

        normalized_scores: Dict[str, Any] = {}
        for key, value in month_scores.items():
            if key == 'overall':
                normalized_scores[key] = {
                    'group_score': value if value is not None else None
                }
            else:
                normalized_scores[key] = value

        per_month_scores[month] = normalized_scores

        weight = weight_map.get(month, 0.0)
        if weight <= 0:
            continue

        total_weight += weight

        months_used.append({
            'month': month,
            'weight': weight,
            'valid_hands': month_result.get('valid_hands', 0),
            'total_hands': month_result.get('total_hands', 0),
            'overall_score': month_scores.get('overall', 0)
        })

    if not months_used or total_weight == 0:
        return None

    # Normalize weights to ensure they sum to 1.0
    for entry in months_used:
        entry['normalized_weight'] = entry['weight'] / total_weight

    normalized_weights = {m['month']: m['normalized_weight'] for m in months_used}

    def weighted_average(group_key: str, attr: str = 'group_score') -> Optional[float]:
        values = []
        for month, weight in normalized_weights.items():
            month_entry = per_month_scores.get(month, {}).get(group_key)
            if month_entry and month_entry.get(attr) is not None:
                values.append((month_entry.get(attr), weight))
        if not values:
            return None
        return sum(val * wt for val, wt in values)

    final_scores: Dict[str, Any] = {}

    # Non-KO group
    nonko_score = weighted_average('nonko')
    if nonko_score is not None:
        aggregated_subgroups = {}
        subgroup_weights = {}
        for month, weight in normalized_weights.items():
            month_entry = per_month_scores.get(month, {}).get('nonko', {})
            for subgroup, data in (month_entry.get('subgroups') or {}).items():
                aggregated_subgroups.setdefault(subgroup, 0.0)
                subgroup_weights.setdefault(subgroup, 0.0)
                if data.get('score') is not None:
                    aggregated_subgroups[subgroup] += data['score'] * weight
                    subgroup_weights[subgroup] += weight

        for subgroup, total in aggregated_subgroups.items():
            weight = subgroup_weights.get(subgroup, 1.0)
            if weight > 0:
                aggregated_subgroups[subgroup] = total / weight

        weight_9max = weighted_average('nonko', 'weight_9max')
        weight_6max = weighted_average('nonko', 'weight_6max')

        final_scores['nonko'] = {
            'group_score': nonko_score,
            'subgroups': {k: {'score': v} for k, v in aggregated_subgroups.items()},
        }

        if weight_9max is not None and weight_6max is not None:
            final_scores['nonko']['weight_9max'] = int(round(weight_9max))
            final_scores['nonko']['weight_6max'] = int(round(weight_6max))

    # PKO group
    pko_score = weighted_average('pko')
    if pko_score is not None:
        aggregated_subgroups = {}
        subgroup_weights = {}
        for month, weight in normalized_weights.items():
            month_entry = per_month_scores.get(month, {}).get('pko', {})
            for subgroup, data in (month_entry.get('subgroups') or {}).items():
                aggregated_subgroups.setdefault(subgroup, 0.0)
                subgroup_weights.setdefault(subgroup, 0.0)
                if data.get('score') is not None:
                    aggregated_subgroups[subgroup] += data['score'] * weight
                    subgroup_weights[subgroup] += weight

        for subgroup, total in aggregated_subgroups.items():
            weight = subgroup_weights.get(subgroup, 1.0)
            if weight > 0:
                aggregated_subgroups[subgroup] = total / weight

        final_scores['pko'] = {
            'group_score': pko_score,
            'subgroups': {k: {'score': v} for k, v in aggregated_subgroups.items()},
        }

    # Postflop group
    postflop_score = weighted_average('postflop')
    if postflop_score is not None:
        aggregated_subgroups = {}
        subgroup_weights = {}
        for month, weight in normalized_weights.items():
            month_entry = per_month_scores.get(month, {}).get('postflop', {})
            for subgroup, data in (month_entry.get('subgroups') or {}).items():
                aggregated_subgroups.setdefault(subgroup, 0.0)
                subgroup_weights.setdefault(subgroup, 0.0)
                if data.get('score') is not None:
                    aggregated_subgroups[subgroup] += data['score'] * weight
                    subgroup_weights[subgroup] += weight

        for subgroup, total in aggregated_subgroups.items():
            weight = subgroup_weights.get(subgroup, 1.0)
            if weight > 0:
                aggregated_subgroups[subgroup] = total / weight

        final_scores['postflop'] = {
            'group_score': postflop_score,
            'subgroups': {k: {'score': v} for k, v in aggregated_subgroups.items()},
        }

    overall_score = weighted_average('overall')
    if overall_score is not None:
        final_scores['overall'] = overall_score

    return {
        'weighted_scores': final_scores,
        'months_used': months_used,
        'per_month_scores': per_month_scores
    }


def _build_dashboard_payload_from_pipeline(
    *,
    pipeline_result: Dict[str, Any],
    token: Optional[str],
    month: Optional[str],
    base: Path,
    months_manifest: Optional[Dict[str, Any]],
    selected_scope: str,
    month_not_found: bool,
    result_storage: Optional[Any],
    extra_meta: Optional[Dict[str, Any]] = None,
) -> dict:
    import logging

    logger = logging.getLogger(__name__)

    # Find stats file
    stats_path = base / 'stats' / 'stat_counts.json'
    if not stats_path.exists():
        stats_path = Path('stats') / 'stat_counts.json'

    # Find score file
    score_path = base / 'scores' / 'scorecard.json'
    if not score_path.exists():
        score_path = Path('scores') / 'scorecard.json'

    # Load config file for weights and ideals
    config_path = Path('app/score/config.yml')
    config = {}
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
        except Exception:
            config = {}
    
    # Load JSONs if they exist
    counts = {}
    if stats_path.exists():
        try:
            with open(stats_path, 'r', encoding='utf-8') as f:
                counts = json.load(f)
        except Exception:
            counts = {}

    score = {}
    if score_path.exists():
        try:
            with open(score_path, 'r', encoding='utf-8') as f:
                score = json.load(f)
        except Exception:
            score = {}

    counts_for_structure = counts.get('counts', {}) if isinstance(counts, dict) else {}
    counts_payload = counts
    if isinstance(pipeline_result, dict):
        pipeline_counts = pipeline_result.get('counts')
        if isinstance(pipeline_counts, dict):
            counts_for_structure = pipeline_counts
            counts_payload = pipeline_counts
    
    # Normalize score keys
    overall = (
        score.get('overall') or
        score.get('data', {}).get('overall') or
        score.get('_raw', {}).get('overall')
    )

    if not isinstance(overall, dict):
        overall_value = overall
        overall = {}
        if overall_value is not None:
            overall['score'] = overall_value
    
    group_level = (
        score.get('group_level') or 
        score.get('data', {}).get('group_level') or 
        {}
    )
    
    samples = (
        score.get('samples') or 
        score.get('sample') or 
        {}
    )
    
    # Get scoring data
    stat_level = score.get('stat_level') or score.get('data', {}).get('stat_level') or {}
    subgroup_level = score.get('subgroup_level') or score.get('data', {}).get('subgroup_level') or {}
    
    # Extract config data
    weights = config.get('weights', {})
    ideals = config.get('ideals', {})
    group_weights = weights.get('groups', {})
    subgroup_weights = weights.get('subgroups', {})
    stat_weights = weights.get('stats', {})
    
    # Build hierarchical groups structure
    groups = build_groups_structure(
        counts=counts_for_structure,
        ideals=ideals,
        stat_level=stat_level,
        subgroup_level=subgroup_level,
        group_weights=group_weights,
        subgroup_weights=subgroup_weights,
        stat_weights=stat_weights
    )

    # Get list of months (prefer manifest when available)
    months: List[str] = []
    if months_manifest and isinstance(months_manifest, dict):
        manifest_months: List[str] = []
        for entry in months_manifest.get('months', []):
            if not isinstance(entry, dict):
                continue
            month_value = entry.get('month')
            if not month_value:
                continue
            if entry.get('has_data') is False and not entry.get('total_hands') and not entry.get('valid_hands'):
                continue
            manifest_months.append(month_value)
        months = manifest_months
    if not months and isinstance(counts_for_structure, dict):
        ignored = {'generated_at', 'input', 'dsl', 'metric', 'hands_processed', 'errors', 'stats_computed'}
        months = [key for key in counts_for_structure.keys() if key not in ignored]

    if months:
        # Ensure months are unique and sorted chronologically, keeping 'unknown' last
        months = _sort_months(sorted(set(months)))

    # Month-specific dashboards are temporarily disabled to prioritise the
    # original global behaviour, so surface no month entries to the frontend.
    months = []

    # Load ingest manifest if exists
    ingest = {}
    manifest_path = base / "manifest.json"
    if manifest_path.exists():
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
                ingest = {
                    "files_total": manifest.get("files_total", 0),
                    "files_pko": manifest.get("files_pko", 0),
                    "files_mystery": manifest.get("files_mystery", 0),
                    "files_nonko": manifest.get("files_nonko", 0),
                    "timestamp": manifest.get("timestamp", ""),
                    "source_zip": manifest.get("source_zip", "")
                }
        except Exception:
            pass
    
    # Build weighted_scores from group_level for frontend compatibility
    weighted_scores = {}
    if group_level:
        for group_name, group_data in group_level.items():
            weighted_scores[group_name] = {
                "group_score": group_data.get("score"),
                "subgroups": {}
            }
            # Add subgroup scores if available
            if subgroup_level:
                for subgroup_key, subgroup_data in subgroup_level.items():
                    if subgroup_key.startswith(f"{group_name}_"):
                        subgroup_name = subgroup_key.replace(f"{group_name}_", "")
                        weighted_scores[group_name]["subgroups"][subgroup_name] = {
                            "score": subgroup_data.get("score")
                        }
    
    # Extract data from pipeline_result if available
    discard_stats: Dict[str, Any] = {}
    if pipeline_result and 'classification' in pipeline_result:
        import logging
        logger = logging.getLogger(__name__)
        classification = pipeline_result['classification']

        # Extract totals from classification
        total_hands_found = classification.get('total_hands', 0)
        discarded_data = classification.get('discarded_hands', {})
        
        # Calculate valid hands
        total_discarded = discarded_data.get('total', 0) if isinstance(discarded_data, dict) else 0
        valid_hands_calculated = total_hands_found - total_discarded
        
        # Build discard_stats for frontend
        # NOTE: Stack and all-in exclusions REMOVED - validations now per-stat only
        if isinstance(discarded_data, dict):
            discard_stats = {
                'mystery': discarded_data.get('mystery', 0),
                'less_than_4_players': discarded_data.get('less_than_4_players', 0),
                'tournament_summary': discarded_data.get('tournament_summary', 0),
                'cash_game': discarded_data.get('cash_game', 0),
                'invalid_format': discarded_data.get('invalid_format', 0),
                'other': discarded_data.get('other', 0)
            }
        
        # Create or update overall with correct data
        if not overall:
            overall = {}
        overall['total_hands'] = total_hands_found
        overall['valid_hands'] = valid_hands_calculated
        overall['discarded_hands'] = total_discarded
        
        logger.info(f"[CLASSIFICATION] Extracted: total={total_hands_found}, valid={valid_hands_calculated}, discarded={total_discarded}, mystery={discard_stats.get('mystery', 0)}")

    if pipeline_result:
        # Fall back to aggregated discards when classification data is absent
        aggregated_discards = pipeline_result.get('aggregated_discards')
        if not discard_stats and isinstance(aggregated_discards, dict):
            discard_stats = aggregated_discards

        if isinstance(pipeline_result.get('discarded_hands'), dict) and not discard_stats:
            discard_stats = pipeline_result['discarded_hands']

        if pipeline_result.get('valid_hands') is not None:
            overall['valid_hands'] = pipeline_result.get('valid_hands')

        if pipeline_result.get('total_hands') is not None:
            overall['total_hands'] = pipeline_result.get('total_hands')

        if isinstance(discard_stats, dict) and 'total' in discard_stats:
            overall['discarded_hands'] = discard_stats.get('total', overall.get('discarded_hands', 0))

    hands_per_month: Dict[str, int] = {}
    if isinstance(pipeline_result, dict):
        raw_hands_per_month = pipeline_result.get('hands_per_month')
        if isinstance(raw_hands_per_month, dict):
            normalized: Dict[str, int] = {}
            for month_key, value in raw_hands_per_month.items():
                if value is None:
                    continue
                if not isinstance(month_key, str) or not MONTH_KEY_PATTERN.fullmatch(month_key):
                    continue
                try:
                    normalized[month_key] = int(value)
                except (TypeError, ValueError):
                    continue

            if normalized:
                ordered_keys = _sort_months(list(normalized.keys()))
                hands_per_month = {month_key: normalized[month_key] for month_key in ordered_keys}

    if hands_per_month:
        month_total = sum(hands_per_month.values())
        valid_total = None
        if overall and isinstance(overall.get('valid_hands'), (int, float)):
            valid_total = int(overall['valid_hands'])
        elif isinstance(pipeline_result, dict) and isinstance(pipeline_result.get('valid_hands'), (int, float)):
            valid_total = int(pipeline_result['valid_hands'])

        if valid_total is not None and month_total != valid_total:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(
                "[MONTH CHECK] hands_per_month total %s does not match valid_hands %s",
                month_total,
                valid_total,
            )
    elif month and selected_scope == 'monthly':
        valid_total = None
        if overall and isinstance(overall.get('valid_hands'), (int, float)):
            valid_total = int(overall['valid_hands'])
        elif isinstance(pipeline_result, dict) and isinstance(pipeline_result.get('valid_hands'), (int, float)):
            valid_total = int(pipeline_result['valid_hands'])

        if valid_total is not None:
            hands_per_month = {month: valid_total}

    # Add preflop groups from pipeline_result to groups structure
    if pipeline_result and 'combined' in pipeline_result:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"[DEBUG] Adding groups from pipeline_result")
        logger.info(f"[DEBUG] Pipeline groups: {list(pipeline_result['combined'].keys())}")
        
        # Add each group (nonko_9max, nonko_6max, pko) to groups
        # NOTE: hand_count is already correct in pipeline_result['combined'] - DO NOT override
        for grp_name, grp_data in pipeline_result['combined'].items():
            if isinstance(grp_data, dict):
                # Transform pipeline_result format to frontend format (preserves hand_count)
                transformed_group = transform_group_for_frontend(grp_data)
                groups[grp_name] = transformed_group
                logger.info(f"[DEBUG] Added group '{grp_name}' with {transformed_group.get('hands_count', 0)} hands, {len(transformed_group.get('stats', {}))} preflop stats")
        
        # Aggregate postflop stats
        postflop_total_override: Optional[int] = None
        postflop_hand_ids: Optional[List[str]] = None

        global_samples_data = pipeline_result.get('global_samples') if isinstance(pipeline_result, dict) else None
        if isinstance(global_samples_data, dict):
            raw_total = global_samples_data.get('validas')
            if isinstance(raw_total, (int, float)):
                postflop_total_override = int(raw_total)

            groups_snapshot = global_samples_data.get('groups')
            if isinstance(groups_snapshot, dict):
                postflop_snapshot = groups_snapshot.get('postflop_all')
                if isinstance(postflop_snapshot, dict):
                    ids = postflop_snapshot.get('hand_ids')
                    if isinstance(ids, list):
                        postflop_hand_ids = ids
                        if postflop_total_override is None:
                            postflop_total_override = len(ids)

        postflop_all = aggregate_postflop_stats(
            pipeline_result['combined'],
            ideals,
            stat_weights,
            total_valid_hands=postflop_total_override,
            valid_hand_ids=postflop_hand_ids,
        )
        
        if postflop_all:
            subgroups_count = len(postflop_all.get('subgroups', {}))
            total_stats = sum(len(sg.get('stats', {})) for sg in postflop_all.get('subgroups', {}).values())
            logger.info(f"[DEBUG] postflop_all result: hands={postflop_all.get('hands_count', 0)}, subgroups={subgroups_count}, total_stats={total_stats}")
            
            # Check if there's already a postflop_all in groups from pipeline
            if 'postflop_all' in groups:
                logger.warning(f"[DEBUG] WARNING: postflop_all already exists in groups! Will be overwritten.")
                logger.warning(f"[DEBUG] Existing postflop_all structure: has stats={bool(groups.get('postflop_all', {}).get('stats'))}, has subgroups={bool(groups.get('postflop_all', {}).get('subgroups'))}")
            
            # Check if postflop_all is in pipeline_result['combined']
            if 'postflop_all' in pipeline_result.get('combined', {}):
                logger.warning(f"[DEBUG] WARNING: postflop_all found in pipeline_result['combined']! This should not happen!")
                pipeline_postflop = pipeline_result['combined']['postflop_all']
                logger.warning(f"[DEBUG] Pipeline postflop_all has: file_count={pipeline_postflop.get('file_count')}, sites_included={pipeline_postflop.get('sites_included')}")
            
            groups['postflop_all'] = postflop_all
            logger.info(f"[DEBUG] Added correct postflop_all to groups")
        else:
            logger.warning(f"[DEBUG] postflop_all is None or empty")
        
        # Calculate weighted_scores from pipeline groups (including postflop)
        weighted_scores = calculate_weighted_scores_from_groups(pipeline_result['combined'], postflop_all)
        logger.info(f"[DEBUG] Calculated weighted_scores: {list(weighted_scores.keys())}")

        # If a specific month is selected and we have result storage, blend scores across months
        if token and month and result_storage:
            month_weighting = compute_weighted_scores_for_month_selection(
                token,
                month,
                result_storage,
                ideals,
                stat_weights,
            )

            if month_weighting:
                response_month_details = month_weighting
            else:
                response_month_details = None
        else:
            response_month_details = None
    else:
        response_month_details = None

    groups = ensure_group_defaults(groups)

    # Detect poker sites from hands
    hands_by_site = detect_sites_from_hands(token, pipeline_result) if token and pipeline_result else {}
    
    # Build response
    response_data = {
        "run": {
            "token": token,
            "base": str(base)
        },
        "overall": overall,
        "group_level": group_level,
        "weighted_scores": weighted_scores,  # Frontend compatibility
        "samples": samples,
        "months": months,
        "hands_per_month": hands_per_month,
        "counts": counts_payload,
        "groups": groups,  # New hierarchical structure
        "ingest": ingest,  # Classification summary
        "discard_stats": discard_stats,  # Discard breakdown for frontend
        "total_hands": overall.get('total_hands', 0) if overall else 0,  # Direct access for frontend
        "valid_hands": overall.get('valid_hands', 0) if overall else 0,  # Direct access for frontend
        "hands_by_site": hands_by_site,  # Detected poker sites with hand counts
        "requested_month": month,
        "selected_month": month if selected_scope == 'monthly' else None,
        "month_scope": selected_scope,
        "month_not_found": month_not_found,
    }

    if month and month_not_found:
        response_data['groups'] = reset_groups_for_missing_data(groups)
        response_data['valid_hands'] = 0
        response_data['total_hands'] = 0
        response_data['hands_by_site'] = {}
        response_data['discard_stats'] = {}
        response_data['overall'] = {'total_hands': 0, 'valid_hands': 0}
        response_data['group_level'] = {}
        response_data['samples'] = {}
        response_data['counts'] = {}
        response_data['weighted_scores'] = {}
        response_data['ingest'] = {}
        response_data['hands_per_month'] = {}
        response_data.pop('monthly_score_details', None)
        response_data['month_scope'] = 'missing'
        response_data['selected_month'] = None
    else:
        response_data['groups'] = groups

    if response_month_details:
        response_data['monthly_score_details'] = response_month_details

    if months_manifest:
        response_data['months_manifest'] = months_manifest

    if extra_meta:
        response_data['meta'] = extra_meta

    if month and month_not_found:
        logger.warning("[LOAD] Returning empty dashboard payload for %s month %s (no monthly data)", token, month)

    # Final check on postflop_all before returning
    if 'postflop_all' in response_data.get('groups', {}):
        pf_all = response_data['groups']['postflop_all']
        logger.info(f"[DEBUG] Final check - postflop_all in response has:")
        logger.info(f"[DEBUG]   - hands_count: {pf_all.get('hands_count', 'N/A')}")
        logger.info(f"[DEBUG]   - overall_score: {pf_all.get('overall_score', 'N/A')}")
        logger.info(f"[DEBUG]   - subgroups count: {len(pf_all.get('subgroups', {}))}")
        logger.info(f"[DEBUG]   - has 'stats' field: {bool(pf_all.get('stats'))}")
        logger.info(f"[DEBUG]   - has 'file_count' field: {bool(pf_all.get('file_count'))}")
        logger.info(f"[DEBUG]   - has 'sites_included' field: {bool(pf_all.get('sites_included'))}")

        if pf_all.get('subgroups'):
            logger.info(f"[DEBUG]   - subgroup names: {list(pf_all.get('subgroups', {}).keys())}")

    return response_data


def build_dashboard_payload(
    token: Optional[str],
    month: Optional[str] = None,
    *,
    include_months: bool = False,
) -> dict:
    """Build consolidated dashboard payload from runs or current directory

    Args:
        token: Job token (12 hex characters)
        month: Optional month in YYYY-MM format. If provided, loads month-specific data.
               If None, loads aggregate data across all months.
        include_months: When True, include months metadata if available. Defaults
            to False to prioritise the classic global dashboard flow.

    Returns:
        Dashboard payload dict
    """

    # Temporarily disable month-specific views to restore the original global flow
    month = None

    # Resolve base directory
    if token:
        base = Path('runs') / token
    else:
        base = Path('.')

    # Try to load pipeline_result.json first (new pipeline)
    # Use ResultStorage to read from cloud storage instead of local work directory
    pipeline_result = {}
    months_manifest = None
    import logging
    logger = logging.getLogger(__name__)

    result_storage = None
    selected_scope = 'aggregate'
    month_not_found = False
    months_info: List[Dict[str, Any]] = []

    if token:
        try:
            from app.services.result_storage import get_result_storage
            result_storage = get_result_storage()
        except Exception as storage_error:
            logger.error(f"[LOAD] Failed to initialise result storage: {storage_error}")
            result_storage = None

    if token and result_storage:
        aggregate_result: Optional[Dict[str, Any]] = None

        if month:
            try:
                pipeline_result = result_storage.get_pipeline_result(token, month=month)
                if pipeline_result:
                    selected_scope = 'monthly'
                    logger.info(
                        "[LOAD] ✓ Loaded monthly pipeline_result for %s (%s)",
                        token,
                        month,
                    )
            except FileNotFoundError:
                month_not_found = True
                selected_scope = 'missing'
                logger.warning("[LOAD] Monthly pipeline_result missing for %s/%s", token, month)
            except Exception as load_error:
                logger.error(f"[LOAD] Failed to load monthly pipeline_result: {load_error}")

        if not pipeline_result:
            aggregate_result = result_storage.get_pipeline_result(token)
            if not aggregate_result:
                logger.error("[LOAD] No aggregate pipeline_result found for token %s", token)
                raise FileNotFoundError(f"Pipeline result not found for token {token}")
            pipeline_result = aggregate_result
            if not month_not_found:
                selected_scope = 'aggregate'
            logger.info(
                "[LOAD] ✓ Loaded aggregate pipeline_result with keys: %s",
                list(pipeline_result.keys()),
            )

        if include_months:
            if hasattr(result_storage, 'list_available_months'):
                try:
                    months_info = result_storage.list_available_months(token)
                except Exception as manifest_error:
                    logger.debug(f"[LOAD] Unable to list months: {manifest_error}")
                    months_info = []

            if months_info:
                months_manifest = {'months': months_info}
            else:
                try:
                    months_manifest = result_storage.get_months_manifest(token)
                except Exception as manifest_error:
                    logger.debug(f"[LOAD] Months manifest not available: {manifest_error}")
                    months_manifest = None

    elif token:
        # Fallback to local filesystem (primarily used in tests/dev)
        local_base = Path('work') / token
        if month:
            month_path = local_base / 'months' / month / 'pipeline_result.json'
            if month_path.exists():
                pipeline_result = json.loads(month_path.read_text())
                selected_scope = 'monthly'
            else:
                month_not_found = True
        if not pipeline_result:
            aggregate_path = local_base / 'pipeline_result.json'
            if aggregate_path.exists():
                pipeline_result = json.loads(aggregate_path.read_text())
                selected_scope = 'aggregate'
        if not pipeline_result:
            raise FileNotFoundError(f"Pipeline result not found for token {token}")

        manifest_path = local_base / 'months_manifest.json'
        if include_months and manifest_path.exists():
            try:
                months_manifest = json.loads(manifest_path.read_text())
            except Exception:
                months_manifest = None

    return _build_dashboard_payload_from_pipeline(
        pipeline_result=pipeline_result,
        token=token,
        month=month,
        base=base,
        months_manifest=months_manifest,
        selected_scope=selected_scope,
        month_not_found=month_not_found,
        result_storage=result_storage,
    )


def build_user_month_dashboard_payload(
    user_id: str,
    month: str,
    *,
    use_cache: bool = True,
    result_storage: ResultStorageService | None = None,
) -> dict:
    """
    Devolve o payload de dashboard para este utilizador+month,
    com a mesma estrutura de /api/dashboard/<token>?month=YYYY-MM.
    """

    if month != LATEST_UPLOAD_KEY and (not isinstance(month, str) or not MONTH_KEY_PATTERN.fullmatch(month)):
        raise ValueError("month must be in YYYY-MM format")

    start = time.monotonic()
    result_storage = result_storage or ResultStorageService()

    # Special case: latest upload without month split
    if month == LATEST_UPLOAD_KEY:
        latest_upload = uploads_service.get_master_or_latest_upload_for_user(user_id)
        if not latest_upload or not latest_upload.get("token"):
            return {"month_not_found": True}

        try:
            payload = build_dashboard_payload(
                latest_upload["token"],
                month=None,
                include_months=False,
            )
            payload.setdefault("meta", {})["mode"] = "user_latest_upload"
            payload["meta"]["month"] = LATEST_UPLOAD_KEY
            return payload
        except Exception as exc:  # noqa: BLE001 - keep same contract
            logger.debug(
                "[USER_MONTH] Failed to build latest-upload payload for %s: %s", user_id, exc
            )
            return {"month_not_found": True}

    if use_cache:
        try:
            cached = result_storage.load_month_dashboard_payload(user_id, month)
            if cached:
                logger.info(
                    "[USER_MONTH] Loaded cached dashboard payload for %s/%s in %.2fs",
                    user_id,
                    month,
                    time.monotonic() - start,
                )
                return cached
        except Exception as exc:  # noqa: BLE001 - best-effort fallback
            logger.debug(
                "[USER_MONTH] Failed to read cached dashboard payload for %s/%s: %s",
                user_id,
                month,
                exc,
            )
    pipeline_result = None
    selected_scope = 'monthly'
    month_not_found = False

    # 1) Tenta ler diretamente o cache consolidado do utilizador (user-<id>)
    try:
        pipeline_result = result_storage.get_pipeline_result(f"user-{user_id}", month=month)
        selected_scope = 'monthly'
        logger.debug(
            "[USER_MONTH] Loaded cached pipeline_result for user %s month %s", user_id, month
        )
    except FileNotFoundError:
        logger.debug(
            "[USER_MONTH] Missing cached pipeline_result for user %s month %s", user_id, month
        )
    except Exception as exc:  # noqa: BLE001 - fallback to merge path
        logger.debug(
            "[USER_MONTH] Failed to load cached pipeline_result for %s/%s: %s", user_id, month, exc
        )

    # 2) Não reconstrói – apenas usa artefactos persistidos
    if pipeline_result is None:
        month_not_found = True
        pipeline_result = {}
        selected_scope = 'missing'

    base = Path('results') / 'users' / str(user_id) / 'months' / month
    months_manifest = {
        'months': [
            {
                'month': month,
                'has_data': not month_not_found,
                'source': 'user_month',
            }
        ]
    }

    meta = {
        'mode': 'user_month',
        'user_id': str(user_id),
        'month': month,
    }

    payload = _build_dashboard_payload_from_pipeline(
        pipeline_result=pipeline_result,
        token=None,
        month=month,
        base=base,
        months_manifest=months_manifest,
        selected_scope=selected_scope,
        month_not_found=month_not_found,
        result_storage=None,
        extra_meta=meta,
    )

    if use_cache and payload:
        try:
            result_storage.save_month_dashboard_payload(user_id, month, payload)
        except Exception as exc:  # noqa: BLE001 - optional cache write
            logger.debug(
                "[USER_MONTH] Failed to persist dashboard payload for %s/%s: %s",
                user_id,
                month,
                exc,
            )

    logger.info(
        "[USER_MONTH] Built dashboard payload for %s/%s in %.2fs",
        user_id,
        month,
        time.monotonic() - start,
    )

    return payload


def build_groups_structure(
    counts: Dict[str, Any], 
    ideals: Dict[str, Any],
    stat_level: Dict[str, Any],
    subgroup_level: Dict[str, Any],
    group_weights: Dict[str, float],
    subgroup_weights: Dict[str, float],
    stat_weights: Dict[str, float]
) -> Dict[str, Any]:
    """Build hierarchical groups -> subgroups -> stats structure"""
    
    groups = {}
    
    # Find the first month with actual data (skip metadata keys)
    data_month = None
    for key in counts:
        if key not in ['generated_at', 'input', 'dsl', 'metric', 'hands_processed', 'errors', 'stats_computed']:
            data_month = key
            break
    
    if not data_month:
        return groups
    
    month_data = counts[data_month]
    
    # Detect subgroup from stat name
    def get_subgroup(stat_name: str) -> str:
        """Map stat name to subgroup"""
        stat_upper = stat_name.upper()
        
        # RFI stats
        if any(x in stat_upper for x in ['RFI_', 'CO_STEAL', 'BTN_STEAL']):
            return 'RFI'
        
        # BvB stats  
        if any(x in stat_upper for x in ['SB_UO', 'SB_STEAL', 'BB_FOLD_TO_SB', 'BB_RAISE_VS_SB']):
            return 'BvB'
            
        # VS_3BET stats (check before generic 3BET to avoid mis-grouping)
        if 'FOLD_TO_3BET' in stat_upper:
            return 'VS_3BET'
            
        # 3BET stats (generic, after VS_3BET check)
        if '3BET' in stat_upper:
            return '3BET_RANGE'
            
        # Cold call stats
        if 'COLD_CALL' in stat_upper:
            return 'CC_RANGE'
            
        # VPIP stats
        if 'VPIP' in stat_upper:
            return 'VPIP'
            
        # FOLD_RANGE stats
        if 'FOLD_TO_CO_STEAL' in stat_upper:
            return 'FOLD_RANGE'
            
        # SQUEEZE stats
        if 'SQUEEZE' in stat_upper:
            return 'SQUEEZE'
            
        # BB_DEFENSE stats
        if any(x in stat_upper for x in ['BB_FOLD_VS_', 'BB_RESTEAL']):
            return 'BB_DEFENSE'
            
        # SB_DEFENSE stats
        if any(x in stat_upper for x in ['SB_FOLD_TO_', 'SB_RESTEAL']):
            return 'SB_DEFENSE'
            
        # Postflop - FLOP_CBET
        if 'FLOP_CBET' in stat_upper:
            return 'FLOP_CBET'
            
        # Postflop - VS_CBET (fold/raise vs cbet or check-raise)
        if any(x in stat_upper for x in ['FOLD_VS_CBET', 'RAISE_CBET', 'FOLD_VS_CHECK_RAISE', 'FLOP_FOLD_VS_CBET', 'FLOP_RAISE_CBET']):
            return 'VS_CBET'
            
        # Postflop - VS_SKIPPED_CBET
        if 'BET_VS_MISSED_CBET' in stat_upper or 'FLOP_BET_VS_MISSED' in stat_upper:
            return 'VS_SKIPPED_CBET'
            
        # Postflop - TURN_PLAY
        if any(x in stat_upper for x in ['TURN_CBET', 'TURN_DONK', 'BET_TURN_VS_MISSED', 'TURN_FOLD_VS_CBET']):
            return 'TURN_PLAY'
            
        # Postflop - RIVER_PLAY
        if any(x in stat_upper for x in ['WTSD', 'W$SD', 'W$WSF', 'RIVER_AGG', 'RIVER_BET', 'W$SD% B']):
            return 'RIVER_PLAY'
            
        # Default
        return 'OTHER'
    
    # Process each group in month data
    for group_name, group_stats in month_data.items():
        if not isinstance(group_stats, dict):
            continue
            
        # Organize stats by subgroup
        subgroups = {}
        
        for stat_name, stat_data in group_stats.items():
            if not isinstance(stat_data, dict):
                continue
                
            subgroup = get_subgroup(stat_name)
            
            if subgroup not in subgroups:
                subgroups[subgroup] = {
                    "weight": subgroup_weights.get(subgroup, 0.05),
                    "stats": {}
                }
            
            # Get ideal for this stat and group
            stat_ideals = ideals.get(stat_name, {})
            ideal_value = stat_ideals.get(group_name, None)
            
            # Convert single ideal to range [ideal-2, ideal+2]
            ideal_range = None
            if ideal_value is not None:
                ideal_range = [ideal_value - 2, ideal_value + 2]
            
            # Get score for this stat
            stat_score = None
            if stat_level:
                stat_key = f"{group_name}_{stat_name}"
                if stat_key in stat_level:
                    stat_score = stat_level[stat_key].get('score')
            
            # Build stat entry
            subgroups[subgroup]["stats"][stat_name] = {
                "opps": stat_data.get("opportunities", 0),
                "att": stat_data.get("attempts", 0),
                "pct": stat_data.get("percentage", 0),
                "ideal": ideal_range,
                "score": stat_score,
                "weight": stat_weights.get(stat_name, 0.05),
                "idx": stat_data.get("index_files", {})
            }
        
        # Get group weight 
        group_weight = group_weights.get(group_name, 0.1)
        
        # Add to groups structure
        groups[group_name] = {
            "weight": group_weight,
            "subgroups": subgroups
        }
    
    return groups


def aggregate_postflop_stats(
    pipeline_groups: Dict[str, Any],
    ideals: Dict[str, Any],
    stat_weights: Dict[str, float],
    total_valid_hands: Optional[int] = None,
    valid_hand_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Aggregate postflop stats from all groups into postflop_all with proper schema using scoring_config"""
    import logging
    from app.stats.scoring_config import SCORING_CONFIG
    from app.score.scoring import score_step
    
    logger = logging.getLogger(__name__)
    
    # Aggregate postflop stats from all groups
    aggregated_stats = {}
    total_hands = 0

    if valid_hand_ids is not None:
        expected_total = len(valid_hand_ids)
        if total_valid_hands is not None:
            assert expected_total == total_valid_hands, (
                "Provided POSTFLOP override does not match hand_ids length"
            )
        total_valid_hands = expected_total
    
    for group_name, group_data in pipeline_groups.items():
        if not isinstance(group_data, dict):
            continue
        
        # Debug: Log the structure of group_data
        logger.info(f"[AGGREGATE DEBUG] Group {group_name} keys: {list(group_data.keys())}")
        
        # First check for postflop_stats field
        postflop_stats = group_data.get('postflop_stats', {})
        
        # If not found, try to extract postflop stats from the stats field
        if not postflop_stats and 'stats' in group_data:
            # Extract postflop stats from the main stats dictionary
            all_stats = group_data.get('stats', {})
            postflop_stats = {}
            
            # Define postflop stat patterns
            postflop_patterns = [
                "Flop CBet", "Flop fold", "Flop raise", "Fold vs Check Raise",
                "Check Raise", "Flop Bet vs", "Turn CBet", "Turn Cbet", "Turn Donk", "Turn donk",
                "Turn Fold", "Bet Turn", "Bet turn", "WTSD", "W$SD", "W$WSF", "W$SD% B River",
                "River Agg", "River Bet", "River bet"
            ]
            
            # Extract stats that match postflop patterns
            for stat_name, stat_data in all_stats.items():
                if any(pattern.lower() in stat_name.lower() for pattern in postflop_patterns):
                    postflop_stats[stat_name] = stat_data
                    logger.debug(f"[AGGREGATE] Extracted postflop stat '{stat_name}' from group {group_name}")
        
        # Get postflop hands count - try multiple locations
        postflop_hands_count = group_data.get('postflop_hands_count', 0)
        if postflop_hands_count == 0:
            # If not found, use the general hand count as fallback
            postflop_hands_count = group_data.get('hand_count', 0)
        
        total_hands += postflop_hands_count
        
        logger.info(f"[AGGREGATE] Group {group_name}: {len(postflop_stats)} postflop stats extracted, postflop_hands_count={postflop_hands_count}")
        if postflop_stats:
            logger.info(f"[AGGREGATE] Group {group_name} postflop stats: {list(postflop_stats.keys())[:5]}...")  # Show first 5 for brevity
        
        # Aggregate each stat
        for stat_name, stat_data in postflop_stats.items():
            if stat_name not in aggregated_stats:
                aggregated_stats[stat_name] = {
                    'opportunities': 0,
                    'attempts': 0
                }
                # Initialize player_sum for W$WSF Rating
                if stat_name == 'W$WSF Rating' and 'player_sum' in stat_data:
                    aggregated_stats[stat_name]['player_sum'] = 0
                # Initialize total_hands for River Agg %
                if stat_name == 'River Agg %' and 'total_hands' in stat_data:
                    aggregated_stats[stat_name]['total_hands'] = 0
                logger.debug(f"[AGGREGATE] First time seeing '{stat_name}' from {group_name}")
            
            aggregated_stats[stat_name]['opportunities'] += stat_data.get('opportunities', 0)
            aggregated_stats[stat_name]['attempts'] += stat_data.get('attempts', 0)
            
            # Aggregate player_sum and calculate rating for W$WSF Rating
            if stat_name == 'W$WSF Rating' and 'player_sum' in stat_data:
                aggregated_stats[stat_name]['player_sum'] = aggregated_stats[stat_name].get('player_sum', 0) + stat_data.get('player_sum', 0)
                
                # Recalculate W$WSF Rating percentage after aggregating player_sum
                total_player_sum = aggregated_stats[stat_name]['player_sum']
                total_opps = aggregated_stats[stat_name]['opportunities']
                total_wins = aggregated_stats[stat_name]['attempts']
                
                if total_opps > 0 and total_player_sum > 0:
                    avg_players = total_player_sum / total_opps
                    win_rate = total_wins / total_opps
                    expected_rate = 1.0 / avg_players
                    rating = win_rate / expected_rate if expected_rate > 0 else 0
                    aggregated_stats[stat_name]['percentage'] = round(rating, 2)
            
            # Aggregate total_hands and recalculate ratio for River Agg %
            # Ratio: (bets+raises) / calls = aggression level
            if stat_name == 'River Agg %':
                # Aggregate total_hands count
                if 'total_hands' in stat_data:
                    aggregated_stats[stat_name]['total_hands'] = aggregated_stats[stat_name].get('total_hands', 0) + stat_data.get('total_hands', 0)
                
                total_bets_raises = aggregated_stats[stat_name]['attempts']  # numerator
                total_calls = aggregated_stats[stat_name]['opportunities']  # denominator
                if total_calls > 0:
                    aggregated_stats[stat_name]['percentage'] = round(total_bets_raises / total_calls, 2)
                elif total_bets_raises > 0:
                    aggregated_stats[stat_name]['percentage'] = 10.0  # Pure aggression (no calls)
                else:
                    aggregated_stats[stat_name]['percentage'] = 0.0
    
    # Log aggregated stats
    logger.info(f"[AGGREGATE] Total aggregated stats: {len(aggregated_stats)}, Total hands: {total_hands}")
    if aggregated_stats:
        first_key = list(aggregated_stats.keys())[0] if aggregated_stats else None
        if first_key:
            logger.info(f"[AGGREGATE] Sample stat '{first_key}': {aggregated_stats[first_key]}")
    
    # If no postflop stats found, return empty structure instead of None
    if not aggregated_stats:
        logger.warning("[AGGREGATE] No postflop stats found, returning empty structure")
        return {
            'weight': 0.1,
            'hands_count': total_hands,
            'subgroups': {}
        }
    
    # Get postflop configuration
    postflop_config = SCORING_CONFIG.get('postflop_all', {})
    
    # Build stats grouped by subgroup from config using 'group' field
    subgroups = {}
    group_stats_count = {}  # Track stats per group for weight calculation
    
    logger.info(f"[POSTFLOP] Processing {len(aggregated_stats)} aggregated stats: {list(aggregated_stats.keys())}")
    
    for stat_name, stat_data in aggregated_stats.items():
        opps = stat_data['opportunities']
        att = stat_data['attempts']
        
        # Use pre-calculated percentage if available (for special stats like W$WSF Rating, River Agg %)
        # Otherwise calculate standard percentage
        if 'percentage' in stat_data:
            pct = stat_data['percentage']
        else:
            pct = (att / opps * 100) if opps > 0 else 0
        
        # Get config for this stat from postflop_all
        stat_config = postflop_config.get(stat_name)
        
        if not stat_config:
            logger.warning(f"[POSTFLOP] No config found for '{stat_name}', skipping")
            continue
        
        # Get group from config (e.g., 'Flop Cbet', 'Vs Cbet', etc.)
        group_name = stat_config.get('group', 'Other')
        
        # Get scoring parameters
        ideal = stat_config.get('ideal', 50)
        osc_down = stat_config.get('oscillation_down', 5)
        osc_up = stat_config.get('oscillation_up', 5)
        stat_weight = stat_config.get('weight', 0.05)
        
        # Calculate ideal range (±2% default)
        ideal_range = [ideal - 2, ideal + 2] if ideal else None
        
        # Calculate score (skip if no ideal value - e.g., W$WSF Rating)
        score_value = None
        if opps > 0 and ideal is not None:
            score_value = score_step(
                actual_pct=pct,
                ideal_pct=ideal,
                step_down_pct=osc_down,
                step_up_pct=osc_up,
                points_per_step_down=10,
                points_per_step_up=10
            )
        
        # Initialize subgroup if needed
        if group_name not in subgroups:
            subgroups[group_name] = {
                'weight': 0.0,  # Will be calculated from stat weights
                'stats': {}
            }
            group_stats_count[group_name] = 0
        
        # Add stat to subgroup - using frontend-expected field names
        subgroups[group_name]['stats'][stat_name] = {
            'key': stat_name,  # Add key field for frontend
            'opportunities': opps,  # Frontend expects 'opportunities'
            'attempts': att,  # Frontend expects 'attempts'  
            'percentage': pct,  # Frontend expects 'percentage'
            'ideal': ideal_range,
            'score': score_value,
            'weight': stat_weight,
            'idx': {},
            # Also include short names for backward compatibility
            'opps': opps,
            'att': att,
            'pct': pct
        }
        
        # Include total_hands for River Agg % (total unique hands with action)
        if stat_name == 'River Agg %' and 'total_hands' in stat_data:
            subgroups[group_name]['stats'][stat_name]['total_hands'] = stat_data['total_hands']
        
        # Track group weight (sum of stat weights)
        subgroups[group_name]['weight'] += stat_weight
        group_stats_count[group_name] += 1
    
    # Normalize group weights to sum to 1.0
    total_weight = sum(sg['weight'] for sg in subgroups.values())
    if total_weight > 0:
        for subgroup in subgroups.values():
            subgroup['weight'] = subgroup['weight'] / total_weight
    
    logger.info(f"[POSTFLOP] Created {len(subgroups)} subgroups: {list(subgroups.keys())}")
    
    # Calculate aggregate score for each subgroup (weighted average of stat scores)
    subgroup_scores = []
    for group_name, group_data in subgroups.items():
        weighted_score_sum = 0.0
        total_stat_weight = 0.0
        
        for stat_name, stat_data in group_data['stats'].items():
            stat_score = stat_data.get('score')
            stat_weight = stat_data.get('weight', 0.0)
            
            # Only include stats with valid scores (skip W$WSF Rating with score=None)
            if stat_score is not None and stat_weight > 0:
                weighted_score_sum += stat_score * stat_weight
                total_stat_weight += stat_weight
        
        # Calculate weighted average score for this subgroup
        group_score = (weighted_score_sum / total_stat_weight) if total_stat_weight > 0 else 0
        group_data['score'] = round(group_score, 1)
        
        # Track for overall POSTFLOP score calculation
        if group_score > 0:
            subgroup_scores.append(group_score)
        
        logger.info(f"[POSTFLOP] Subgroup '{group_name}' score: {group_score:.1f}")
    
    # Calculate overall POSTFLOP score (weighted average of subgroup scores)
    # Weights: Flop Cbet 35%, Vs Cbet 20%, Vs Skipped Cbet 10%, Turn Play 20%, River play 15%
    subgroup_weights = {
        'Flop Cbet': 0.35,
        'Vs Cbet': 0.20,
        'vs Skipped Cbet': 0.10,
        'Turn Play': 0.20,
        'River play': 0.15
    }
    
    overall_postflop_score = 0
    if subgroups:
        weighted_sum = 0.0
        for group_name, group_data in subgroups.items():
            group_score = group_data.get('score', 0)
            group_weight = subgroup_weights.get(group_name, 0)
            weighted_sum += group_score * group_weight
        
        overall_postflop_score = round(weighted_sum, 1)
        logger.info(f"[POSTFLOP] Overall POSTFLOP score: {overall_postflop_score}")
    
    # Return with proper group schema (weight + subgroups + hands_count + overall_score)
    hands_count = total_hands
    if total_valid_hands is not None:
        hands_count = int(total_valid_hands)
        assert hands_count >= total_hands or total_hands == 0, (
            "POSTFLOP override should not be lower than aggregated opportunities"
        )

    result = {
        'weight': 0.1,
        'hands_count': hands_count,
        'overall_score': overall_postflop_score,
        'subgroups': subgroups
    }

    if valid_hand_ids is not None:
        result['hand_ids'] = valid_hand_ids

    return result