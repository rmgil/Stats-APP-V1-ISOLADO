"""
API endpoints for the simplified workflow
"""
from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import login_required
from app.auth.decorators import email_confirmation_required
import os
import tempfile
import json
from pathlib import Path

bp = Blueprint('simplified', __name__)

@bp.route('/upload')
@login_required
@email_confirmation_required
def upload_page():
    """Render the upload page - requires authentication and email confirmation"""
    from flask_login import current_user
    # Check if user is admin
    is_admin = current_user.email == 'gilrmendes@gmail.com'
    return render_template('simple_upload.html', is_admin=is_admin)

@bp.route('/dashboard')
@login_required
@email_confirmation_required
def dashboard_page():
    """Render the dashboard page - requires authentication and email confirmation"""
    from flask_login import current_user
    from app.services.supabase_history import SupabaseHistoryService

    month = request.args.get('month')
    latest_token = None

    history_service = SupabaseHistoryService()
    if history_service.enabled:
        user_id = current_user.email if hasattr(current_user, 'email') else str(current_user.id)
        latest_run = history_service.get_latest_successful_run(user_id)
        if latest_run:
            latest_token = latest_run.get('token')

    return render_template('dashboard_tabs.html', token=latest_token, month=month)

@bp.route('/dashboard/<token>')
def dashboard_with_token(token):
    """Render the dashboard page for a specific analysis token - no authentication required"""
    # Validate token format (hexadecimal, 12 characters)
    import re
    if not re.match(r'^[a-f0-9]{12}$', token):
        return render_template('error.html', 
            error_title="Token Inválido",
            error_message=f"O token fornecido ('{token}') não é válido. Tokens devem ter exatamente 12 caracteres hexadecimais (0-9, a-f).",
            suggestion="Verifique se copiou o URL corretamente."), 400
    
    # Check if token exists in database or storage
    from app.services.job_queue_service import JobQueueService
    from app.services.result_storage import get_result_storage
    
    job_queue = JobQueueService()
    job = job_queue.get_job(token)
    
    if not job:
        return render_template('error.html',
            error_title="Token Não Encontrado", 
            error_message=f"Não foi encontrado nenhum resultado para o token '{token}'.",
            suggestion="Verifique se o processamento foi concluído ou se o token está correto."), 404
    
    if job['status'] != 'completed':
        return render_template('error.html',
            error_title="Processamento Incompleto",
            error_message=f"O processamento ainda está {job['status']} ({job.get('progress', 0)}%).",
            suggestion="Aguarde alguns minutos e tente novamente."), 400
    
    # Verify results exist in storage (cloud or local)
    result_storage = get_result_storage()
    if not result_storage.job_exists(token):
        return render_template('error.html',
            error_title="Resultados Não Encontrados",
            error_message=f"O processamento foi concluído, mas os resultados não foram encontrados no armazenamento.",
            suggestion="Os resultados podem ter expirado. Tente processar novamente."), 404
    
    return render_template('dashboard_tabs.html', token=token)

@bp.route('/api/analyze', methods=['POST'])
@login_required
@email_confirmation_required
def analyze_archive():
    """
    Handle archive upload and start analysis - requires authentication and email confirmation
    """
    import threading
    
    try:
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({'ok': False, 'error': 'Nenhum arquivo enviado'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'ok': False, 'error': 'Nenhum arquivo selecionado'}), 400
        
        # Check file extension
        if file.filename and not (file.filename.lower().endswith('.zip') or file.filename.lower().endswith('.rar')):
            return jsonify({'ok': False, 'error': 'Apenas arquivos ZIP ou RAR são aceitos'}), 400
        
        # Save uploaded file temporarily
        suffix = os.path.splitext(file.filename)[1] if file.filename else '.zip'
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            file.save(tmp.name)
            temp_path = tmp.name
        
        # Generate token immediately
        from app.pipeline.runner import generate_token
        token = generate_token()
        
        # Initialize progress tracking
        from app.utils.progress_tracker import progress_tracker
        progress_tracker.init_job(token)
        
        # Define background processing function
        def process_in_background(archive_path: str, job_token: str):
            try:
                from app.pipeline.multi_site_runner import run_multi_site_pipeline
                # This will update progress_tracker internally, pass the token so it doesn't generate a new one
                success, _, result_data = run_multi_site_pipeline(archive_path, work_root="work", token=job_token)
                
                if not success:
                    error_msg = result_data.get('error', {}).get('error', 'Erro no processamento')
                    progress_tracker.fail_job(job_token, error_msg)
                    
            except Exception as e:
                import traceback
                traceback.print_exc()
                progress_tracker.fail_job(job_token, str(e))
            finally:
                # Clean up temp file
                try:
                    os.unlink(archive_path)
                except:
                    pass
        
        # Start processing in background thread
        thread = threading.Thread(
            target=process_in_background,
            args=(temp_path, token),
            daemon=True
        )
        thread.start()
        
        # Return token immediately so frontend can start polling
        return jsonify({
            'ok': True,
            'token': token,
            'message': 'Processamento iniciado'
        })
                
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@bp.route('/api/progress/<token>')
def get_progress(token):
    """
    Get real-time processing progress for a token
    """
    from app.utils.progress_tracker import progress_tracker
    
    progress_data = progress_tracker.get_progress(token)
    
    if not progress_data:
        return jsonify({'ok': False, 'error': 'Token não encontrado'}), 404
    
    return jsonify({'ok': True, 'progress': progress_data})

@bp.route('/api/dashboard/<token>')
def get_dashboard_data(token):
    """
    Get dashboard data for a specific token
    Reads from Object Storage (cloud) or local filesystem (dev)
    """
    try:
        from app.services.result_storage import get_result_storage
        import json
        
        result_storage = get_result_storage()
        
        # Load pipeline result from storage (cloud or local)
        result_data = result_storage.get_pipeline_result(token)
        
        if not result_data:
            return jsonify({'ok': False, 'error': 'Dados não encontrados'}), 404
        
        # Check if this is a multi-site result
        is_multi_site = result_data.get('multi_site', False)
        
        # Load appropriate manifest from storage
        if is_multi_site:
            manifest = result_storage.get_multi_site_manifest(token)
        else:
            manifest = result_storage.get_group_manifest(token)
        
        if not manifest:
            manifest = {}
        
        # Extract discard statistics from classification
        discard_stats = {}
        if 'classification' in result_data and 'discarded_hands' in result_data['classification']:
            discard_stats = result_data['classification']['discarded_hands']
        
        # Calculate total valid hands
        if is_multi_site:
            # For multi-site, sum from all sites
            total_valid_hands = 0
            for site_data in result_data.get('sites', {}).values():
                for group_data in site_data.values():
                    total_valid_hands += group_data.get('hand_count', 0)
        else:
            # For single site, use hand_count from groups (not hands_parsed)
            total_valid_hands = sum(g.get('hand_count', 0) for g in result_data.get('groups', {}).values())
        
        # Calculate total hands correctly
        if is_multi_site:
            # For multi-site, use classification data if available, otherwise use valid + discarded
            if 'classification' in result_data and 'total_hands' in result_data['classification']:
                total_hands = result_data['classification']['total_hands']
            else:
                # Calculate from valid hands + all discarded
                total_discarded = sum(v for k, v in discard_stats.items() if k != 'total')
                total_hands = total_valid_hands + total_discarded
        else:
            # For single site, use classification data
            total_hands = result_data.get('classification', {}).get('total_hands', 0)
        
        # Calculate hands by site
        hands_by_site = {}
        if is_multi_site:
            # For multi-site results, iterate through sites
            for site_name, site_data in result_data.get('sites', {}).items():
                site_hands = 0
                for group_data in site_data.values():
                    site_hands += group_data.get('hand_count', 0)
                if site_hands > 0:
                    hands_by_site[site_name] = site_hands
        else:
            # For single-site results, use the detected site
            sites = result_data.get('sites_detected', [])
            if sites and total_valid_hands > 0:
                hands_by_site[sites[0]] = total_valid_hands
        
        # Prepare dashboard data
        dashboard_data = {
            'token': token,
            'status': result_data.get('status', 'unknown'),
            'total_files': result_data.get('extracted_files', 0),
            'total_hands': total_hands,
            'valid_hands': total_valid_hands,
            'discard_stats': discard_stats,
            'groups': {},
            'multi_site': is_multi_site,
            'sites_detected': result_data.get('sites_detected', []),
            'hands_by_site': hands_by_site,  # New field with hand counts by site
            'player_info': {
                'name': '',  # Placeholder para login futuro
                'email': '',  # Placeholder para login futuro
                'level': 'NL50',  # Placeholder
                'photo': ''  # Placeholder para foto
            }
        }
        
        # Ensure all expected groups are present (even if empty)
        # This ensures NON-KO groups always appear for weighted average calculation
        expected_groups = ['nonko_9max', 'nonko_6max', 'pko', 'postflop_all']
        existing_groups = result_data.get('groups', {})
        
        # Get classification data for group labels
        classification = result_data.get('classification', {})
        group_labels = classification.get('group_labels', {
            'nonko_9max': '9-max NON-KO',
            'nonko_6max': '6-max NON-KO',
            'pko': 'PKO (All)',
            'postflop_all': 'POSTFLOP',
        })
        
        # Add all groups (including empty ones)
        if is_multi_site:
            # Use combined data for multi-site results
            combined_groups = result_data.get('combined', {})
            for group_key in expected_groups:
                if group_key in combined_groups:
                    group_data = combined_groups[group_key]
                    group_scores = group_data.get('scores', {})
                    # Count total hands from all sites for this group
                    hands_count = 0
                    if 'sites' in result_data:
                        for site_name, site_data in result_data['sites'].items():
                            if group_key in site_data:
                                hands_count += site_data[group_key].get('hand_count', 0)
                    
                    dashboard_data['groups'][group_key] = {
                        'label': group_labels.get(group_key, group_key),
                        'file_count': len(group_data.get('sites_included', [])),  # Number of sites
                        'hands_count': hands_count,
                        'stats': generate_real_stats(group_key, token, is_multi_site=True),
                        'overall_score': group_scores.get('overall_score', 0),
                        'subgroups': calculate_subgroup_scores(group_key, token),
                        'sites_included': group_data.get('sites_included', [])
                    }
                elif group_key == 'postflop_all':
                    # Special handling for postflop_all - aggregate from source groups
                    total_postflop_hands = 0  # Use postflop-eligible hands only
                    total_files = 0
                    sites_included = set()
                    source_groups = ['pko', 'nonko_9max', 'nonko_6max']
                    
                    # Count POSTFLOP hands (eligible hands, not total) from all sites for source groups
                    if 'sites' in result_data:
                        for site_name, site_data in result_data['sites'].items():
                            for src_group in source_groups:
                                if src_group in site_data:
                                    postflop_count = site_data[src_group].get('postflop_hands_count', 0)
                                    total_postflop_hands += postflop_count
                                    if postflop_count > 0:
                                        sites_included.add(site_name)
                    
                    # Count files from combined groups
                    for src_group in source_groups:
                        if src_group in combined_groups:
                            total_files += combined_groups[src_group].get('file_count', 0)
                    
                    dashboard_data['groups'][group_key] = {
                        'label': group_labels.get(group_key, 'POSTFLOP'),
                        'file_count': total_files,
                        'hands_count': total_postflop_hands,  # Use postflop-eligible hands
                        'stats': generate_real_stats(group_key, token, is_multi_site=True),
                        'overall_score': 0,  # Postflop doesn't have overall score yet
                        'subgroups': {},  # Postflop doesn't use subgroups
                        'sites_included': list(sites_included)
                    }
                else:
                    # Group has no data
                    dashboard_data['groups'][group_key] = {
                        'label': group_labels.get(group_key, group_key),
                        'file_count': 0,
                        'hands_count': 0,
                        'stats': {},
                        'overall_score': 0,
                        'subgroups': calculate_subgroup_scores(group_key, token),
                        'sites_included': []
                    }
        else:
            # Single-site processing - use existing logic
            for group_key in expected_groups:
                if group_key in existing_groups:
                    # Group has data - use it
                    group_data = existing_groups[group_key]
                    group_scores = group_data.get('scores', {})
                    dashboard_data['groups'][group_key] = {
                        'label': group_data.get('label', group_labels.get(group_key, group_key)),
                        'file_count': group_data.get('file_count', 0),
                        'hands_count': group_data.get('hand_count', 0),
                        'stats': generate_real_stats(group_key, token),
                        'overall_score': group_scores.get('overall_score', 0),
                        'subgroups': calculate_subgroup_scores(group_key, token)
                    }
                elif group_key == 'postflop_all':
                    # Special handling for postflop_all - aggregate from source groups
                    total_postflop_hands = 0  # Use postflop-eligible hands only
                    total_files = 0
                    source_groups = ['pko', 'nonko_9max', 'nonko_6max']
                    
                    for src_group in source_groups:
                        if src_group in existing_groups:
                            total_postflop_hands += existing_groups[src_group].get('postflop_hands_count', 0)
                            total_files += existing_groups[src_group].get('file_count', 0)
                    
                    dashboard_data['groups'][group_key] = {
                        'label': group_labels.get(group_key, 'POSTFLOP'),
                        'file_count': total_files,
                        'hands_count': total_postflop_hands,  # Use postflop-eligible hands
                        'stats': generate_real_stats(group_key, token),
                        'overall_score': 0,  # Postflop doesn't have overall score yet
                        'subgroups': {}  # Postflop doesn't use subgroups
                    }
                else:
                    # Group has no data - create empty structure
                    dashboard_data['groups'][group_key] = {
                        'label': group_labels.get(group_key, group_key),
                        'file_count': 0,
                        'hands_count': 0,
                        'stats': {},  # Empty stats for groups with no data
                        'overall_score': 0,
                        'subgroups': calculate_subgroup_scores(group_key, token)
                    }
        
        # Calculate weighted scores for NON-KO and overall
        dashboard_data['weighted_scores'] = calculate_weighted_scores(dashboard_data['groups'])
        
        return jsonify({'ok': True, 'data': dashboard_data})
        
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

def calculate_subgroup_scores(group_key, token):
    """
    Calculate scores for each subgroup (RFI, BvB, etc.)
    """
    # Subgroup definitions with preserved order and placeholder weights
    from collections import OrderedDict
    subgroups = OrderedDict([
        ('RFI', {'weight': 0.25, 'score': 0}),
        ('BvB', {'weight': 0.20, 'score': 0}),
        ('3b & CC', {'weight': 0.20, 'score': 0}),
        ('vs 3b IP/OOP', {'weight': 0.10, 'score': 0}),
        ('Squeeze', {'weight': 0.10, 'score': 0}),
        ('Defesa da BB', {'weight': 0.10, 'score': 0}),
        ('Defesa da SB', {'weight': 0.05, 'score': 0})
    ])
    
    # Try to load actual scores from pipeline results
    try:
        from app.services.result_storage import get_result_storage
        result_storage = get_result_storage()
        
        result_data = result_storage.get_pipeline_result(token)
        
        if result_data:
                # Check if multi-site or single-site
                is_multi_site = result_data.get('multi_site', False)
                
                if is_multi_site and 'combined' in result_data and group_key in result_data['combined']:
                    # Multi-site: get data from combined
                    group_data = result_data['combined'][group_key]
                    scores = group_data.get('scores', {})
                    rfi_stats = group_data.get('stats', {})  # In combined, stats are under 'stats'
                elif 'groups' in result_data and group_key in result_data['groups']:
                    # Single-site: get data from groups
                    group_data = result_data['groups'][group_key]
                    scores = group_data.get('scores', {})
                    rfi_stats = group_data.get('rfi_stats', {})
                else:
                    # No data found
                    return subgroups
                
                # Check RFI scores from pipeline
                # Pipeline structure now has scores.rfi.overall_score
                if 'rfi' in scores and 'overall_score' in scores['rfi']:
                    # Score from new pipeline structure
                    score_value = scores['rfi']['overall_score']
                    subgroups['RFI']['score'] = score_value
                elif 'overall_score' in scores:
                    # Score from old structure (backward compatibility)
                    score_value = scores['overall_score']
                    subgroups['RFI']['score'] = score_value
                else:
                    # Check if we have enough data for scoring
                    min_opportunities = 20  # Minimum hands for valid score
                    total_opportunities = sum(
                        rfi_stats.get(stat, {}).get('opportunities', 0)
                        for stat in ['Early RFI', 'Middle RFI', 'CO Steal', 'BTN Steal']
                    )
                    
                    if total_opportunities < min_opportunities:
                        # Insufficient data
                        subgroups['RFI']['score'] = None
                    else:
                        # Has data but no score calculated
                        subgroups['RFI']['score'] = None
                
                # Load BvB stats and scores if available
                bvb_stats = group_data.get('bvb_stats', {})
                bvb_scores = group_data.get('scores', {}).get('bvb', {})
                
                # Get BvB score if available (no minimum sample requirement)
                if 'overall_score' in bvb_scores:
                    subgroups['BvB']['score'] = bvb_scores['overall_score']
                else:
                    subgroups['BvB']['score'] = None
                
                # Load 3bet/CC stats and scores if available
                threbet_cc_stats = group_data.get('threbet_cc_stats', {})
                threbet_cc_scores = group_data.get('scores', {}).get('threbet_cc', {})
                
                # Get 3bet/CC score if available (no minimum sample requirement)
                if 'overall_score' in threbet_cc_scores:
                    subgroups['3b & CC']['score'] = threbet_cc_scores['overall_score']
                else:
                    subgroups['3b & CC']['score'] = None
                
                # Load vs 3bet stats and scores if available
                vs_3bet_stats = group_data.get('vs_3bet_stats', {})
                vs_3bet_scores = group_data.get('scores', {}).get('vs_3bet', {})
                
                # Get vs 3bet IP/OOP score if available (no minimum sample requirement)
                if 'overall_score' in vs_3bet_scores:
                    subgroups['vs 3b IP/OOP']['score'] = vs_3bet_scores['overall_score']
                else:
                    subgroups['vs 3b IP/OOP']['score'] = None
                
                # Load Squeeze stats and scores if available
                squeeze_stats = group_data.get('squeeze_stats', {})
                squeeze_scores = group_data.get('scores', {}).get('squeeze', {})
                
                # Get Squeeze score if available (no minimum sample requirement)
                if '_weighted_average' in squeeze_scores:
                    subgroups['Squeeze']['score'] = squeeze_scores['_weighted_average']
                else:
                    subgroups['Squeeze']['score'] = None
                
                # Load BB Defense stats and scores if available
                bb_defense_stats = group_data.get('bb_defense_stats', {})
                bb_defense_scores = group_data.get('scores', {}).get('bb_defense', {})
                
                # Load SB Defense stats and scores if available
                sb_defense_stats = group_data.get('sb_defense_stats', {})
                sb_defense_scores = group_data.get('scores', {}).get('sb_defense', {})
                
                # Get BB Defense score if available (no minimum sample requirement)
                if '_weighted_average' in bb_defense_scores:
                    subgroups['Defesa da BB']['score'] = bb_defense_scores['_weighted_average']
                else:
                    subgroups['Defesa da BB']['score'] = None
                
                # Get SB Defense score if available (no minimum sample requirement)
                if 'score' in sb_defense_scores:
                    subgroups['Defesa da SB']['score'] = sb_defense_scores['score']
                else:
                    subgroups['Defesa da SB']['score'] = None
                
                # Other subgroups still null (not implemented yet)
                for subgroup in subgroups:
                    if subgroup not in ['RFI', 'BvB', '3b & CC', 'vs 3b IP/OOP', 'Squeeze', 'Defesa da BB', 'Defesa da SB']:
                        subgroups[subgroup]['score'] = None
    except Exception as e:
        print(f"Error loading subgroup scores: {e}")
    
    return subgroups

def calculate_weighted_scores(groups):
    """
    Calculate weighted scores for NON-KO and overall average
    """
    from collections import OrderedDict
    scores = {}
    
    # Get NON-KO weights based on hands distribution
    nonko_9max = groups.get('nonko_9max', {})
    nonko_6max = groups.get('nonko_6max', {})
    nonko_9max_hands = nonko_9max.get('hands_count', 0)
    nonko_6max_hands = nonko_6max.get('hands_count', 0)
    total_nonko_hands = nonko_9max_hands + nonko_6max_hands
    
    if total_nonko_hands > 0:
        weight_9max = nonko_9max_hands / total_nonko_hands
        weight_6max = nonko_6max_hands / total_nonko_hands
        
        # Calculate weighted subgroup scores for NON-KO
        # Preserve order from OrderedDict
        nonko_subgroups = OrderedDict()
        
        # Use the ordered list of subgroups
        subgroup_order = ['RFI', 'BvB', '3b & CC', 'vs 3b IP/OOP', 'Squeeze', 'Defesa da BB', 'Defesa da SB']
        subgroups_9max = nonko_9max.get('subgroups', {})
        subgroups_6max = nonko_6max.get('subgroups', {})
        
        for subgroup_name in subgroup_order:
            if subgroup_name in subgroups_9max:
                score_9max = subgroups_9max[subgroup_name].get('score')
                score_6max = subgroups_6max.get(subgroup_name, {}).get('score')
                
                # If both are null, result is null
                if score_9max is None and score_6max is None:
                    weighted_score = None
                else:
                    # Treat None as 0 for calculation if one has data
                    score_9max_calc = score_9max if score_9max is not None else 0
                    score_6max_calc = score_6max if score_6max is not None else 0
                    weighted_score = (score_9max_calc * weight_9max) + (score_6max_calc * weight_6max)
                    weighted_score = round(weighted_score, 1)
                
                nonko_subgroups[subgroup_name] = {
                    'score': weighted_score,
                    'weight': subgroups_9max[subgroup_name].get('weight', 0)
                }
        
        # Calculate NON-KO group score (ignoring null values)
        nonko_group_score = 0
        total_weight = 0
        for sub in nonko_subgroups.values():
            if sub['score'] is not None:
                nonko_group_score += sub['score'] * sub['weight']
                total_weight += sub['weight']
        
        # If no valid scores, the group score is null
        if total_weight == 0:
            nonko_group_score = None
        else:
            # Normalize by actual weights used
            nonko_group_score = nonko_group_score / total_weight if total_weight > 0 else 0
        
        scores['nonko'] = {
            'subgroups': nonko_subgroups,
            'group_score': round(nonko_group_score, 1) if nonko_group_score is not None else None,
            'weight_9max': round(weight_9max * 100, 1),
            'weight_6max': round(weight_6max * 100, 1)
        }
    
    # PKO scores (direct, no weighting needed)
    from collections import OrderedDict
    pko_data = groups.get('pko', {})
    if pko_data.get('hands_count', 0) > 0:
        # Use ordered subgroups
        pko_subgroups = OrderedDict()
        subgroup_order = ['RFI', 'BvB', '3b & CC', 'vs 3b IP/OOP', 'Squeeze', 'Defesa da BB', 'Defesa da SB']
        existing_subgroups = pko_data.get('subgroups', {})
        
        for subgroup_name in subgroup_order:
            if subgroup_name in existing_subgroups:
                pko_subgroups[subgroup_name] = existing_subgroups[subgroup_name]
        
        # Calculate PKO group score (ignoring null values)
        pko_group_score = 0
        total_weight_pko = 0
        for sub in pko_subgroups.values():
            if sub['score'] is not None:
                pko_group_score += sub['score'] * sub['weight']
                total_weight_pko += sub['weight']
        
        # If no valid scores, the group score is null
        if total_weight_pko == 0:
            pko_group_score = None
        else:
            # Normalize by actual weights used
            pko_group_score = pko_group_score / total_weight_pko if total_weight_pko > 0 else 0
        
        scores['pko'] = {
            'subgroups': pko_subgroups,
            'group_score': round(pko_group_score, 1) if pko_group_score is not None else None
        }
    
    # Calculate overall average (only with non-null values)
    # Will use weights: NON-KO 50%, PKO 50% (POSTFLOP not yet implemented)
    overall_score = 0
    active_weights = 0
    
    if 'nonko' in scores and scores['nonko']['group_score'] is not None:
        overall_score += scores['nonko']['group_score'] * 0.5
        active_weights += 0.5
    
    if 'pko' in scores and scores['pko']['group_score'] is not None:
        overall_score += scores['pko']['group_score'] * 0.5
        active_weights += 0.5
    
    # If no valid scores, overall is null
    if active_weights > 0:
        overall_score = overall_score / active_weights
        scores['overall'] = round(overall_score, 1)
    else:
        scores['overall'] = None
    
    return scores


def generate_postflop_all_stats(token, is_multi_site=False):
    """
    Generate aggregated postflop stats from all groups (PKO, NONKO_9MAX, NONKO_6MAX)
    """
    import os
    import json
    from collections import OrderedDict
    
    aggregated_stats = OrderedDict()
    total_postflop_hands = 0  # Track eligible postflop hands
    
    # Load pipeline result to get postflop stats from all groups
    from app.services.result_storage import get_result_storage
    result_storage = get_result_storage()
    
    pipeline_data = result_storage.get_pipeline_result(token)
    
    if pipeline_data:
        
        # Groups to aggregate from
        source_groups = ['pko', 'nonko_9max', 'nonko_6max']
        
        # Initialize postflop_totals with all 20 stats (ensures all stats appear even if no opportunities)
        all_stat_names = [
            "Flop CBet IP %", "Flop CBet 3BetPot IP", "Flop Cbet OOP%",
            "Flop fold vs Cbet IP", "Flop raise Cbet IP", "Flop raise Cbet OOP", "Fold vs Check Raise",
            "Flop bet vs missed Cbet SRP",
            "Turn CBet IP%", "Turn Cbet OOP%", "Turn donk bet", "Turn donk bet SRP vs PFR",
            "Bet turn vs Missed Flop Cbet OOP SRP", "Turn Fold vs CBet OOP",
            "WTSD%", "W$SD%", "W$WSF Rating", "River Agg %", "River bet - Single Rsd Pot", "W$SD% B River"
        ]
        
        postflop_totals = {stat_name: {'opportunities': 0, 'attempts': 0} for stat_name in all_stat_names}
        
        for group in source_groups:
            # Get stats from combined section (if exists)
            group_stats = None
            if 'combined' in pipeline_data and group in pipeline_data['combined']:
                group_stats = pipeline_data['combined'][group].get('postflop_stats', {})
            
            # Get hand_count from sites (always sum from sites, not from combined)
            if 'sites' in pipeline_data:
                for site, site_data in pipeline_data['sites'].items():
                    if group in site_data:
                        # Use postflop_hands_count if available, otherwise use hand_count
                        # V3 calculator should make these equal
                        postflop_count = site_data[group].get('postflop_hands_count', 0)
                        if postflop_count > 0:
                            total_postflop_hands += postflop_count
                        else:
                            total_postflop_hands += site_data[group].get('hand_count', 0)
                        # If stats not found in combined, get from site
                        if not group_stats:
                            group_stats = site_data[group].get('postflop_stats', {})
            
            if group_stats:
                for stat_name, stat_data in group_stats.items():
                    if stat_name not in postflop_totals:
                        postflop_totals[stat_name] = {
                            'opportunities': 0,
                            'attempts': 0
                        }
                    
                    postflop_totals[stat_name]['opportunities'] += stat_data.get('opportunities', 0)
                    postflop_totals[stat_name]['attempts'] += stat_data.get('attempts', 0)
        
        # Weight, ideal and label config for postflop stats
        postflop_config = {
            # Flop Cbet group
            "Flop CBet IP %": {"weight": 40, "ideal": 90, "label": "Flop CBet IP %"},
            "Flop CBet 3BetPot IP": {"weight": 20, "ideal": 90, "label": "Flop CBet 3BetPot IP"},
            "Flop Cbet OOP%": {"weight": 40, "ideal": 37, "label": "Flop Cbet OOP%"},
            # Vs Cbet group
            "Flop fold vs Cbet IP": {"weight": 30, "ideal": 31, "label": "Flop fold vs Cbet IP"},
            "Flop raise Cbet IP": {"weight": 10, "ideal": 12.5, "label": "Raise Flop CBet IP"},
            "Flop raise Cbet OOP": {"weight": 40, "ideal": 20, "label": "Raise Flop CBet OOP"},
            "Fold vs Check Raise": {"weight": 20, "ideal": 32, "label": "Fold vs Check Raise"},
            # vs Skipped Cbet group
            "Flop bet vs missed Cbet SRP": {"weight": 100, "ideal": 60, "label": "Flop bet vs missed Cbet SRP"},
            # Turn Play group
            "Turn CBet IP%": {"weight": 50, "ideal": 60, "label": "Turn CBet IP%"},
            "Turn Cbet OOP%": {"weight": 10, "ideal": 50, "label": "Turn Cbet OOP%"},
            "Turn donk bet": {"weight": 5, "ideal": 8, "label": "Turn Donk Bet"},
            "Turn donk bet SRP vs PFR": {"weight": 5, "ideal": 12, "label": "Turn Donk Bet SRP"},
            "Bet turn vs Missed Flop Cbet OOP SRP": {"weight": 20, "ideal": 45, "label": "Bet Turn vs Missed Flop CBet OOP"},
            "Turn Fold vs CBet OOP": {"weight": 10, "ideal": 43, "label": "Turn Fold vs CBet OOP"},
            # River play group
            "WTSD%": {"weight": 15, "ideal": 30, "label": "WTSD%"},
            "W$SD%": {"weight": 15, "ideal": 50, "label": "W$SD%"},
            "W$WSF Rating": {"weight": 0, "ideal": None, "label": "W$WSF Rating"},  # Não conta
            "River Agg %": {"weight": 15, "ideal": 2.5, "label": "River Agg %"},
            "River bet - Single Rsd Pot": {"weight": 40, "ideal": 45, "label": "River Bet SRP"},
            "W$SD% B River": {"weight": 15, "ideal": 57, "label": "W$SD% B River"}
        }
        
        # Calculate percentages for aggregated stats
        for stat_name, totals in postflop_totals.items():
            opportunities = totals['opportunities']
            attempts = totals['attempts']
            
            # Get config (weight, ideal, label) from config
            config = postflop_config.get(stat_name, {"weight": 0, "ideal": 50, "label": stat_name})
            
            # Calculate percentage
            percentage = 0.0
            if opportunities > 0:
                percentage = (attempts / opportunities) * 100
            
            # Calculate delta (difference from ideal)
            delta = None
            if config['ideal'] is not None:
                delta = round(percentage - config['ideal'], 1)
            
            # Calculate score (0-100 scale based on distance from ideal)
            score = 0
            if config['ideal'] is not None and opportunities > 0:
                diff = abs(percentage - config['ideal'])
                score = max(0, 100 - diff * 2)  # Simplified scoring
            
            aggregated_stats[stat_name] = {
                'label': config['label'],
                'opportunities': opportunities,
                'attempts': attempts,
                'percentage': round(percentage, 1),
                'weight': config['weight'],
                'ideal': config['ideal'],
                'score': round(score, 1) if config['ideal'] is not None else None,
                'delta': delta,
                'eligible_hands': total_postflop_hands
            }
    
    return aggregated_stats

def generate_real_stats(group_key, token, is_multi_site=False):
    """
    Generate real statistics from parsed hands
    """
    import os
    import json
    from collections import OrderedDict
    from app.stats.scoring_calculator import ScoringCalculator
    from app.stats.scoring_config import get_stat_config
    
    stats = OrderedDict()
    
    # Special handling for postflop_all - aggregate from other groups
    if group_key == "postflop_all":
        return generate_postflop_all_stats(token, is_multi_site)
    
    
    # Try to load RFI stats and scores from pipeline results
    rfi_stats = None
    scores = None
    result_data = {}  # Initialize result_data
    try:
        from app.services.result_storage import get_result_storage
        result_storage = get_result_storage()
        
        if is_multi_site:
            # Load from combined stats for multi-site (from Object Storage or local)
            combined_stats = result_storage.get_stats_json(token, group_key, is_multi_site=True)
            if combined_stats:
                # Extract RFI stats from combined
                rfi_stats = {k: v for k, v in combined_stats.items() 
                           if "RFI" in k or k in ["CO Steal", "BTN Steal"]}
                    
            # Also load pipeline result for scores
            result_data = result_storage.get_pipeline_result(token)
            if result_data and 'combined' in result_data and group_key in result_data['combined']:
                scores = result_data['combined'][group_key].get('scores', {})
        else:
            # Single-site logic (existing)
            result_data = result_storage.get_pipeline_result(token)
            if result_data and 'groups' in result_data and group_key in result_data['groups']:
                rfi_stats = result_data['groups'][group_key].get('rfi_stats', {})
                scores = result_data['groups'][group_key].get('scores', {})
    except Exception as e:
        print(f"Error loading RFI stats: {e}")
    
    # Try to load BvB stats from pipeline results
    bvb_stats = None
    threbet_cc_stats = None
    threbet_cc_scores = None
    vs_3bet_stats = None
    vs_3bet_scores = None
    squeeze_stats = None
    squeeze_scores = None
    bb_defense_stats = None
    bb_defense_scores = None
    sb_defense_stats = None
    sb_defense_scores = None
    try:
        if is_multi_site:
            # Load from combined stats for multi-site (from Object Storage or local)
            combined_stats = result_storage.get_stats_json(token, group_key, is_multi_site=True)
            if combined_stats:
                # Extract different stat categories
                bvb_stats = {k: v for k, v in combined_stats.items() 
                           if k in ["SB UO VPIP", "BB fold vs SB steal", "BB raise vs SB limp UOP", "SB Steal"]}
                threbet_cc_stats = {k: v for k, v in combined_stats.items() 
                                  if "3bet" in k or "Cold Call" in k or "VPIP" in k or "BTN fold to CO steal" in k}
                vs_3bet_stats = {k: v for k, v in combined_stats.items() 
                               if "Fold to 3bet" in k}
                squeeze_stats = {k: v for k, v in combined_stats.items() 
                               if "Squeeze" in k}
                bb_defense_stats = {k: v for k, v in combined_stats.items() 
                                  if "BB fold vs CO steal" in k or "BB fold vs BTN steal" in k or "BB resteal vs BTN steal" in k}
                sb_defense_stats = {k: v for k, v in combined_stats.items() 
                                  if "SB fold to CO Steal" in k or "SB fold to BTN Steal" in k or "SB resteal vs BTN" in k}
                    
            # Get scores from combined results
            if 'combined' in result_data and group_key in result_data['combined']:
                combined_scores = result_data['combined'][group_key].get('scores', {})
                threbet_cc_scores = combined_scores.get('threbet_cc', {})
                vs_3bet_scores = combined_scores.get('vs_3bet', {})
                squeeze_scores = combined_scores.get('squeeze', {})
                bb_defense_scores = combined_scores.get('bb_defense', {})
                sb_defense_scores = combined_scores.get('sb_defense', {})
        else:
            # Single-site logic (existing)
            if 'groups' in result_data and group_key in result_data['groups']:
                bvb_stats = result_data['groups'][group_key].get('bvb_stats', {})
                threbet_cc_stats = result_data['groups'][group_key].get('threbet_cc_stats', {})
                threbet_cc_scores = result_data['groups'][group_key].get('scores', {}).get('threbet_cc', {})
                vs_3bet_stats = result_data['groups'][group_key].get('vs_3bet_stats', {})
                vs_3bet_scores = result_data['groups'][group_key].get('scores', {}).get('vs_3bet', {})
                squeeze_stats = result_data['groups'][group_key].get('squeeze_stats', {})
                squeeze_scores = result_data['groups'][group_key].get('scores', {}).get('squeeze', {})
                bb_defense_stats = result_data['groups'][group_key].get('bb_defense_stats', {})
                bb_defense_scores = result_data['groups'][group_key].get('scores', {}).get('bb_defense', {})
                sb_defense_stats = result_data['groups'][group_key].get('sb_defense_stats', {})
                sb_defense_scores = result_data['groups'][group_key].get('scores', {}).get('sb_defense', {})
    except:
        pass
    
    # RFI - use real data if available, otherwise zeros
    if rfi_stats:
        # Get scoring data for each stat if available
        stat_scores = scores.get('stats', {}) if scores else {}
        
        for stat_name in ["Early RFI", "Middle RFI", "CO Steal", "BTN Steal"]:
            stat_data = rfi_stats.get(stat_name, {"opportunities": 0, "attempts": 0})
            # Add scoring information if available
            if stat_name in stat_scores:
                stat_data['score'] = stat_scores[stat_name].get('score', 0)
                stat_data['ideal'] = stat_scores[stat_name].get('ideal', 0)
                stat_data['deviation'] = stat_scores[stat_name].get('deviation', 0)
                stat_data['trend'] = stat_scores[stat_name].get('trend', 'unknown')
            stats[stat_name] = stat_data
    else:
        stats["Early RFI"] = {"opportunities": 0, "attempts": 0}
        stats["Middle RFI"] = {"opportunities": 0, "attempts": 0}
        stats["CO Steal"] = {"opportunities": 0, "attempts": 0}
        stats["BTN Steal"] = {"opportunities": 0, "attempts": 0}
    
    # BvB - use real data if available
    if bvb_stats:
        for stat_name in ["SB UO VPIP", "BB fold vs SB steal", "BB raise vs SB limp UOP", "SB Steal"]:
            stat_data = bvb_stats.get(stat_name, {"opportunities": 0, "attempts": 0})
            stats[stat_name] = stat_data
    else:
        stats["SB UO VPIP"] = {"opportunities": 0, "attempts": 0}
        stats["BB fold vs SB steal"] = {"opportunities": 0, "attempts": 0}
        stats["BB raise vs SB limp UOP"] = {"opportunities": 0, "attempts": 0}
        stats["SB Steal"] = {"opportunities": 0, "attempts": 0}
    
    # Ranges de CC/3Bet IP - use real data if available
    if not threbet_cc_stats:
        threbet_cc_stats = {}
    if not threbet_cc_scores:
        threbet_cc_scores = {}
    
    # EP stats
    stats["EP 3bet"] = threbet_cc_stats.get("EP 3bet", {"opportunities": 0, "attempts": 0})
    stats["EP Cold Call"] = threbet_cc_stats.get("EP Cold Call", {"opportunities": 0, "attempts": 0})
    stats["EP VPIP"] = threbet_cc_stats.get("EP VPIP", {"opportunities": 0, "attempts": 0})
    
    # MP stats
    stats["MP 3bet"] = threbet_cc_stats.get("MP 3bet", {"opportunities": 0, "attempts": 0})
    stats["MP Cold Call"] = threbet_cc_stats.get("MP Cold Call", {"opportunities": 0, "attempts": 0})
    stats["MP VPIP"] = threbet_cc_stats.get("MP VPIP", {"opportunities": 0, "attempts": 0})
    
    # CO stats
    stats["CO 3bet"] = threbet_cc_stats.get("CO 3bet", {"opportunities": 0, "attempts": 0})
    stats["CO Cold Call"] = threbet_cc_stats.get("CO Cold Call", {"opportunities": 0, "attempts": 0})
    stats["CO VPIP"] = threbet_cc_stats.get("CO VPIP", {"opportunities": 0, "attempts": 0})
    
    # BTN stats
    stats["BTN 3bet"] = threbet_cc_stats.get("BTN 3bet", {"opportunities": 0, "attempts": 0})
    stats["BTN Cold Call"] = threbet_cc_stats.get("BTN Cold Call", {"opportunities": 0, "attempts": 0})
    stats["BTN VPIP"] = threbet_cc_stats.get("BTN VPIP", {"opportunities": 0, "attempts": 0})
    stats["BTN fold to CO steal"] = threbet_cc_stats.get("BTN fold to CO steal", {"opportunities": 0, "attempts": 0})
    
    # vs 3bet IP/OOP - use real data if available
    if not vs_3bet_stats:
        vs_3bet_stats = {}
    if not vs_3bet_scores:
        vs_3bet_scores = {}
    
    stats["Fold to 3bet IP"] = vs_3bet_stats.get("Fold to 3bet IP", {"opportunities": 0, "attempts": 0})
    stats["Fold to 3bet OOP"] = vs_3bet_stats.get("Fold to 3bet OOP", {"opportunities": 0, "attempts": 0})
    stats["Fold to 3bet"] = vs_3bet_stats.get("Fold to 3bet", {"opportunities": 0, "attempts": 0})
    
    # Squeeze - use real data if available
    if not squeeze_stats:
        squeeze_stats = {}
    if not squeeze_scores:
        squeeze_scores = {}
    
    stats["Squeeze"] = squeeze_stats.get("Squeeze", {"opportunities": 0, "attempts": 0})
    stats["Squeeze vs BTN Raiser"] = squeeze_stats.get("Squeeze vs BTN Raiser", {"opportunities": 0, "attempts": 0})
    
    # Defesa da BB - use real data if available
    if not bb_defense_stats:
        bb_defense_stats = {}
    if not bb_defense_scores:
        bb_defense_scores = {}
    
    stats["BB fold vs CO steal"] = bb_defense_stats.get("BB fold vs CO steal", {"opportunities": 0, "attempts": 0})
    stats["BB fold vs BTN steal"] = bb_defense_stats.get("BB fold vs BTN steal", {"opportunities": 0, "attempts": 0})
    # BB fold vs SB steal comes from BvB stats
    stats["BB fold vs SB steal"] = bvb_stats.get("BB fold vs SB steal", {"opportunities": 0, "attempts": 0})
    stats["BB resteal vs BTN steal"] = bb_defense_stats.get("BB resteal vs BTN steal", {"opportunities": 0, "attempts": 0})
    
    # Defesa da SB - use real data if available
    if not sb_defense_stats:
        sb_defense_stats = {}
    if not sb_defense_scores:
        sb_defense_scores = {}
    
    stats["SB fold to CO Steal"] = sb_defense_stats.get("SB fold to CO Steal", {"opportunities": 0, "attempts": 0})
    stats["SB fold to BTN Steal"] = sb_defense_stats.get("SB fold to BTN Steal", {"opportunities": 0, "attempts": 0})
    stats["SB resteal vs BTN"] = sb_defense_stats.get("SB resteal vs BTN", {"opportunities": 0, "attempts": 0})
    
    # Calculate individual scores for each stat using ScoringCalculator
    scoring_calc = ScoringCalculator()
    
    # Add scores and ideal values to each stat
    for stat_name in stats:
        stat_data = stats[stat_name]
        
        # Check if we have score data from threbet_cc_scores, vs_3bet_scores, squeeze_scores, bb_defense_scores or sb_defense_scores
        if stat_name in threbet_cc_scores:
            score_data = threbet_cc_scores[stat_name]
            if 'score' in score_data:
                stats[stat_name]['score'] = score_data['score']
            if 'ideal' in score_data:
                stats[stat_name]['ideal'] = score_data['ideal']
            if 'percentage' in score_data and score_data['percentage'] is not None:
                stats[stat_name]['percentage'] = score_data['percentage']
            continue
        elif stat_name in vs_3bet_scores:
            score_data = vs_3bet_scores[stat_name]
            if 'score' in score_data:
                stats[stat_name]['score'] = score_data['score']
            if 'ideal' in score_data:
                stats[stat_name]['ideal'] = score_data['ideal']
            if 'percentage' in score_data and score_data['percentage'] is not None:
                stats[stat_name]['percentage'] = score_data['percentage']
            continue
        elif stat_name in squeeze_scores:
            score_data = squeeze_scores[stat_name]
            if 'score' in score_data:
                stats[stat_name]['score'] = score_data['score']
            if 'ideal' in score_data:
                stats[stat_name]['ideal'] = score_data['ideal']
            if 'percentage' in score_data and score_data['percentage'] is not None:
                stats[stat_name]['percentage'] = score_data['percentage']
            continue
        elif stat_name in bb_defense_scores:
            score_data = bb_defense_scores[stat_name]
            if 'score' in score_data:
                stats[stat_name]['score'] = score_data['score']
            if 'ideal' in score_data:
                stats[stat_name]['ideal'] = score_data['ideal']
            if 'percentage' in score_data and score_data['percentage'] is not None:
                stats[stat_name]['percentage'] = score_data['percentage']
            continue
        elif sb_defense_scores and 'details' in sb_defense_scores and stat_name in sb_defense_scores.get('details', {}):
            score_data = sb_defense_scores['details'][stat_name]
            if 'score' in score_data:
                stats[stat_name]['score'] = score_data['score']
            if 'ideal' in score_data:
                stats[stat_name]['ideal'] = score_data['ideal']
            if 'percentage' in score_data and score_data['percentage'] is not None:
                stats[stat_name]['percentage'] = score_data['percentage']
            continue
        
        # Otherwise calculate using config
        config = get_stat_config(group_key, stat_name)
        
        if config and stat_data.get('opportunities', 0) >= 1:  # Generate score from first occurrence
            # Calculate percentage
            attempts = stat_data.get('attempts', 0)
            opportunities = stat_data.get('opportunities', 0)
            percentage = (attempts / opportunities) * 100 if opportunities > 0 else 0
            
            # Get detailed score info
            detailed = scoring_calc.calculate_detailed_score(percentage, opportunities, config)
            
            # Add score and ideal to stat
            stats[stat_name]['score'] = detailed['score']
            stats[stat_name]['ideal'] = detailed['ideal']
            stats[stat_name]['deviation'] = detailed['deviation']
            stats[stat_name]['trend'] = detailed['trend']
        else:
            # No score if insufficient data or no config
            stats[stat_name]['score'] = None
            if config:
                stats[stat_name]['ideal'] = config.get('ideal')
    
    # Try to load POSTFLOP stats from pipeline results
    postflop_stats = None
    try:
        if is_multi_site:
            # Load from combined results for multi-site
            if 'combined' in result_data and group_key in result_data['combined']:
                postflop_stats = result_data['combined'][group_key].get('postflop_stats', {})
        else:
            # Single-site logic
            if 'groups' in result_data and group_key in result_data['groups']:
                postflop_stats = result_data['groups'][group_key].get('postflop_stats', {})
    except:
        pass
    
    # Add POSTFLOP stats if available
    if postflop_stats:
        # List of postflop stat names we expect
        postflop_stat_names = [
            "Flop CBet IP", "Flop CBet 3Bet Pot IP", "Flop CBet OOP",
            "Flop Fold vs CBet IP", "Flop Raise CBet IP", "Flop Raise CBet OOP",
            "Fold vs Check Raise", "Flop Bet vs Missed CBet SRP",
            "Turn CBet IP", "Turn CBet OOP", "Turn Donk Bet", "Turn Donk Bet SRP vs PFR",
            "Bet Turn vs Missed Flop CBet OOP SRP", "Turn Fold vs CBet OOP",
            "WTSD", "Won $ at Showdown", "Won When Saw Flop",
            "River Agg %", "River Bet Single Raised Pot", "Won $ SD when Bet River"
        ]
        
        for stat_name in postflop_stat_names:
            if stat_name in postflop_stats:
                stats[stat_name] = postflop_stats[stat_name]
            else:
                # Add zero stats for missing postflop stats
                stats[stat_name] = {"opportunities": 0, "attempts": 0}
    
    
    return stats