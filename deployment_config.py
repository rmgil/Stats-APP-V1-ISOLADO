"""
Deployment-specific configurations and middleware
"""
import os
from flask import jsonify, make_response

def configure_deployment(app):
    """Apply deployment-specific configurations to Flask app"""
    
    # Force JSON responses for all errors in deployment
    if os.environ.get('DEPLOYMENT') == 'true':
        
        @app.errorhandler(404)
        def not_found_error(error):
            return make_response(jsonify({
                'success': False,
                'error': 'Recurso não encontrado'
            }), 404)
        
        @app.errorhandler(413)
        def request_entity_too_large(error):
            return make_response(jsonify({
                'success': False,
                'error': 'Ficheiro muito grande. Por favor, use um ficheiro menor que 500MB.'
            }), 413)
        
        @app.errorhandler(503)
        def service_unavailable(error):
            return make_response(jsonify({
                'success': False,
                'error': 'Serviço temporariamente indisponível. Tente novamente.'
            }), 503)
        
        @app.errorhandler(502)
        def bad_gateway(error):
            return make_response(jsonify({
                'success': False,
                'error': 'Erro de gateway. O servidor pode estar sobrecarregado.'
            }), 502)
        
        @app.errorhandler(504)
        def gateway_timeout(error):
            return make_response(jsonify({
                'success': False,
                'error': 'Tempo limite excedido. O ficheiro pode ser muito grande ou complexo.'
            }), 504)
        
        # Middleware to ensure JSON responses
        @app.after_request
        def ensure_json_response(response):
            # Only modify error responses
            if response.status_code >= 400:
                # Check if response is already JSON
                if not response.content_type.startswith('application/json'):
                    # Convert HTML error to JSON
                    response = make_response(jsonify({
                        'success': False,
                        'error': f'Erro {response.status_code}: Problema no processamento do pedido',
                        'status': response.status_code
                    }), response.status_code)
                    response.headers['Content-Type'] = 'application/json'
            return response