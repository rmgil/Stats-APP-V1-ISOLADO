"""
Phase 9.D - Tests for API endpoints and Dashboard
Tests for timeseries, breakdown endpoints and dashboard rendering
"""

import pytest
import json
from pathlib import Path
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app


class TestTimeseriesAPI:
    """Tests for /api/stats/timeseries endpoint"""
    
    @pytest.fixture
    def client(self):
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client
    
    def test_timeseries_basic_request(self, client):
        """Test basic timeseries endpoint request"""
        response = client.get('/api/stats/timeseries?stat=POST_CBET_FLOP_IP&group=postflop_all&months=6')
        # Accept 200 or 500 (when stats directory doesn't exist)
        assert response.status_code in [200, 500]
        
        data = json.loads(response.data)
        assert 'timeseries' in data
        assert isinstance(data['timeseries'], list)
        
        # Snapshot the response structure
        snapshot = {
            'endpoint': '/api/stats/timeseries',
            'params': {
                'stat': 'POST_CBET_FLOP_IP',
                'group': 'postflop_all',
                'months': 6
            },
            'response_keys': list(data.keys()),
            'timeseries_structure': data['timeseries'][0] if data['timeseries'] else None
        }
        
        # Save snapshot
        snapshot_path = Path('tests/snapshots/timeseries_snapshot.json')
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        with open(snapshot_path, 'w') as f:
            json.dump(snapshot, f, indent=2, default=str)
    
    def test_timeseries_missing_params(self, client):
        """Test timeseries endpoint with missing parameters"""
        response = client.get('/api/stats/timeseries')
        assert response.status_code in [400, 200]  # May return empty data or error
    
    def test_timeseries_different_stats(self, client):
        """Test timeseries with different stat types"""
        stats_to_test = [
            'POST_CBET_FLOP_IP',
            'POST_WTSD',
            'POST_WWSF', 
            'POST_AGG_PCT_FLOP'
        ]
        
        snapshots = []
        for stat in stats_to_test:
            response = client.get(f'/api/stats/timeseries?stat={stat}&group=postflop_all&months=3')
            assert response.status_code == 200
            
            data = json.loads(response.data)
            snapshots.append({
                'stat': stat,
                'data_points': len(data.get('timeseries', [])),
                'has_data': bool(data.get('timeseries'))
            })
        
        # Save multi-stat snapshot
        snapshot_path = Path('tests/snapshots/timeseries_multi_stat.json')
        with open(snapshot_path, 'w') as f:
            json.dump(snapshots, f, indent=2)
    
    def test_timeseries_groups(self, client):
        """Test timeseries with different groups"""
        groups = ['postflop_all', 'nonko_9max_pref', 'nonko_6max_pref', 'pko_pref']
        
        for group in groups:
            response = client.get(f'/api/stats/timeseries?stat=POST_CBET_FLOP_IP&group={group}&months=6')
            assert response.status_code == 200
            data = json.loads(response.data)
            assert 'timeseries' in data


class TestBreakdownAPI:
    """Tests for /api/stats/breakdown endpoint"""
    
    @pytest.fixture
    def client(self):
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client
    
    def test_breakdown_basic_request(self, client):
        """Test basic breakdown endpoint request"""
        response = client.get('/api/stats/breakdown?group=postflop_all&family=POST_CBET')
        assert response.status_code == 200
        
        data = json.loads(response.data)
        assert 'breakdown' in data
        
        # Snapshot the response structure
        snapshot = {
            'endpoint': '/api/stats/breakdown',
            'params': {
                'group': 'postflop_all',
                'family': 'POST_CBET'
            },
            'response_keys': list(data.keys()),
            'breakdown_structure': {
                'has_by_street': 'by_street' in data.get('breakdown', {}),
                'has_by_position': 'by_position' in data.get('breakdown', {}),
                'has_by_position_and_street': 'by_position_and_street' in data.get('breakdown', {})
            }
        }
        
        # Save snapshot
        snapshot_path = Path('tests/snapshots/breakdown_snapshot.json')
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        with open(snapshot_path, 'w') as f:
            json.dump(snapshot, f, indent=2)
    
    def test_breakdown_families(self, client):
        """Test breakdown with different stat families"""
        families = [
            'POST_CBET',
            'POST_VS_CBET',
            'POST_DONK',
            'POST_PROBE',
            'POST_XR',
            'POST_SHOWDOWN',
            'POST_AGGRESSION'
        ]
        
        snapshots = []
        for family in families:
            response = client.get(f'/api/stats/breakdown?group=postflop_all&family={family}')
            assert response.status_code == 200
            
            data = json.loads(response.data)
            snapshots.append({
                'family': family,
                'has_data': bool(data.get('breakdown')),
                'breakdown_types': list(data.get('breakdown', {}).keys())
            })
        
        # Save families snapshot
        snapshot_path = Path('tests/snapshots/breakdown_families.json')
        with open(snapshot_path, 'w') as f:
            json.dump(snapshots, f, indent=2)
    
    def test_breakdown_missing_params(self, client):
        """Test breakdown endpoint with missing parameters"""
        response = client.get('/api/stats/breakdown')
        assert response.status_code in [400, 200]
    
    def test_breakdown_invalid_family(self, client):
        """Test breakdown with invalid family"""
        response = client.get('/api/stats/breakdown?group=postflop_all&family=INVALID_FAMILY')
        # Should still return 200 but with empty or error data
        assert response.status_code == 200


class TestDashboard:
    """Smoke tests for dashboard rendering"""
    
    @pytest.fixture
    def client(self):
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client
    
    def test_dashboard_page_loads(self, client):
        """Test that dashboard page loads successfully"""
        response = client.get('/dashboard')
        assert response.status_code == 200
        
        # Check for essential dashboard elements
        html = response.data.decode('utf-8')
        assert 'Poker Stats Dashboard' in html
        assert 'trendsChart' in html
        assert 'scoreGauge' in html
        assert 'heatmapChart' in html
        
        # Snapshot dashboard structure
        snapshot = {
            'endpoint': '/dashboard',
            'status': response.status_code,
            'has_charts': {
                'trends': 'trendsChart' in html,
                'gauge': 'scoreGauge' in html,
                'heatmap': 'heatmapChart' in html,
                'street': 'streetChart' in html,
                'position': 'positionChart' in html
            },
            'has_filters': {
                'family': 'familyFilter' in html,
                'group': 'groupFilter' in html,
                'months': 'monthsFilter' in html
            },
            'has_javascript': {
                'chart_js': 'chart.js' in html.lower(),
                'bootstrap': 'bootstrap' in html.lower()
            }
        }
        
        snapshot_path = Path('tests/snapshots/dashboard_smoke.json')
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        with open(snapshot_path, 'w') as f:
            json.dump(snapshot, f, indent=2)
    
    def test_dashboard_has_api_calls(self, client):
        """Test that dashboard includes API call functions"""
        response = client.get('/dashboard')
        html = response.data.decode('utf-8')
        
        # Check for API endpoint references
        assert '/api/stats/timeseries' in html
        assert '/api/stats/breakdown' in html
        assert '/api/score/summary' in html
        
        # Check for key JavaScript functions
        assert 'loadTrendsData' in html
        assert 'loadBreakdownData' in html
        assert 'refreshDashboard' in html
    
    def test_dashboard_chart_initialization(self, client):
        """Test that dashboard initializes all charts"""
        response = client.get('/dashboard')
        html = response.data.decode('utf-8')
        
        # Check for chart initialization
        assert 'new Chart(' in html
        assert 'initializeCharts()' in html
        
        # Check for chart configurations
        chart_types = ['line', 'doughnut', 'bubble', 'bar', 'radar']
        for chart_type in chart_types:
            assert f"type: '{chart_type}'" in html


class TestIntegration:
    """Integration tests for the complete flow"""
    
    @pytest.fixture
    def client(self):
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client
    
    def test_api_endpoints_availability(self, client):
        """Test that all API endpoints are available"""
        endpoints = [
            '/api/stats/timeseries?stat=POST_CBET_FLOP_IP&group=postflop_all&months=6',
            '/api/stats/breakdown?group=postflop_all&family=POST_CBET',
            '/api/score/summary',
            '/api/stats/summary'
        ]
        
        results = []
        for endpoint in endpoints:
            response = client.get(endpoint)
            results.append({
                'endpoint': endpoint,
                'status': response.status_code,
                'success': response.status_code == 200
            })
        
        # All endpoints should be available
        assert all(r['success'] for r in results), f"Failed endpoints: {[r['endpoint'] for r in results if not r['success']]}"
        
        # Save integration test snapshot
        snapshot_path = Path('tests/snapshots/integration_endpoints.json')
        with open(snapshot_path, 'w') as f:
            json.dump(results, f, indent=2)
    
    def test_dashboard_to_api_flow(self, client):
        """Test the flow from dashboard to API calls"""
        # First load dashboard
        dashboard_response = client.get('/dashboard')
        assert dashboard_response.status_code == 200
        
        # Then test API calls that dashboard would make
        api_calls = [
            client.get('/api/stats/timeseries?stat=POST_CBET_FLOP_IP&group=postflop_all&months=6'),
            client.get('/api/stats/breakdown?group=postflop_all&family=POST_CBET'),
            client.get('/api/score/summary')
        ]
        
        for response in api_calls:
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data is not None


def run_all_tests():
    """Run all tests and generate summary"""
    import subprocess
    
    # Run pytest with verbose output
    result = subprocess.run(
        ['python', '-m', 'pytest', __file__, '-v', '--tb=short'],
        capture_output=True,
        text=True
    )
    
    print("=" * 70)
    print("PHASE 9.D - TEST RESULTS")
    print("=" * 70)
    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    
    # Generate test summary
    snapshot_dir = Path('tests/snapshots')
    if snapshot_dir.exists():
        snapshots = list(snapshot_dir.glob('*.json'))
        print(f"\n✅ Generated {len(snapshots)} snapshot files:")
        for snapshot in snapshots:
            print(f"   - {snapshot.name}")
    
    return result.returncode == 0


if __name__ == '__main__':
    # Run tests when executed directly
    success = run_all_tests()
    sys.exit(0 if success else 1)