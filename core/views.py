import json
import logging
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from .utils.parquet_utils import ParquetDataManager

logger = logging.getLogger(__name__)


@login_required
def home(request):
    """Home page with dashboard overview."""
    return render(request, 'core/home.html')


@login_required
def commercial_rate_insights_map(request):
    """
    Interactive US Map Landing Page for Commercial Rate Insights
    Shows available states and allows users to click to view state-specific data
    """
    try:
        # Get available states
        available_states = ParquetDataManager.get_available_states()
        
        context = {
            'states_data': json.dumps(available_states),
            'available_count': len([s for s in available_states.values() if s == 'available']),
            'total_states': len(available_states)
        }
        
    except Exception as e:
        logger.error(f"Error in commercial_rate_insights_map view: {str(e)}")
        context = {
            'states_data': '{}',
            'available_count': 0,
            'total_states': 0,
            'error_message': 'An error occurred while loading the map.'
        }
    
    return render(request, 'core/commercial_rate_insights_map.html', context)


@login_required
def commercial_rate_insights_state(request, state_code):
    """
    State-specific Commercial Rate Insights Dashboard
    Displays interactive visualizations and analysis for a specific state
    """
    try:
        # Validate state code
        state_code = state_code.upper()
        available_states = ParquetDataManager.get_available_states()
        
        if state_code not in available_states or available_states[state_code] != 'available':
            context = {
                'has_data': False,
                'error_message': f'Sorry, {state_code} data is not available yet. Please try another state.',
                'state_code': state_code,
                'state_name': ParquetDataManager.get_state_name(state_code)
            }
            return render(request, 'core/commercial_rate_insights_state.html', context)
        
        # Initialize data manager with state-specific file
        data_manager = ParquetDataManager(state=state_code)
        
        # Check if this is a comparison request
        if 'compare' in request.GET:
            compare_data = json.loads(request.GET['compare'])
            comparison_stats = data_manager.get_comparison_stats(
                orgs=compare_data.get('orgs', []),
                payers=compare_data.get('payers', [])
            )
            return JsonResponse(comparison_stats)
        
        # Get active filters from request
        active_filters = {
            'payer': request.GET.get('payer'),
            'org_name': request.GET.get('org_name'),
            'procedure_set': request.GET.get('procedure_set'),
            'procedure_class': request.GET.get('procedure_class'),
            'procedure_group': request.GET.get('procedure_group'),
            'cbsa': request.GET.get('cbsa'),
            'billing_code': request.GET.get('billing_code')
        }
        
        # Remove empty filters
        active_filters = {k: v for k, v in active_filters.items() if v}
        
        # Get filtered options for each field based on current selections
        filters = {
            'payers': data_manager.get_unique_values('payer', active_filters),
            'organizations': data_manager.get_unique_values('org_name', active_filters),
            'procedure_sets': data_manager.get_unique_values('procedure_set', active_filters),
            'procedure_classes': data_manager.get_unique_values('procedure_class', active_filters),
            'procedure_groups': data_manager.get_unique_values('procedure_group', active_filters),
            'cbsa_regions': data_manager.get_unique_values('cbsa', active_filters),
            'billing_codes': data_manager.get_unique_values('billing_code', active_filters),
        }
        
        # Get aggregated statistics with filters
        stats = data_manager.get_aggregated_stats(active_filters)
        
        # Get sample records
        sample_records = data_manager.get_sample_records(active_filters, limit=10)
        
        context = {
            'filters': filters,
            'stats': stats,
            'active_filters': active_filters,
            'sample_records': sample_records,
            'has_data': True,
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code)
        }
            
    except Exception as e:
        logger.error(f"Error in commercial_rate_insights_state view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while processing the data.',
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code)
        }
    
    return render(request, 'core/commercial_rate_insights_state.html', context)


@login_required
def commercial_rate_insights(request):
    """
    Commercial Rate Insights Dashboard (Legacy - redirects to map)
    """
    return commercial_rate_insights_map(request)


@login_required
def commercial_rate_insights_compare(request):
    """Side-by-side comparison of Commercial Rate Insights."""
    data_manager = ParquetDataManager()
    
    # Get active filters from query params
    active_filters = {
        'procedure_set': request.GET.get('procedure_set', ''),
        'procedure_class': request.GET.get('procedure_class', ''),
        'procedure_group': request.GET.get('procedure_group', ''),
        'cbsa': request.GET.get('cbsa', ''),
        'billing_code': request.GET.get('billing_code', '')
    }
    
    # Get selected entities from query params
    selected_orgs = request.GET.getlist('orgs[]', [])
    selected_payers = request.GET.getlist('payers[]', [])
    
    # Get filter options based on active filters
    filters = {
        'procedure_sets': data_manager.get_unique_values('procedure_set', active_filters),
        'procedure_classes': data_manager.get_unique_values('procedure_class', active_filters),
        'procedure_groups': data_manager.get_unique_values('procedure_group', active_filters),
        'cbsa_regions': data_manager.get_unique_values('cbsa', active_filters),
        'billing_codes': data_manager.get_unique_values('billing_code', active_filters)
    }
    
    # Get all available orgs and payers for selection, filtered by active filters
    all_orgs = data_manager.get_unique_values('org_name', active_filters)
    all_payers = data_manager.get_unique_values('payer', active_filters)
    
    # Get data for each selected entity
    entities_data = []
    
    # Process organizations
    for org in selected_orgs:
        # Combine org filter with active filters
        org_filters = {**active_filters, 'org_name': org}
        stats = data_manager.get_aggregated_stats(org_filters)
        sample_records = data_manager.get_sample_records(org_filters, limit=5)
        entities_data.append({
            'name': org,
            'type': 'organization',
            'stats': stats,
            'sample_records': sample_records
        })
    
    # Process payers
    for payer in selected_payers:
        # Combine payer filter with active filters
        payer_filters = {**active_filters, 'payer': payer}
        stats = data_manager.get_aggregated_stats(payer_filters)
        sample_records = data_manager.get_sample_records(payer_filters, limit=5)
        entities_data.append({
            'name': payer,
            'type': 'payer',
            'stats': stats,
            'sample_records': sample_records
        })
    
    context = {
        'filters': filters,
        'active_filters': active_filters,
        'all_orgs': all_orgs,
        'all_payers': all_payers,
        'selected_orgs': selected_orgs,
        'selected_payers': selected_payers,
        'entities_data': entities_data,
        'max_selections': 4  # Limit to 4 side-by-side comparisons
    }
    
    return render(request, 'core/commercial_rate_insights_compare.html', context)
