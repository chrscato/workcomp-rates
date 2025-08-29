import json
import logging
from django.shortcuts import render
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import JsonResponse
from django.utils import timezone
from datetime import timedelta
from .utils.parquet_utils import ParquetDataManager
from .models import UserActivity
from django.db import models

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
        
        # Get active filters from request
        active_filters = {
            'payer': request.GET.get('payer'),
            'org_name': request.GET.get('org_name'),
            'procedure_set': request.GET.get('procedure_set'),
            'procedure_class': request.GET.get('procedure_class'),
            'procedure_group': request.GET.get('procedure_group'),
            'cbsa': request.GET.get('cbsa'),
            'billing_code': request.GET.get('billing_code'),
            'tin_value': request.GET.get('tin_value'),
            'primary_taxonomy_code': request.GET.get('primary_taxonomy_code'),
            'primary_taxonomy_desc': request.GET.get('primary_taxonomy_desc')
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
            'tin_values': data_manager.get_unique_values('tin_value', active_filters),
            'primary_taxonomy_codes': data_manager.get_unique_values('primary_taxonomy_code', active_filters),
            'primary_taxonomy_descs': data_manager.get_unique_values('primary_taxonomy_desc', active_filters),
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
        
        # Debug logging
        logger.info(f"State: {state_code}")
        logger.info(f"Active filters: {active_filters}")
        logger.info(f"Available organizations: {len(filters['organizations'])}")
        logger.info(f"Available payers: {len(filters['payers'])}")
            
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
    Commercial Rate Insights Dashboard with sample data for demonstration
    """
    try:
        # Initialize data manager with sample data
        data_manager = ParquetDataManager()
        
        # Get active filters from request
        active_filters = {
            'payer': request.GET.get('payer'),
            'org_name': request.GET.get('org_name'),
            'procedure_set': request.GET.get('procedure_set'),
            'procedure_class': request.GET.get('procedure_class'),
            'procedure_group': request.GET.get('procedure_group'),
            'cbsa': request.GET.get('cbsa'),
            'billing_code': request.GET.get('billing_code'),
            'tin_value': request.GET.get('tin_value'),
            'primary_taxonomy_code': request.GET.get('primary_taxonomy_code'),
            'primary_taxonomy_desc': request.GET.get('primary_taxonomy_desc')
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
            'tin_values': data_manager.get_unique_values('tin_value', active_filters),
            'primary_taxonomy_codes': data_manager.get_unique_values('primary_taxonomy_code', active_filters),
            'primary_taxonomy_descs': data_manager.get_unique_values('primary_taxonomy_desc', active_filters),
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
            'has_data': True
        }
        
    except Exception as e:
        logger.error(f"Error in commercial_rate_insights view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while processing the data.'
        }
    
    return render(request, 'core/commercial_rate_insights.html', context)


@login_required
def commercial_rate_insights_compare(request, state_code):
    """
    State-specific Commercial Rate Insights Comparison View
    Shows side-by-side comparison of organizations and payers for a specific state
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
            return render(request, 'core/commercial_rate_insights_compare.html', context)
        
        # Initialize data manager with state-specific file
        data_manager = ParquetDataManager(state=state_code)
        
        # Get active filters from request
        active_filters = {
            'payer': request.GET.get('payer'),
            'org_name': request.GET.get('org_name'),
            'procedure_set': request.GET.get('procedure_set'),
            'procedure_class': request.GET.get('procedure_class'),
            'procedure_group': request.GET.get('procedure_group'),
            'cbsa': request.GET.get('cbsa'),
            'billing_code': request.GET.get('billing_code'),
            'tin_value': request.GET.get('tin_value'),
            'primary_taxonomy_code': request.GET.get('primary_taxonomy_code'),
            'primary_taxonomy_desc': request.GET.get('primary_taxonomy_desc')
        }
        
        # Remove empty filters
        active_filters = {k: v for k, v in active_filters.items() if v}
        
        # Get selected entities for comparison
        compare_orgs = request.GET.getlist('compare_orgs[]', [])
        compare_payers = request.GET.getlist('compare_payers[]', [])
        
        # Get comparison data
        comparison_data = []
        if compare_orgs or compare_payers:
            # Process organizations
            for org in compare_orgs:
                # Combine org filter with active filters
                org_filters = {**active_filters, 'org_name': org}
                stats = data_manager.get_aggregated_stats(org_filters)
                sample_records = data_manager.get_sample_records(org_filters, limit=5)
                comparison_data.append({
                    'name': org,
                    'type': 'organization',
                    'stats': stats,
                    'sample_records': sample_records
                })
            
            # Process payers
            for payer in compare_payers:
                # Combine payer filter with active filters
                payer_filters = {**active_filters, 'payer': payer}
                stats = data_manager.get_aggregated_stats(payer_filters)
                sample_records = data_manager.get_sample_records(payer_filters, limit=5)
                comparison_data.append({
                    'name': payer,
                    'type': 'payer',
                    'stats': stats,
                    'sample_records': sample_records
                })
        
        # Get filtered options for each field based on current selections
        filters = {
            'payers': data_manager.get_unique_values('payer', active_filters),
            'organizations': data_manager.get_unique_values('org_name', active_filters),
            'procedure_sets': data_manager.get_unique_values('procedure_set', active_filters),
            'procedure_classes': data_manager.get_unique_values('procedure_class', active_filters),
            'procedure_groups': data_manager.get_unique_values('procedure_group', active_filters),
            'cbsa_regions': data_manager.get_unique_values('cbsa', active_filters),
            'billing_codes': data_manager.get_unique_values('billing_code', active_filters),
            'tin_values': data_manager.get_unique_values('tin_value', active_filters),
            'primary_taxonomy_codes': data_manager.get_unique_values('primary_taxonomy_code', active_filters),
            'primary_taxonomy_descs': data_manager.get_unique_values('primary_taxonomy_desc', active_filters),
        }
        
        # Get base state statistics (without comparison filters)
        base_stats = data_manager.get_aggregated_stats(active_filters)
        
        context = {
            'filters': filters,
            'base_stats': base_stats,
            'active_filters': active_filters,
            'comparison_data': comparison_data,
            'compare_orgs_selected': compare_orgs,
            'compare_payers_selected': compare_payers,
            'has_data': True,
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code)
        }
        
        # Debug logging
        logger.info(f"Compare view - State: {state_code}")
        logger.info(f"Active filters: {active_filters}")
        logger.info(f"Compare orgs: {compare_orgs}")
        logger.info(f"Compare payers: {compare_payers}")
        logger.info(f"Comparison data count: {len(comparison_data)}")
            
    except Exception as e:
        logger.error(f"Error in commercial_rate_insights_compare view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while processing the comparison data.',
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code)
        }
    
    return render(request, 'core/commercial_rate_insights_compare.html', context)


@login_required
@user_passes_test(lambda u: u.is_staff)
def user_activity_dashboard(request):
    """Dashboard for viewing user activity data (staff only)."""
    
    # Get date range from request or default to last 7 days
    days = int(request.GET.get('days', 7))
    end_date = timezone.now()
    start_date = end_date - timedelta(days=days)
    
    # Get activity data
    activities = UserActivity.objects.filter(
        timestamp__gte=start_date,
        timestamp__lte=end_date
    ).select_related('user').order_by('-timestamp')
    
    # Get summary statistics
    total_activities = activities.count()
    unique_users = activities.values('user').distinct().count()
    
    # Action breakdown
    action_counts = activities.values('action').annotate(
        count=models.Count('id')
    ).order_by('-count')
    
    # Recent activities (last 50)
    recent_activities = activities[:50]
    
    # Top active users
    top_users = activities.values('user__username').annotate(
        count=models.Count('id')
    ).order_by('-count')[:10]
    
    context = {
        'total_activities': total_activities,
        'unique_users': unique_users,
        'action_counts': action_counts,
        'recent_activities': recent_activities,
        'top_users': top_users,
        'days': days,
        'start_date': start_date,
        'end_date': end_date,
    }
    
    return render(request, 'core/user_activity_dashboard.html', context)



