import json
import logging
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import JsonResponse
from django.utils import timezone
from datetime import timedelta
from .utils.parquet_utils import ParquetDataManager
from .models import UserActivity
from django.db import models
import pandas as pd
import io
from django.core.cache import cache

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
def npi_type_selection(request, state_code):
    """
    NPI Type Selection Page
    Allows users to choose between NPI-1 (Organization) or NPI-2 (Individual Provider) data
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
            return render(request, 'core/npi_type_selection.html', context)
        
        # Get available NPI types for this state
        available_npi_types = ParquetDataManager.get_available_npi_types(state_code)
        
        if not available_npi_types:
            context = {
                'has_data': False,
                'error_message': f'No data files found for {state_code}. Please try another state.',
                'state_code': state_code,
                'state_name': ParquetDataManager.get_state_name(state_code)
            }
            return render(request, 'core/npi_type_selection.html', context)
        
        context = {
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code),
            'available_npi_types': available_npi_types,
            'has_data': True
        }
        
    except Exception as e:
        logger.error(f"Error in npi_type_selection view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while loading the NPI selection page.',
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code) if 'state_code' in locals() else 'Unknown'
        }
    
    return render(request, 'core/npi_type_selection.html', context)


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
        
        # Get NPI type from request parameters
        npi_type = request.GET.get('npi_type')
        
        # Initialize data manager with state-specific file and NPI type
        try:
            data_manager = ParquetDataManager(state=state_code, npi_type=npi_type)
            if not data_manager.has_data:
                logger.error(f"Data file not found for {state_code}")
                context = {
                    'has_data': False,
                    'error_message': f'Sorry, {state_code} data is not available yet. Please try another state.',
                    'state_code': state_code,
                    'state_name': ParquetDataManager.get_state_name(state_code)
                }
                return render(request, 'core/commercial_rate_insights_state.html', context)
        except Exception as e:
            logger.error(f"Failed to initialize data manager for {state_code}: {str(e)}")
            context = {
                'has_data': False,
                'error_message': 'An error occurred while initializing the data manager.',
                'state_code': state_code,
                'state_name': ParquetDataManager.get_state_name(state_code)
            }
            return render(request, 'core/commercial_rate_insights_state.html', context)
        
        # Get active filters from request - simplified to core fields only
        active_filters = {
            'payer': request.GET.getlist('payer'),
            'procedure_set': request.GET.getlist('procedure_set'),
            'procedure_class': request.GET.getlist('procedure_class'),
            'procedure_group': request.GET.getlist('procedure_group'),
            'org_name': request.GET.getlist('org_name'),
            'tin_value': request.GET.getlist('tin_value'),
            'billing_code': request.GET.getlist('billing_code')
        }
        
        # Remove empty filters
        active_filters = {k: v for k, v in active_filters.items() if v}
        
        # Create cache key based on state and filters using improved method
        cache_key = ParquetDataManager.generate_cache_key(state_code, active_filters, npi_type)
        
        # Try to get cached data first
        cached_data = cache.get(cache_key)
        if cached_data:
            logger.info(f"Using cached data for {state_code}")
            context = cached_data
            # Add NPI type to cached context if not present
            if 'npi_type' not in context:
                context['npi_type'] = npi_type
            # Extract filters from cached context for logging
            filters = context.get('filters', {})
        else:
            # Get filtered options for each field based on current selections - simplified to core fields only
            filters = {
                'payers': data_manager.get_unique_values('payer', active_filters),
                'procedure_sets': data_manager.get_unique_values('procedure_set', active_filters),
                'procedure_classes': data_manager.get_unique_values('procedure_class', active_filters),
                'procedure_groups': data_manager.get_unique_values('procedure_group', active_filters),
                'organizations': data_manager.get_unique_values('org_name', active_filters),
                'tin_values': data_manager.get_unique_values('tin_value', active_filters),
                'billing_codes': data_manager.get_unique_values('billing_code', active_filters),
            }
            
            # Get aggregated statistics with filters
            stats = data_manager.get_aggregated_stats(active_filters)
            
            # Get base statistics for shared filters template
            base_stats = data_manager.get_base_statistics(active_filters)
            
            # Get sample records
            sample_records = data_manager.get_sample_records(active_filters, limit=10)
            
            context = {
                'filters': filters,
                'stats': stats,
                'base_stats': base_stats,
                'active_filters': active_filters,
                'sample_records': sample_records,
                'has_data': True,
                'state_code': state_code,
                'state_name': ParquetDataManager.get_state_name(state_code),
                'npi_type': npi_type
            }
            
            # Cache the data for 5 minutes
            cache.set(cache_key, context, 300)
            logger.info(f"Cached data for {state_code}")
        
        # Debug logging
        logger.info(f"State: {state_code}")
        logger.info(f"Active filters: {active_filters}")
        logger.info(f"Available organizations: {len(filters.get('organizations', []))}")
        logger.info(f"Available payers: {len(filters.get('payers', []))}")
            
    except Exception as e:
        logger.error(f"Error in commercial_rate_insights_state view: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Clear any corrupted cache entries
        try:
            cache_key = ParquetDataManager.generate_cache_key(state_code, {}, npi_type)
            cache.delete(cache_key)
            logger.info(f"Cleared corrupted cache for {state_code}")
        except:
            pass
        
        # Clean up connections if there's a connection issue
        try:
            ParquetDataManager.cleanup_connections()
            logger.info("Cleaned up connections due to error")
        except:
            pass
        
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
    Commercial Rate Insights Dashboard - redirects to GA state view by default
    """
    # Redirect to GA state view since we have GA data available
    return redirect('commercial_rate_insights_state', state_code='GA')


@login_required
def commercial_rate_insights_compare(request, state_code):
    """
    State-specific Commercial Rate Comparison Dashboard
    Allows side-by-side comparison of organizations and payers
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
        
        # Get NPI type from request parameters
        npi_type = request.GET.get('npi_type')
        
        # Initialize data manager with state-specific file and NPI type
        data_manager = ParquetDataManager(state=state_code, npi_type=npi_type)
        
        # Get active filters from request - simplified to core fields only
        active_filters = {
            'payer': request.GET.getlist('payer'),
            'procedure_set': request.GET.getlist('procedure_set'),
            'procedure_class': request.GET.getlist('procedure_class'),
            'procedure_group': request.GET.getlist('procedure_group'),
            'org_name': request.GET.getlist('org_name'),
            'tin_value': request.GET.getlist('tin_value'),
            'billing_code': request.GET.getlist('billing_code')
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
            'primary_taxonomy_descs': data_manager.get_unique_values('primary_taxonomy_desc', active_filters)
        }
        
        # Get base statistics with current filters
        base_stats = data_manager.get_base_statistics(active_filters)
        
        # Get comparison selections from request
        compare_orgs_selected = request.GET.getlist('compare_orgs')
        compare_payers_selected = request.GET.getlist('compare_payers')
        
        # Get comparison data if selections are made
        comparison_data = None
        if compare_orgs_selected or compare_payers_selected:
            comparison_data = data_manager.get_comparison_data(active_filters, compare_orgs_selected, compare_payers_selected)
        
        context = {
            'has_data': True,
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code),
            'filters': filters,
            'active_filters': active_filters,
            'base_stats': base_stats,
            'comparison_data': comparison_data,
            'compare_orgs_selected': compare_orgs_selected,
            'compare_payers_selected': compare_payers_selected
        }
        
    except Exception as e:
        logger.error(f"Error in commercial_rate_insights_compare view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': f'An error occurred while loading comparison data: {str(e)}',
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code) if 'state_code' in locals() else 'Unknown'
        }
    
    return render(request, 'core/commercial_rate_insights_compare.html', context)


@login_required
def custom_network_analysis(request, state_code):
    """
    Custom Network Analysis Dashboard
    Allows users to upload their own TIN list for personalized network performance analysis
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
            return render(request, 'core/custom_network_analysis.html', context)
        
        # Get NPI type from request parameters
        npi_type = request.GET.get('npi_type')
        
        # Initialize data manager with state-specific file and NPI type
        data_manager = ParquetDataManager(state=state_code, npi_type=npi_type)
        
        # Handle file upload
        custom_tins = []
        upload_error = None
        upload_success = False
        
        if request.method == 'POST' and request.FILES.get('tin_file'):
            try:
                uploaded_file = request.FILES['tin_file']
                file_content = uploaded_file.read().decode('utf-8')
                
                # Determine file type and parse accordingly
                if uploaded_file.name.endswith('.csv'):
                    # Parse CSV
                    df = pd.read_csv(io.StringIO(file_content))
                    # Look for TIN column (case insensitive)
                    tin_columns = [col for col in df.columns if 'tin' in col.lower() or 'tax' in col.lower()]
                    if tin_columns:
                        custom_tins = df[tin_columns[0]].dropna().astype(str).tolist()
                    else:
                        # Assume first column contains TINs
                        custom_tins = df.iloc[:, 0].dropna().astype(str).tolist()
                        
                elif uploaded_file.name.endswith(('.xlsx', '.xls')):
                    # Parse Excel
                    df = pd.read_excel(io.BytesIO(uploaded_file.read()))
                    # Look for TIN column (case insensitive)
                    tin_columns = [col for col in df.columns if 'tin' in col.lower() or 'tax' in col.lower()]
                    if tin_columns:
                        custom_tins = df[tin_columns[0]].dropna().astype(str).tolist()
                    else:
                        # Assume first column contains TINs
                        custom_tins = df.iloc[:, 0].dropna().astype(str).tolist()
                        
                elif uploaded_file.name.endswith('.txt'):
                    # Parse TXT (one TIN per line)
                    custom_tins = [line.strip() for line in file_content.split('\n') if line.strip()]
                    
                else:
                    upload_error = "Unsupported file format. Please use CSV, Excel, or TXT files."
                
                # Clean and validate TINs
                if custom_tins:
                    # Remove any non-numeric characters and validate
                    cleaned_tins = []
                    for tin in custom_tins:
                        # Extract numeric characters only
                        clean_tin = ''.join(filter(str.isdigit, str(tin)))
                        if clean_tin and len(clean_tin) >= 9:  # Basic TIN validation
                            cleaned_tins.append(clean_tin)
                    
                    custom_tins = cleaned_tins
                    
                    if not custom_tins:
                        upload_error = "No valid TIN numbers found in the uploaded file."
                    else:
                        upload_success = True
                        # Store in session for persistence
                        request.session[f'custom_tins_{state_code}'] = custom_tins
                        
            except Exception as e:
                upload_error = f"Error processing file: {str(e)}"
        
        # Get custom TINs from session if no new upload
        if not custom_tins and not upload_error:
            custom_tins = request.session.get(f'custom_tins_{state_code}', [])
            if custom_tins:
                upload_success = True
        
        # Get active filters (always include custom TINs if available)
        active_filters = {
            'payer': request.GET.getlist('payer'),
            'org_name': request.GET.getlist('org_name'),
            'procedure_set': request.GET.getlist('procedure_set'),
            'procedure_class': request.GET.getlist('procedure_class'),
            'procedure_group': request.GET.getlist('procedure_group'),
            'cbsa': request.GET.getlist('cbsa'),
            'billing_code': request.GET.getlist('billing_code'),
            'tin_value': custom_tins + request.GET.getlist('tin_value'),  # Combine custom TINs with manual selections
            'primary_taxonomy_code': request.GET.getlist('primary_taxonomy_code'),
            'primary_taxonomy_desc': request.GET.getlist('primary_taxonomy_desc')
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
            'primary_taxonomy_descs': data_manager.get_unique_values('primary_taxonomy_desc', active_filters)
        }
        
        # Get base statistics with current filters
        base_stats = data_manager.get_base_statistics(active_filters)
        
        # Get comparison data
        comparison_data = data_manager.get_comparison_data(active_filters)
        
        # Get network performance metrics for custom TINs
        network_metrics = None
        if custom_tins:
            network_metrics = data_manager.get_network_performance_metrics(custom_tins, active_filters)
        
        context = {
            'has_data': True,
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code),
            'filters': filters,
            'active_filters': active_filters,
            'base_stats': base_stats,
            'comparison_data': comparison_data,
            'custom_tins': custom_tins,
            'upload_error': upload_error,
            'upload_success': upload_success,
            'network_metrics': network_metrics
        }
        
    except Exception as e:
        logger.error(f"Error in custom_network_analysis view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': f'An error occurred while loading custom network analysis: {str(e)}',
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code) if 'state_code' in locals() else 'Unknown'
        }
    
    return render(request, 'core/custom_network_analysis.html', context)


@login_required
def commercial_rate_insights_overview(request, state_code):
    """
    State Overview Page - Shows distinct payers, organizations, and procedure sets
    Allows users to set prefilters before going to the detailed insights page
    """
    try:
        # Validate state code
        state_code = state_code.upper()
        available_states = ParquetDataManager.get_available_states()
        
        # Debug logging for request parameters
        logger.info(f"Overview request - State: {state_code}")
        logger.info(f"Request GET params: {dict(request.GET)}")
        logger.info(f"Available states: {available_states}")
        
        if state_code not in available_states or available_states[state_code] != 'available':
            context = {
                'has_data': False,
                'error_message': f'Sorry, {state_code} data is not available yet. Please try another state.',
                'state_code': state_code,
                'state_name': ParquetDataManager.get_state_name(state_code)
            }
            return render(request, 'core/commercial_rate_insights_overview.html', context)
        
        # Get NPI type from request parameters
        npi_type = request.GET.get('npi_type')
        
        # Initialize data manager with state-specific file and NPI type
        data_manager = ParquetDataManager(state=state_code, npi_type=npi_type)
        logger.info(f"Data manager initialized with file: {data_manager.file_path}")
        logger.info(f"Data manager has_data: {data_manager.has_data}")
        
        # Get overview statistics without any filters (full dataset)
        overview_stats = data_manager.get_overview_statistics()
        
        # Get active prefilters from request
        active_prefilters = {
            'payer': request.GET.getlist('payer'),
            'org_name': request.GET.getlist('org_name'),
            'procedure_set': request.GET.getlist('procedure_set'),
            'primary_taxonomy_desc': request.GET.getlist('primary_taxonomy_desc'),
            'tin_value': request.GET.getlist('tin_value'),
        }
        
        # Remove empty prefilters
        active_prefilters = {k: v for k, v in active_prefilters.items() if v}
        
        logger.info(f"Active prefilters after filtering: {active_prefilters}")
        logger.info(f"Request GET params: {dict(request.GET)}")
        logger.info(f"Payer list: {active_prefilters.get('payer', [])}")
        logger.info(f"Org list: {active_prefilters.get('org_name', [])}")
        
        # Get filtered options for each field based on current prefilter selections
        # This helps users see what's available after applying prefilters
        filtered_options = {
            'payers': data_manager.get_unique_values('payer', active_prefilters),
            'organizations': data_manager.get_unique_values('org_name', active_prefilters),
            'procedure_sets': data_manager.get_unique_values('procedure_set', active_prefilters),
            'primary_taxonomy_descs': data_manager.get_unique_values('primary_taxonomy_desc', active_prefilters),
            'tin_values': data_manager.get_unique_values('tin_value', active_prefilters),
        }
        
        logger.info(f"Filtered options - Payers: {len(filtered_options['payers'])}, Orgs: {len(filtered_options['organizations'])}, Proc Sets: {len(filtered_options['procedure_sets'])}")
        
        # Get sample records with prefilters applied (limited for performance)
        sample_records = data_manager.get_sample_records(active_prefilters, limit=5)
        
        context = {
            'overview_stats': overview_stats,
            'filtered_options': filtered_options,
            'active_prefilters': active_prefilters,
            'sample_records': sample_records,
            'has_data': True,
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code),
            'npi_type': npi_type
        }
        
        # Debug logging
        logger.info(f"Overview - State: {state_code}")
        logger.info(f"Active prefilters: {active_prefilters}")
        logger.info(f"Overview stats: {overview_stats}")
        logger.info(f"Sample records count: {len(sample_records)}")
            
    except Exception as e:
        logger.error(f"Error in commercial_rate_insights_overview view: {str(e)}")
        logger.error(f"Exception details: {e.__class__.__name__}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while processing the data.',
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code)
        }
    
    return render(request, 'core/commercial_rate_insights_overview.html', context)


@login_required
def commercial_rate_insights_overview_simple(request, state_code):
    """
    Simplified State-specific Commercial Rate Data Overview
    Shows dataset statistics without prefilters, with direct link to insights
    """
    try:
        # Validate state code
        state_code = state_code.upper()
        logger.info(f"Loading overview for state: {state_code}")
        
        available_states = ParquetDataManager.get_available_states()
        logger.info(f"Available states check - GA status: {available_states.get('GA', 'not found')}")
        
        if state_code not in available_states or available_states[state_code] != 'available':
            logger.warning(f"State {state_code} not available. Status: {available_states.get(state_code, 'not found')}")
            context = {
                'has_data': False,
                'error_message': f'Sorry, {state_code} data is not available yet. Please try another state.',
                'state_code': state_code,
                'state_name': ParquetDataManager.get_state_name(state_code)
            }
            return render(request, 'core/commercial_rate_insights_overview_simple.html', context)
        
        # Get NPI type from request parameters
        npi_type = request.GET.get('npi_type')
        
        # Initialize data manager with state-specific file and NPI type
        logger.info(f"Initializing data manager for {state_code} with NPI type: {npi_type}")
        data_manager = ParquetDataManager(state=state_code, npi_type=npi_type)
        logger.info(f"Data manager has_data: {data_manager.has_data}")
        
        # Get overview statistics without any prefilters
        logger.info("Getting overview statistics...")
        overview_stats = data_manager.get_overview_statistics()
        logger.info(f"Overview stats retrieved: {len(overview_stats)} keys")
        
        # Get sample records without any prefilters
        logger.info("Getting sample records...")
        sample_records = data_manager.get_sample_records({}, limit=5)
        logger.info(f"Sample records retrieved: {len(sample_records)} records")
        
        context = {
            'has_data': True,
            'state_code': state_code,
            'state_name': ParquetDataManager.get_state_name(state_code),
            'overview_stats': overview_stats,
            'sample_records': sample_records,
            'npi_type': npi_type
        }
        
        logger.info("Context prepared successfully")
        
    except Exception as e:
        logger.error(f"Error in commercial_rate_insights_overview_simple view: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        context = {
            'has_data': False,
            'error_message': f'An error occurred while loading overview data: {str(e)}',
            'state_code': state_code if 'state_code' in locals() else 'Unknown',
            'state_name': ParquetDataManager.get_state_name(state_code) if 'state_code' in locals() else 'Unknown'
        }
    
    return render(request, 'core/commercial_rate_insights_overview_simple.html', context)


@login_required
def api_filter_options(request, state_code):
    """
    API endpoint to get filter options for a state
    Used for preloading data
    """
    try:
        # Validate state code
        state_code = state_code.upper()
        available_states = ParquetDataManager.get_available_states()
        
        if state_code not in available_states or available_states[state_code] != 'available':
            return JsonResponse({'error': 'State not available'}, status=404)
        
        # Get NPI type from request parameters
        npi_type = request.GET.get('npi_type')
        
        # Initialize data manager with state-specific file and NPI type
        data_manager = ParquetDataManager(state=state_code, npi_type=npi_type)
        
        # Get filter options without any active filters
        filters = {
            'payers': data_manager.get_unique_values('payer', {}),
            'organizations': data_manager.get_unique_values('org_name', {}),
            'procedure_sets': data_manager.get_unique_values('procedure_set', {}),
            'procedure_classes': data_manager.get_unique_values('procedure_class', {}),
            'procedure_groups': data_manager.get_unique_values('procedure_group', {}),
            'cbsa_regions': data_manager.get_unique_values('cbsa', {}),
            'billing_codes': data_manager.get_unique_values('billing_code', {}),
            'tin_values': data_manager.get_unique_values('tin_value', {}),
            'primary_taxonomy_codes': data_manager.get_unique_values('primary_taxonomy_code', {}),
            'primary_taxonomy_descs': data_manager.get_unique_values('primary_taxonomy_desc', {})
        }
        
        return JsonResponse(filters)
        
    except Exception as e:
        logger.error(f"Error in api_filter_options: {str(e)}")
        return JsonResponse({'error': 'Internal server error'}, status=500)


@login_required
def api_sample_data(request, state_code):
    """
    API endpoint to get sample data for charts
    Used for preloading data
    """
    try:
        # Validate state code
        state_code = state_code.upper()
        available_states = ParquetDataManager.get_available_states()
        
        if state_code not in available_states or available_states[state_code] != 'available':
            return JsonResponse({'error': 'State not available'}, status=404)
        
        # Get NPI type from request parameters
        npi_type = request.GET.get('npi_type')
        
        # Initialize data manager with state-specific file and NPI type
        data_manager = ParquetDataManager(state=state_code, npi_type=npi_type)
        
        # Get sample data for charts
        sample_records = data_manager.get_sample_records({}, limit=50)
        
        # Get some aggregated stats for charts
        base_stats = data_manager.get_base_statistics({})
        
        sample_data = {
            'sample_records': sample_records,
            'base_stats': base_stats
        }
        
        return JsonResponse(sample_data)
        
    except Exception as e:
        logger.error(f"Error in api_sample_data: {str(e)}")
        return JsonResponse({'error': 'Internal server error'}, status=500)


@login_required
@user_passes_test(lambda u: u.is_staff)
def user_activity_dashboard(request):
    """Dashboard for viewing user activity data (staff only)."""
    
    # Get date range from request or default to last 7 days
    days = int(request.GET.get('days', 7))
    end_date = timezone.now()
    start_date = end_date - timedelta(days=days)
    
    # Cache key based on date range
    cache_key = f"user_activity_dashboard_{start_date.isoformat()}_{end_date.isoformat()}"
    context = cache.get(cache_key)

    if not context:
        filter_params = {
            'timestamp__gte': start_date,
            'timestamp__lte': end_date,
        }

        # Get summary statistics
        total_activities = UserActivity.objects.filter(**filter_params).count()
        unique_users = (
            UserActivity.objects.filter(**filter_params)
            .values('user')
            .distinct()
            .count()
        )

        # Action breakdown
        action_counts = list(
            UserActivity.objects.filter(**filter_params)
            .values('action')
            .annotate(count=models.Count('id'))
            .order_by('-count')
        )

        # Recent activities (last 50) with only required fields
        recent_activities = list(
            UserActivity.objects.filter(**filter_params)
            .select_related('user')
            .only('timestamp', 'action', 'page_url', 'page_title', 'user__username')
            .order_by('-timestamp')[:50]
        )

        # Top active users
        top_users = list(
            UserActivity.objects.filter(**filter_params)
            .values('user__username')
            .annotate(count=models.Count('id'))
            .order_by('-count')[:10]
        )

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

        cache.set(cache_key, context, 300)

    return render(request, 'core/user_activity_dashboard.html', context)



