import json
import logging
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import JsonResponse, HttpResponse
from django.utils import timezone
from django.conf import settings
from datetime import timedelta
from .utils.parquet_utils import ParquetDataManager
from .utils.partition_navigator import PartitionNavigator
from .models import UserActivity
from django.db import models
import pandas as pd
import io
from django.core.cache import cache

logger = logging.getLogger(__name__)

def _get_payer_breakdown_analysis(df, selected_payer_slugs):
    """Get detailed breakdown analysis for selected payer slugs"""
    if df is None or df.empty or not selected_payer_slugs:
        return {}
    
    # Ensure we have the required columns
    if 'payer_slug' not in df.columns:
        return {}
    
    # Filter to only selected payer slugs
    filtered_df = df[df['payer_slug'].isin(selected_payer_slugs)]
    
    if filtered_df.empty:
        return {}
    
    breakdown = {}
    
    # Group by payer_slug and calculate comprehensive statistics
    payer_stats = filtered_df.groupby('payer_slug').agg({
        'negotiated_rate': ['count', 'mean', 'median', 'std', 'min', 'max'],
        'medicare_professional_rate': ['count', 'mean', 'median'],
        'medicare_asc_stateavg': ['count', 'mean', 'median'],
        'medicare_opps_stateavg': ['count', 'mean', 'median'],
        'negotiated_rate_pct_of_medicare_professional': ['count', 'mean', 'median'],
        'negotiated_rate_pct_of_medicare_asc': ['count', 'mean', 'median'],
        'negotiated_rate_pct_of_medicare_opps': ['count', 'mean', 'median'],
        'organization_name': 'nunique',
        'primary_taxonomy_desc': 'nunique',
        'stat_area_name': 'nunique'
    }).round(2)
    
    # Flatten column names
    payer_stats.columns = ['_'.join(col).strip() for col in payer_stats.columns]
    payer_stats = payer_stats.reset_index()
    
    # Convert to list of dictionaries with proper formatting
    for _, row in payer_stats.iterrows():
        payer_slug = row['payer_slug']
        breakdown[payer_slug] = {
            'payer_slug': payer_slug,
            'record_count': int(row['negotiated_rate_count']),
            'avg_negotiated_rate': row['negotiated_rate_mean'],
            'median_negotiated_rate': row['negotiated_rate_median'],
            'std_negotiated_rate': row['negotiated_rate_std'],
            'min_negotiated_rate': row['negotiated_rate_min'],
            'max_negotiated_rate': row['negotiated_rate_max'],
            'unique_organizations': int(row['organization_name_nunique']),
            'unique_taxonomies': int(row['primary_taxonomy_desc_nunique']),
            'unique_stat_areas': int(row['stat_area_name_nunique']),
            'medicare_professional': {
                'count': int(row['medicare_professional_rate_count']),
                'avg_rate': row['medicare_professional_rate_mean'],
                'median_rate': row['medicare_professional_rate_median']
            },
            'medicare_asc': {
                'count': int(row['medicare_asc_stateavg_count']),
                'avg_rate': row['medicare_asc_stateavg_mean'],
                'median_rate': row['medicare_asc_stateavg_median']
            },
            'medicare_opps': {
                'count': int(row['medicare_opps_stateavg_count']),
                'avg_rate': row['medicare_opps_stateavg_mean'],
                'median_rate': row['medicare_opps_stateavg_median']
            },
            'percentage_analysis': {
                'prof_count': int(row['negotiated_rate_pct_of_medicare_professional_count']),
                'prof_avg_pct': row['negotiated_rate_pct_of_medicare_professional_mean'],
                'prof_median_pct': row['negotiated_rate_pct_of_medicare_professional_median'],
                'asc_count': int(row['negotiated_rate_pct_of_medicare_asc_count']),
                'asc_avg_pct': row['negotiated_rate_pct_of_medicare_asc_mean'],
                'asc_median_pct': row['negotiated_rate_pct_of_medicare_asc_median'],
                'opps_count': int(row['negotiated_rate_pct_of_medicare_opps_count']),
                'opps_avg_pct': row['negotiated_rate_pct_of_medicare_opps_mean'],
                'opps_median_pct': row['negotiated_rate_pct_of_medicare_opps_median']
            }
        }
    
    return breakdown


@login_required
def home(request):
    """Home page with dashboard overview."""
    return render(request, 'core/home.html')


@login_required
def commercial_rate_insights_tile(request):
    """
    Tile-based Commercial Rate Insights Landing Page
    Implements hierarchical filtering system for partition discovery
    """
    try:
        # Initialize partition navigator
        navigator = PartitionNavigator(
            db_path='core/data/partition_navigation.db',
            s3_bucket='partitioned-data'  # Update with your actual bucket
        )
        
        # Get filter options
        filter_options = navigator.get_filter_options()
        
        # Debug logging
        logger.info(f"Filter options loaded: {[(k, len(v)) for k, v in filter_options.items()]}")
        
        # Get current filters from request
        current_filters = {
            'payer_slug': request.GET.getlist('payer_slug'),
            'state': request.GET.get('state'),
            'billing_class': request.GET.get('billing_class'),
            'procedure_set': request.GET.get('procedure_set'),
            'taxonomy_code': request.GET.get('taxonomy_code'),
            'taxonomy_desc': request.GET.get('taxonomy_desc'),
            'stat_area_name': request.GET.get('stat_area_name'),
            'year': request.GET.get('year'),
            'month': request.GET.get('month')
        }
        
        # Remove empty filters
        current_filters = {k: v for k, v in current_filters.items() if v}
        
        # Get partition summary
        partition_summary = navigator.get_partition_summary(current_filters)
        
        context = {
            'filter_options': filter_options,
            'current_filters': current_filters,
            'partition_summary': partition_summary,
            'has_required_filters': all(current_filters.get(f) for f in navigator.required_filters)
        }
        
    except Exception as e:
        logger.error(f"Error in commercial_rate_insights_tile view: {str(e)}")
        context = {
            'filter_options': {},
            'current_filters': {},
            'partition_summary': {'partition_count': 0, 'total_size_mb': 0, 'total_estimated_records': 0},
            'has_required_filters': False,
            'error_message': 'An error occurred while loading the partition navigator.'
        }
    
    return render(request, 'core/commercial_rate_insights_tile.html', context)


@login_required
def debug_filter_options(request):
    """
    Debug view to check filter options loading
    """
    try:
        navigator = PartitionNavigator(
            db_path='core/data/partition_navigation.db',
            s3_bucket='partitioned-data'
        )
        
        filter_options = navigator.get_filter_options()
        
        debug_info = {}
        for category, options in filter_options.items():
            debug_info[category] = {
                'count': len(options),
                'sample': options[:5] if options else [],
                'types': list(set(type(opt).__name__ for opt in options[:5])) if options else []
            }
        
        return JsonResponse(debug_info, json_dumps_params={'indent': 2})
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@login_required
def debug_s3_connection(request):
    """
    Debug view to test S3 connection and partition access
    """
    debug_info = {
        'environment_check': {},
        's3_connection': {},
        'database_check': {},
        'partition_access': {},
        'errors': []
    }
    
    try:
        # Check environment variables
        debug_info['environment_check'] = {
            'aws_access_key_id': 'Set' if getattr(settings, 'AWS_ACCESS_KEY_ID', None) else 'Missing',
            'aws_secret_access_key': 'Set' if getattr(settings, 'AWS_SECRET_ACCESS_KEY', None) else 'Missing',
            'aws_default_region': getattr(settings, 'AWS_DEFAULT_REGION', 'Not set'),
            'aws_s3_bucket': getattr(settings, 'AWS_S3_BUCKET', 'Not set'),
        }
        
        # Test S3 connection
        navigator = PartitionNavigator(db_path='core/data/partition_navigation.db')
        s3_client = navigator.connect_s3()
        
        if s3_client:
            try:
                # Test bucket access
                bucket_name = navigator.s3_bucket
                s3_client.head_bucket(Bucket=bucket_name)
                debug_info['s3_connection']['bucket_access'] = f'✅ Bucket {bucket_name} is accessible'
                
                # List some objects
                response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=5)
                if 'Contents' in response:
                    debug_info['s3_connection']['sample_objects'] = [
                        obj['Key'] for obj in response['Contents']
                    ]
                else:
                    debug_info['s3_connection']['sample_objects'] = 'No objects found'
                    
            except Exception as e:
                debug_info['s3_connection']['bucket_access'] = f'❌ Bucket access failed: {str(e)}'
                debug_info['errors'].append(f'S3 bucket access error: {str(e)}')
        else:
            debug_info['s3_connection']['bucket_access'] = '❌ S3 client creation failed'
            debug_info['errors'].append('S3 client creation failed')
        
        # Check database
        try:
            conn = navigator.connect_db()
            partitions_count = conn.execute("SELECT COUNT(*) FROM partitions").fetchone()[0]
            debug_info['database_check']['partitions_count'] = partitions_count
            
            # Get sample partitions
            sample_partitions = conn.execute(
                "SELECT s3_bucket, s3_key FROM partitions LIMIT 3"
            ).fetchall()
            debug_info['database_check']['sample_partitions'] = [
                {'bucket': row['s3_bucket'], 'key': row['s3_key']} 
                for row in sample_partitions
            ]
            
        except Exception as e:
            debug_info['database_check']['error'] = str(e)
            debug_info['errors'].append(f'Database error: {str(e)}')
        
        # Test partition access
        if s3_client and 'sample_partitions' in debug_info['database_check']:
            partition_tests = []
            for partition in debug_info['database_check']['sample_partitions']:
                try:
                    response = s3_client.head_object(
                        Bucket=partition['bucket'], 
                        Key=partition['key']
                    )
                    size_mb = response['ContentLength'] / 1024 / 1024
                    partition_tests.append({
                        'path': f"s3://{partition['bucket']}/{partition['key']}",
                        'status': '✅ Accessible',
                        'size_mb': round(size_mb, 2)
                    })
                except Exception as e:
                    partition_tests.append({
                        'path': f"s3://{partition['bucket']}/{partition['key']}",
                        'status': f'❌ Error: {str(e)}',
                        'size_mb': None
                    })
            
            debug_info['partition_access'] = partition_tests
        
    except Exception as e:
        debug_info['errors'].append(f'General error: {str(e)}')
        logger.error(f"Debug S3 connection error: {str(e)}")
    
    return JsonResponse(debug_info, json_dumps_params={'indent': 2})


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
            'primary_taxonomy_code': request.GET.getlist('primary_taxonomy_code'),
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
            'primary_taxonomy_codes': data_manager.get_unique_values('primary_taxonomy_code', active_prefilters),
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
def dataset_review_map(request):
    """
    Map view for dataset review showing geographic distribution of rates
    """
    try:
        # Initialize partition navigator
        navigator = PartitionNavigator(
            db_path='core/data/partition_navigation.db'
        )
        
        # Get all filters from request
        all_filters = {
            # Primary required filters (single values)
            'payer_slug': request.GET.get('payer_slug'),
            'state': request.GET.get('state'),
            'billing_class': request.GET.get('billing_class'),
            # Partition-level optional filter
            'procedure_set': request.GET.get('procedure_set'),
            # Additional analysis filters (may be multi-valued)
            'taxonomy_code': request.GET.getlist('taxonomy_code') or request.GET.get('taxonomy_code'),
            'taxonomy_desc': request.GET.getlist('taxonomy_desc') or request.GET.get('taxonomy_desc'),
            'stat_area_name': request.GET.getlist('stat_area_name') or request.GET.get('stat_area_name'),
            'county_name': request.GET.getlist('county_name') or request.GET.get('county_name'),
            'proc_class': request.GET.getlist('proc_class') or request.GET.get('proc_class'),
            'proc_group': request.GET.getlist('proc_group') or request.GET.get('proc_group'),
            'code': request.GET.getlist('code') or request.GET.get('code'),
            # Time filters
            'year': request.GET.get('year'),
            'month': request.GET.get('month')
        }
        
        # Remove empty filters
        all_filters = {k: v for k, v in all_filters.items() if v and (not isinstance(v, list) or len(v) > 0)}
        
        # Separate partition-level filters from data-level filters
        partition_filters = {k: v for k, v in all_filters.items() if k in ['payer_slug', 'state', 'billing_class', 'procedure_set', 'taxonomy_code', 'taxonomy_desc', 'stat_area_name', 'year', 'month']}
        data_filters = {k: v for k, v in all_filters.items() if k in ['proc_class', 'proc_group', 'code', 'county_name']}
        
        # Validate required filters
        if not all(partition_filters.get(f) for f in navigator.required_filters):
            return render(request, 'core/error.html', {
                'error_message': 'Please select all required filters: Payer, State, and Billing Class'
            })
        
        # Search for partitions using only relevant filters
        partitions_df = navigator.search_partitions({k: v for k, v in partition_filters.items() if v})
        
        if partitions_df.empty:
            return render(request, 'core/error.html', {
                'error_message': 'No partitions found matching the selected criteria'
            })
        
        # Get S3 paths for combination
        s3_paths = [f"s3://{row['s3_bucket']}/{row['s3_key']}" for _, row in partitions_df.iterrows()]
        
        # Define required columns (same as dataset_review)
        required_columns = [
            'state', 'year_month', 'payer_slug', 'billing_class', 'code_type', 'code',
            'negotiated_type', 'negotiation_arrangement', 'negotiated_rate',
            'reporting_entity_name', 'code_description', 'code_name', 'proc_set', 'proc_class',
            'proc_group', 'version', 'npi', 'organization_name', 'primary_taxonomy_code',
            'credential', 'enumeration_date', 'primary_taxonomy_state', 'first_name',
            'primary_taxonomy_desc', 'last_name', 'last_updated', 'sole_proprietor',
            'enumeration_type', 'primary_taxonomy_license', 'tin_type', 'tin_value',
            'state_geo', 'latitude', 'longitude', 'county_name', 'county_fips',
            'stat_area_name', 'stat_area_code', 'matched_address'
        ]
        
        # Get analysis parameters
        max_rows = int(request.GET.get('max_rows', 10000))          # Smaller limit for map view
        max_partitions = int(request.GET.get('max_partitions', 200))  # Increased limit for map
        
        # Limit partitions for map view
        if len(s3_paths) > max_partitions:
            s3_paths = s3_paths[:max_partitions]
        
        # Combine partitions for analysis
        combined_df = navigator.combine_partitions_for_analysis(s3_paths, max_rows)
        
        if combined_df is None or combined_df.empty:
            return render(request, 'core/error.html', {
                'error_message': 'Failed to load data from S3 partitions for map view.'
            })
        
        # Apply data-level filters (filters that don't exist in partition metadata)
        logger.info(f"Applying data-level filters to {len(combined_df)} rows for map view")
        original_row_count = len(combined_df)
        
        # Filter by proc_class
        if data_filters.get('proc_class'):
            if isinstance(data_filters['proc_class'], list):
                combined_df = combined_df[combined_df['proc_class'].isin(data_filters['proc_class'])]
            else:
                combined_df = combined_df[combined_df['proc_class'] == data_filters['proc_class']]
            logger.info(f"After proc_class filter: {len(combined_df)} rows (removed {original_row_count - len(combined_df)})")
            original_row_count = len(combined_df)
        
        # Filter by proc_group
        if data_filters.get('proc_group'):
            if isinstance(data_filters['proc_group'], list):
                combined_df = combined_df[combined_df['proc_group'].isin(data_filters['proc_group'])]
            else:
                combined_df = combined_df[combined_df['proc_group'] == data_filters['proc_group']]
            logger.info(f"After proc_group filter: {len(combined_df)} rows (removed {original_row_count - len(combined_df)})")
            original_row_count = len(combined_df)
        
        # Filter by code
        if data_filters.get('code'):
            if isinstance(data_filters['code'], list):
                combined_df = combined_df[combined_df['code'].isin(data_filters['code'])]
            else:
                combined_df = combined_df[combined_df['code'] == data_filters['code']]
            logger.info(f"After code filter: {len(combined_df)} rows (removed {original_row_count - len(combined_df)})")
            original_row_count = len(combined_df)
        
        # Filter by county_name
        if data_filters.get('county_name'):
            if isinstance(data_filters['county_name'], list):
                combined_df = combined_df[combined_df['county_name'].isin(data_filters['county_name'])]
            else:
                combined_df = combined_df[combined_df['county_name'] == data_filters['county_name']]
            logger.info(f"After county_name filter: {len(combined_df)} rows (removed {original_row_count - len(combined_df)})")
            original_row_count = len(combined_df)
        
        # Check if any data remains after filtering
        if combined_df.empty:
            return render(request, 'core/error.html', {
                'error_message': 'No data available with the selected filters for map view. Please try different filter combinations.'
            })
        
        # Get sample data for map (all data with coordinates)
        # Clean the data to handle None/NaN values properly for JSON serialization
        if not combined_df.empty:
            map_df = combined_df.head(5000).copy()
            # Replace NaN/None values with empty strings or null
            map_df = map_df.fillna('')
            # Convert None values to empty strings for JSON serialization
            map_df = map_df.astype(str).replace('None', '')
            # Clean any problematic characters that could cause JS syntax errors
            for col in map_df.columns:
                if map_df[col].dtype == 'object':
                    map_df[col] = map_df[col].str.replace('&', '&amp;', regex=False)
            map_data = map_df.to_dict('records')
            
            # Debug logging
            logger.info(f"Map data prepared: {len(map_data)} records")
            if map_data:
                logger.info(f"Sample record keys: {list(map_data[0].keys())}")
                logger.info(f"Sample record: {map_data[0]}")
                
                # Check for coordinate columns
                coord_data = [item for item in map_data if item.get('latitude') and item.get('longitude')]
                logger.info(f"Records with coordinates: {len(coord_data)} out of {len(map_data)}")
                
                if coord_data:
                    logger.info(f"Sample coordinates: lat={coord_data[0].get('latitude')}, lng={coord_data[0].get('longitude')}")
                else:
                    logger.warning("No records found with valid latitude/longitude coordinates")
        else:
            map_data = []
            logger.warning("No data available for map view")
        
        # Prepare context
        import json
        from django.utils.safestring import mark_safe
        context = {
            'filters': all_filters,
            'partitions_df': partitions_df,
            'combined_df_info': {
                'shape': combined_df.shape,
                'columns': list(combined_df.columns)
            },
            'sample_data': mark_safe(json.dumps(map_data)) if map_data else '[]',
            'has_data': combined_df is not None and not combined_df.empty,
            'total_partitions': len(partitions_df),
            's3_paths_count': len(s3_paths)
        }
        
    except Exception as e:
        logger.error(f"Error in dataset_review_map view: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        context = {
            'filters': {},
            'partitions_df': pd.DataFrame(),
            'combined_df_info': {},
            'sample_data': [],
            'has_data': False,
            'error_message': 'An error occurred while loading the map view.',
            'total_partitions': 0,
            's3_paths_count': 0
        }
    
    return render(request, 'core/dataset_review_map.html', context)


@login_required
def dataset_review_loading(request):
    """
    Loading page for dataset review
    """
    return render(request, 'core/dataset_review_loading.html')


@login_required
def dataset_review(request):
    """
    Dataset Review Page - Combines S3 partitions into unified dataframe for analysis
    """
    import time
    start_time = time.time()
    
    try:
        # Initialize partition navigator
        navigator = PartitionNavigator(
            db_path='core/data/partition_navigation.db'
        )
        
        # Get all filters from request
        all_filters = {
            'payer_slug': request.GET.getlist('payer_slug'),
            'state': request.GET.get('state'),
            'billing_class': request.GET.get('billing_class'),
            'procedure_set': request.GET.get('procedure_set'),
            'taxonomy_code': request.GET.get('taxonomy_code'),
            # Additional analysis filters (may be multi-valued)
            'taxonomy_desc': request.GET.getlist('taxonomy_desc') or request.GET.get('taxonomy_desc'),
            'stat_area_name': request.GET.getlist('stat_area_name') or request.GET.get('stat_area_name'),
            'county_name': request.GET.getlist('county_name') or request.GET.get('county_name'),
            'proc_class': request.GET.getlist('proc_class') or request.GET.get('proc_class'),
            'proc_group': request.GET.getlist('proc_group') or request.GET.get('proc_group'),
            'code': request.GET.getlist('code') or request.GET.get('code'),
            'year': request.GET.get('year'),
            'month': request.GET.get('month')
        }
        
        # Remove empty filters
        all_filters = {k: v for k, v in all_filters.items() if v and (not isinstance(v, list) or len(v) > 0)}
        
        # Separate partition-level filters from data-level filters
        partition_filters = {k: v for k, v in all_filters.items() if k in ['payer_slug', 'state', 'billing_class', 'procedure_set', 'taxonomy_code', 'taxonomy_desc', 'stat_area_name', 'year', 'month']}
        data_filters = {k: v for k, v in all_filters.items() if k in ['proc_class', 'proc_group', 'code', 'county_name']}
        
        # Use partition_filters for partition search
        filters = partition_filters
        
        # Remove empty filters
        filters = {k: v for k, v in filters.items() if v and (not isinstance(v, list) or len(v) > 0)}
        
        # Validate required filters
        if not all(filters.get(f) for f in navigator.required_filters):
            return render(request, 'core/error.html', {
                'error_message': 'Please select all required filters: Payer, State, and Billing Class'
            })
        
        # Search for partitions
        partitions_df = navigator.search_partitions(filters)
        
        if partitions_df.empty:
            # Render the same dataset review page with a warning message, avoiding redirects
            from django.contrib import messages
            messages.warning(request, 'No data available with the selected filters. Please try different filter combinations.')
            context = {
                'filters': all_filters,
                'partitions_df': pd.DataFrame(),
                'combined_df_info': {},
                'analysis': {},
                'sample_data': [],
                'max_rows': int(request.GET.get('max_rows', 50000)),
                'analysis_type': request.GET.get('analysis_type', 'comprehensive'),
                'has_data': False,
                'total_partitions': 0,
                's3_paths_count': 0,
                # Provide empty option lists so the modal still works
                'available_taxonomy_descs': '[]',
                'available_proc_classes': '[]',
                'available_proc_groups': '[]',
                'available_codes': '[]',
                'available_county_names': '[]',
                'available_stat_area_names': '[]'
            }
            return render(request, 'core/dataset_review.html', context)
        
        # Get S3 paths for combination
        s3_paths = [f"s3://{row['s3_bucket']}/{row['s3_key']}" for _, row in partitions_df.iterrows()]
        
        # Get analysis parameters
        max_rows = int(request.GET.get('max_rows', 100000))  # Default to 100k rows
        max_partitions = int(request.GET.get('max_partitions', 500))  # Default to 500 partitions max
        analysis_type = request.GET.get('analysis_type', 'comprehensive')
        
        # Limit partitions to prevent timeouts
        if len(s3_paths) > max_partitions:
            logger.warning(f"Limiting partitions from {len(s3_paths)} to {max_partitions} to prevent timeout")
            s3_paths = s3_paths[:max_partitions]
        
        # Define the columns we actually need (from fact_cols_pull.txt)
        required_columns = [
            'state', 'year_month', 'payer_slug', 'billing_class', 'code_type', 'code',
            'negotiated_type', 'negotiation_arrangement', 'negotiated_rate',
            'reporting_entity_name', 'code_description', 'code_name', 'proc_set', 'proc_class',
            'proc_group', 'version', 'npi', 'organization_name', 'primary_taxonomy_code',
            'credential', 'enumeration_date', 'primary_taxonomy_state', 'first_name',
            'primary_taxonomy_desc', 'last_name', 'last_updated', 'sole_proprietor',
            'enumeration_type', 'primary_taxonomy_license', 'tin_type', 'tin_value',
            'state_geo', 'latitude', 'longitude', 'county_name', 'county_fips',
            'stat_area_name', 'stat_area_code', 'matched_address'
        ]
        
        # Combine partitions for analysis (only load required columns)
        logger.info(f"Starting to combine {len(s3_paths)} partitions (max_rows: {max_rows}) with {len(required_columns)} columns")
        combined_df = navigator.combine_partitions_for_analysis(s3_paths, max_rows)
        logger.info(f"Combination completed. DataFrame shape: {combined_df.shape if combined_df is not None else 'None'}")
        
        if combined_df is None or combined_df.empty:
            return render(request, 'core/error.html', {
                'error_message': 'Failed to load data from S3 partitions. Please check your AWS credentials and network connection.'
            })
        
        # Apply data-level filters (filters that don't exist in partition metadata)
        logger.info(f"Applying data-level filters to {len(combined_df)} rows")
        original_row_count = len(combined_df)
        
        # Filter by proc_class
        if data_filters.get('proc_class'):
            if isinstance(data_filters['proc_class'], list):
                combined_df = combined_df[combined_df['proc_class'].isin(data_filters['proc_class'])]
            else:
                combined_df = combined_df[combined_df['proc_class'] == data_filters['proc_class']]
            logger.info(f"After proc_class filter: {len(combined_df)} rows (removed {original_row_count - len(combined_df)})")
            original_row_count = len(combined_df)
        
        # Filter by proc_group
        if data_filters.get('proc_group'):
            if isinstance(data_filters['proc_group'], list):
                combined_df = combined_df[combined_df['proc_group'].isin(data_filters['proc_group'])]
            else:
                combined_df = combined_df[combined_df['proc_group'] == data_filters['proc_group']]
            logger.info(f"After proc_group filter: {len(combined_df)} rows (removed {original_row_count - len(combined_df)})")
            original_row_count = len(combined_df)
        
        # Filter by code
        if data_filters.get('code'):
            if isinstance(data_filters['code'], list):
                combined_df = combined_df[combined_df['code'].isin(data_filters['code'])]
            else:
                combined_df = combined_df[combined_df['code'] == data_filters['code']]
            logger.info(f"After code filter: {len(combined_df)} rows (removed {original_row_count - len(combined_df)})")
            original_row_count = len(combined_df)
        
        # Filter by county_name
        if data_filters.get('county_name'):
            if isinstance(data_filters['county_name'], list):
                combined_df = combined_df[combined_df['county_name'].isin(data_filters['county_name'])]
            else:
                combined_df = combined_df[combined_df['county_name'] == data_filters['county_name']]
            logger.info(f"After county_name filter: {len(combined_df)} rows (removed {original_row_count - len(combined_df)})")
            original_row_count = len(combined_df)
        
        # Check if any data remains after filtering
        if combined_df.empty:
            return render(request, 'core/error.html', {
                'error_message': 'No data available with the selected filters. Please try different filter combinations.'
            })
        
        # Perform simple analysis first
        logger.info("Starting simple analysis...")
        analysis = {
            'dataset_summary': {
                'total_rows': len(combined_df)
            },
            'basic_stats': {}
        }
        
        # DataFrame already contains only the required columns from the optimized loading
        
        # Get basic stats for key columns
        key_columns = ['state', 'payer_slug', 'billing_class', 'negotiated_rate', 'organization_name', 'primary_taxonomy_desc']
        for col in key_columns:
            if col in combined_df.columns:
                try:
                    if combined_df[col].dtype == 'object':
                        # For text columns, show unique values
                        unique_vals = combined_df[col].dropna().unique()[:10]  # First 10 unique values
                        analysis['basic_stats'][col] = {
                            'type': 'categorical',
                            'unique_count': len(combined_df[col].dropna().unique()),
                            'sample_values': list(unique_vals)
                        }
                    else:
                        # For numeric columns, show basic stats
                        analysis['basic_stats'][col] = {
                            'type': 'numeric',
                            'count': combined_df[col].count(),
                            'mean': round(combined_df[col].mean(), 2) if combined_df[col].dtype in ['int64', 'float64'] else None,
                            'min': combined_df[col].min() if combined_df[col].dtype in ['int64', 'float64'] else None,
                            'max': combined_df[col].max() if combined_df[col].dtype in ['int64', 'float64'] else None
                        }
                except Exception as e:
                    logger.warning(f"Could not analyze column {col}: {e}")
                    analysis['basic_stats'][col] = {'error': str(e)}
        
        logger.info("Simple analysis completed")
        
        # Add key metrics analysis
        analysis['key_metrics'] = {}
        
        # Add payer breakdown if multiple payers selected
        if filters.get('payer_slug') and len(filters['payer_slug']) > 1:
            analysis['payer_breakdown'] = _get_payer_breakdown_analysis(combined_df, filters['payer_slug'])
        
        key_metric_columns = [
            'stat_area_name', 'tin_value', 'county_name', 
            'primary_taxonomy_desc', 'enumeration_type', 'organization_name',
            'proc_class', 'proc_group', 'code'
        ]
        
        for col in key_metric_columns:
            if col in combined_df.columns:
                try:
                    # Get unique values and their counts
                    value_counts = combined_df[col].value_counts().head(20)  # Top 20 values
                    
                    # Calculate metrics for each value based on billing class
                    metrics_data = []
                    for value in value_counts.index:
                        if pd.notna(value):
                            subset = combined_df[combined_df[col] == value]
                            record_count = len(subset)
                            
                            # Base metrics
                            metric_item = {
                                'value': str(value),
                                'record_count': record_count
                            }
                            
                            # Calculate average negotiated rate
                            if 'negotiated_rate' in subset.columns:
                                avg_rate = subset['negotiated_rate'].mean()
                                metric_item['avg_negotiated_rate'] = round(avg_rate, 2) if pd.notna(avg_rate) else None
                            
                            # Add Medicare benchmark metrics based on billing class
                            if 'billing_class' in subset.columns:
                                # Check if all records have the same billing class
                                unique_billing_classes = subset['billing_class'].dropna().unique()
                                
                                if len(unique_billing_classes) == 1:
                                    billing_class = unique_billing_classes[0]
                                    
                                    if billing_class == 'professional':
                                        # Professional billing - use Medicare Professional rates
                                        if 'medicare_professional_rate' in subset.columns:
                                            prof_medicare = subset['medicare_professional_rate'].dropna()
                                            if len(prof_medicare) > 0:
                                                metric_item['avg_medicare_professional_rate'] = round(prof_medicare.mean(), 2)
                                        
                                        if 'negotiated_rate_pct_of_medicare_professional' in subset.columns:
                                            prof_pct = subset['negotiated_rate_pct_of_medicare_professional'].dropna()
                                            if len(prof_pct) > 0:
                                                metric_item['avg_negotiated_rate_pct_of_medicare_professional'] = round(prof_pct.mean(), 2)
                                    
                                    elif billing_class == 'institutional':
                                        # Institutional billing - use Medicare ASC and OPPS rates
                                        if 'medicare_asc_stateavg' in subset.columns:
                                            asc_medicare = subset['medicare_asc_stateavg'].dropna()
                                            if len(asc_medicare) > 0:
                                                metric_item['avg_medicare_asc_stateavg'] = round(asc_medicare.mean(), 2)
                                        
                                        if 'medicare_opps_stateavg' in subset.columns:
                                            opps_medicare = subset['medicare_opps_stateavg'].dropna()
                                            if len(opps_medicare) > 0:
                                                metric_item['avg_medicare_opps_stateavg'] = round(opps_medicare.mean(), 2)
                                        
                                        if 'negotiated_rate_pct_of_medicare_asc' in subset.columns:
                                            asc_pct = subset['negotiated_rate_pct_of_medicare_asc'].dropna()
                                            if len(asc_pct) > 0:
                                                metric_item['avg_negotiated_rate_pct_of_medicare_asc'] = round(asc_pct.mean(), 2)
                                        
                                        if 'negotiated_rate_pct_of_medicare_opps' in subset.columns:
                                            opps_pct = subset['negotiated_rate_pct_of_medicare_opps'].dropna()
                                            if len(opps_pct) > 0:
                                                metric_item['avg_negotiated_rate_pct_of_medicare_opps'] = round(opps_pct.mean(), 2)
                                    
                                    else:
                                        # Mixed or other billing classes - calculate both
                                        logger.info(f"Mixed billing classes for {col}={value}, calculating both professional and institutional metrics")
                                        
                                        # Professional metrics
                                        if 'medicare_professional_rate' in subset.columns:
                                            prof_medicare = subset['medicare_professional_rate'].dropna()
                                            if len(prof_medicare) > 0:
                                                metric_item['avg_medicare_professional_rate'] = round(prof_medicare.mean(), 2)
                                        
                                        if 'negotiated_rate_pct_of_medicare_professional' in subset.columns:
                                            prof_pct = subset['negotiated_rate_pct_of_medicare_professional'].dropna()
                                            if len(prof_pct) > 0:
                                                metric_item['avg_negotiated_rate_pct_of_medicare_professional'] = round(prof_pct.mean(), 2)
                                        
                                        # Institutional metrics
                                        if 'medicare_asc_stateavg' in subset.columns:
                                            asc_medicare = subset['medicare_asc_stateavg'].dropna()
                                            if len(asc_medicare) > 0:
                                                metric_item['avg_medicare_asc_stateavg'] = round(asc_medicare.mean(), 2)
                                        
                                        if 'medicare_opps_stateavg' in subset.columns:
                                            opps_medicare = subset['medicare_opps_stateavg'].dropna()
                                            if len(opps_medicare) > 0:
                                                metric_item['avg_medicare_opps_stateavg'] = round(opps_medicare.mean(), 2)
                                        
                                        if 'negotiated_rate_pct_of_medicare_asc' in subset.columns:
                                            asc_pct = subset['negotiated_rate_pct_of_medicare_asc'].dropna()
                                            if len(asc_pct) > 0:
                                                metric_item['avg_negotiated_rate_pct_of_medicare_asc'] = round(asc_pct.mean(), 2)
                                        
                                        if 'negotiated_rate_pct_of_medicare_opps' in subset.columns:
                                            opps_pct = subset['negotiated_rate_pct_of_medicare_opps'].dropna()
                                            if len(opps_pct) > 0:
                                                metric_item['avg_negotiated_rate_pct_of_medicare_opps'] = round(opps_pct.mean(), 2)
                                
                                else:
                                    # Mixed billing classes - calculate both
                                    logger.info(f"Mixed billing classes for {col}={value}, calculating both professional and institutional metrics")
                                    
                                    # Professional metrics
                                    if 'medicare_professional_rate' in subset.columns:
                                        prof_medicare = subset['medicare_professional_rate'].dropna()
                                        if len(prof_medicare) > 0:
                                            metric_item['avg_medicare_professional_rate'] = round(prof_medicare.mean(), 2)
                                    
                                    if 'negotiated_rate_pct_of_medicare_professional' in subset.columns:
                                        prof_pct = subset['negotiated_rate_pct_of_medicare_professional'].dropna()
                                        if len(prof_pct) > 0:
                                            metric_item['avg_negotiated_rate_pct_of_medicare_professional'] = round(prof_pct.mean(), 2)
                                    
                                    # Institutional metrics
                                    if 'medicare_asc_stateavg' in subset.columns:
                                        asc_medicare = subset['medicare_asc_stateavg'].dropna()
                                        if len(asc_medicare) > 0:
                                            metric_item['avg_medicare_asc_stateavg'] = round(asc_medicare.mean(), 2)
                                    
                                    if 'medicare_opps_stateavg' in subset.columns:
                                        opps_medicare = subset['medicare_opps_stateavg'].dropna()
                                        if len(opps_medicare) > 0:
                                            metric_item['avg_medicare_opps_stateavg'] = round(opps_medicare.mean(), 2)
                                    
                                    if 'negotiated_rate_pct_of_medicare_asc' in subset.columns:
                                        asc_pct = subset['negotiated_rate_pct_of_medicare_asc'].dropna()
                                        if len(asc_pct) > 0:
                                            metric_item['avg_negotiated_rate_pct_of_medicare_asc'] = round(asc_pct.mean(), 2)
                                    
                                    if 'negotiated_rate_pct_of_medicare_opps' in subset.columns:
                                        opps_pct = subset['negotiated_rate_pct_of_medicare_opps'].dropna()
                                        if len(opps_pct) > 0:
                                            metric_item['avg_negotiated_rate_pct_of_medicare_opps'] = round(opps_pct.mean(), 2)
                            
                            metrics_data.append(metric_item)
                    
                    # Add formatting flags for frontend
                    key_metrics_info = {
                        'total_unique_values': len(combined_df[col].dropna().unique()),
                        'top_values': metrics_data[:10]  # Top 10 for display
                    }
                    
                    # Add formatting information for frontend
                    formatting_info = {
                        'avg_negotiated_rate': {'currency_format': True},
                        'avg_medicare_professional_rate': {'currency_format': True},
                        'avg_medicare_asc_stateavg': {'currency_format': True},
                        'avg_medicare_opps_stateavg': {'currency_format': True},
                        'avg_negotiated_rate_pct_of_medicare_professional': {'percentage_format': True},
                        'avg_negotiated_rate_pct_of_medicare_asc': {'percentage_format': True},
                        'avg_negotiated_rate_pct_of_medicare_opps': {'percentage_format': True}
                    }
                    
                    key_metrics_info['formatting_info'] = formatting_info
                    
                    analysis['key_metrics'][col] = key_metrics_info
                    
                except Exception as e:
                    logger.warning(f"Could not analyze key metric {col}: {e}")
                    analysis['key_metrics'][col] = {'error': str(e)}
        
        # Add payer comparison analysis if multiple payers are selected
        analysis['payer_comparison'] = {}
        if 'payer_slug' in combined_df.columns:
            try:
                # Get unique payers in the dataset
                unique_payers = combined_df['payer_slug'].dropna().unique()
                
                if len(unique_payers) > 1:
                    payer_comparison_data = []
                    
                    for payer in unique_payers:
                        payer_data = combined_df[combined_df['payer_slug'] == payer]
                        
                        # Calculate metrics for this payer
                        payer_metrics = {
                            'payer_slug': str(payer),
                            'record_count': len(payer_data),
                            'avg_negotiated_rate': round(payer_data['negotiated_rate'].mean(), 2) if 'negotiated_rate' in payer_data.columns else None,
                            'median_negotiated_rate': round(payer_data['negotiated_rate'].median(), 2) if 'negotiated_rate' in payer_data.columns else None,
                            'min_negotiated_rate': round(payer_data['negotiated_rate'].min(), 2) if 'negotiated_rate' in payer_data.columns else None,
                            'max_negotiated_rate': round(payer_data['negotiated_rate'].max(), 2) if 'negotiated_rate' in payer_data.columns else None,
                        }
                        
                        # Add top taxonomy descriptions for this payer
                        if 'primary_taxonomy_desc' in payer_data.columns:
                            top_taxonomies = payer_data['primary_taxonomy_desc'].value_counts().head(5)
                            payer_metrics['top_taxonomies'] = [
                                {'desc': str(desc), 'count': count} 
                                for desc, count in top_taxonomies.items()
                            ]
                        
                        # Add top organizations for this payer
                        if 'organization_name' in payer_data.columns:
                            top_orgs = payer_data['organization_name'].value_counts().head(5)
                            payer_metrics['top_organizations'] = [
                                {'name': str(name), 'count': count} 
                                for name, count in top_orgs.items()
                            ]
                        
                        payer_comparison_data.append(payer_metrics)
                    
                    # Sort by record count (descending)
                    payer_comparison_data.sort(key=lambda x: x['record_count'], reverse=True)
                    
                    analysis['payer_comparison'] = {
                        'total_payers': len(unique_payers),
                        'payers': payer_comparison_data
                    }
                    
            except Exception as e:
                logger.warning(f"Could not analyze payer comparison: {e}")
                analysis['payer_comparison'] = {'error': str(e)}
        
        # Get sample data for preview
        sample_data = combined_df.head(100).to_dict('records') if not combined_df.empty else []
        
        # Handle export requests
        export_format = request.GET.get('format')
        if export_format and combined_df is not None:
            try:
                export_data = navigator.export_data(combined_df, export_format)
                response = HttpResponse(export_data, content_type=f'application/{export_format}')
                response['Content-Disposition'] = f'attachment; filename="dataset_review.{export_format}"'
                return response
            except Exception as e:
                logger.error(f"Export error: {e}")
                return render(request, 'core/error.html', {
                    'error_message': f'Export failed: {str(e)}'
                })
        
        # Calculate total time
        total_time = time.time() - start_time
        logger.info(f"Total dataset review time: {total_time:.2f} seconds")
        
        # Get available filter options for additional filters modal
        available_filters = {}
        if combined_df is not None and not combined_df.empty:
            try:
                import json
                # Increase limits for filter options since we have more data now
                available_filters = {
                    'available_taxonomy_descs': json.dumps(sorted(combined_df['primary_taxonomy_desc'].dropna().unique().tolist())[:500]),
                    'available_proc_classes': json.dumps(sorted(combined_df['proc_class'].dropna().unique().tolist())[:500]),
                    'available_proc_groups': json.dumps(sorted(combined_df['proc_group'].dropna().unique().tolist())[:500]),
                    'available_codes': json.dumps(sorted(combined_df['code'].dropna().unique().tolist())[:500]),
                    'available_county_names': json.dumps(sorted(combined_df['county_name'].dropna().unique().tolist())[:500]),
                    'available_stat_area_names': json.dumps(sorted(combined_df['stat_area_name'].dropna().unique().tolist())[:500]),
                    'available_payer_slugs': json.dumps(sorted(combined_df['payer_slug'].dropna().unique().tolist())[:500])
                }
            except Exception as e:
                logger.warning(f"Could not get available filter options: {e}")
                available_filters = {
                    'available_taxonomy_descs': '[]',
                    'available_proc_classes': '[]',
                    'available_proc_groups': '[]',
                    'available_codes': '[]',
                    'available_county_names': '[]',
                    'available_stat_area_names': '[]',
                    'available_payer_slugs': '[]'
                }

        # Prepare context
        context = {
            'filters': all_filters,
            'partitions_df': partitions_df,
            'combined_df_info': {
                'shape': combined_df.shape,
                'columns': list(combined_df.columns),
                'load_summary': getattr(combined_df, 'attrs', {}).get('_load_summary', {}),
                'total_time_seconds': round(total_time, 2)
            },
            'analysis': analysis,
            'sample_data': sample_data,
            'max_rows': max_rows,
            'analysis_type': analysis_type,
            'has_data': combined_df is not None and not combined_df.empty,
            'total_partitions': len(partitions_df),
            's3_paths_count': len(s3_paths),
            **available_filters
        }
        
    except Exception as e:
        logger.error(f"Error in dataset_review view: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        context = {
            'filters': {},
            'partitions_df': pd.DataFrame(),
            'combined_df_info': {},
            'analysis': {},
            'sample_data': [],
            'max_rows': 100000,
            'analysis_type': 'comprehensive',
            'has_data': False,
            'error_message': 'An error occurred while analyzing the dataset.',
            'total_partitions': 0,
            's3_paths_count': 0
        }
    
    return render(request, 'core/dataset_review.html', context)


@login_required
def dataset_review_filtered(request):
    """
    API endpoint for client-side filtering on the existing combined DataFrame
    This avoids regenerating partitions and works with the data already in memory
    """
    import json
    
    try:
        import pandas as pd
        
        # Get all filters from request (same as original dataset_review)
        all_filters = {
            'payer_slug': request.GET.getlist('payer_slug'),
            'state': request.GET.get('state'),
            'billing_class': request.GET.get('billing_class'),
            'procedure_set': request.GET.get('procedure_set'),
            'taxonomy_code': request.GET.get('taxonomy_code'),
            # Additional analysis filters (may be multi-valued)
            'taxonomy_desc': request.GET.getlist('taxonomy_desc') or request.GET.get('taxonomy_desc'),
            'stat_area_name': request.GET.getlist('stat_area_name') or request.GET.get('stat_area_name'),
            'county_name': request.GET.getlist('county_name') or request.GET.get('county_name'),
            'proc_class': request.GET.getlist('proc_class') or request.GET.get('proc_class'),
            'proc_group': request.GET.getlist('proc_group') or request.GET.get('proc_group'),
            'code': request.GET.getlist('code') or request.GET.get('code'),
            'year': request.GET.get('year'),
            'month': request.GET.get('month')
        }
        
        # Remove empty filters
        all_filters = {k: v for k, v in all_filters.items() if v and (not isinstance(v, list) or len(v) > 0)}
        
        # Separate partition-level filters from data-level filters (same as original)
        partition_filters = {k: v for k, v in all_filters.items() if k in ['payer_slug', 'state', 'billing_class', 'procedure_set', 'taxonomy_code', 'taxonomy_desc', 'stat_area_name', 'year', 'month']}
        data_filters = {k: v for k, v in all_filters.items() if k in ['proc_class', 'proc_group', 'code', 'county_name']}
        
        # Initialize partition navigator
        navigator = PartitionNavigator(
            db_path='core/data/partition_navigation.db'
        )
        
        # Get partitions based on partition-level filters
        partitions_df = navigator.search_partitions(partition_filters)
        
        if partitions_df.empty:
            return JsonResponse({
                'error': 'No partitions found for the given filters',
                'has_data': False
            })
        
        # Get S3 paths for combination
        s3_paths = [f"s3://{row['s3_bucket']}/{row['s3_key']}" for _, row in partitions_df.iterrows()]
        
        # Get analysis parameters
        max_rows = int(request.GET.get('max_rows', 100000))
        max_partitions = int(request.GET.get('max_partitions', 500))
        
        # Limit partitions
        if len(s3_paths) > max_partitions:
            s3_paths = s3_paths[:max_partitions]
        
        # Combine partitions
        combined_df = navigator.combine_partitions_for_analysis(s3_paths, max_rows)
        
        if combined_df is None or combined_df.empty:
            return JsonResponse({
                'error': 'Failed to load data from partitions',
                'has_data': False
            })
        
        # Apply client-side filters to the existing DataFrame
        original_count = len(combined_df)
        filtered_df = combined_df.copy()
        
        # Apply data-level filters to the existing DataFrame
        for filter_name, filter_values in data_filters.items():
            if filter_values and filter_name in filtered_df.columns:
                if isinstance(filter_values, list):
                    filtered_df = filtered_df[filtered_df[filter_name].isin(filter_values)]
                else:
                    filtered_df = filtered_df[filtered_df[filter_name] == filter_values]
        
        # Generate analysis for filtered data
        analysis = {
            'dataset_summary': {
                'total_rows': len(filtered_df),
                'original_rows': original_count,
                'rows_filtered': original_count - len(filtered_df)
            },
            'basic_stats': {},
            'key_metrics': {}
        }
        
        # Basic stats for key columns
        key_columns = ['state', 'payer_slug', 'billing_class', 'negotiated_rate', 'organization_name', 'primary_taxonomy_desc']
        for col in key_columns:
            if col in filtered_df.columns:
                try:
                    if filtered_df[col].dtype == 'object':
                        unique_vals = filtered_df[col].dropna().unique()[:10]
                        analysis['basic_stats'][col] = {
                            'type': 'categorical',
                            'unique_count': len(filtered_df[col].dropna().unique()),
                            'sample_values': list(unique_vals)
                        }
                    else:
                        analysis['basic_stats'][col] = {
                            'type': 'numeric',
                            'count': filtered_df[col].count(),
                            'mean': round(filtered_df[col].mean(), 2) if filtered_df[col].dtype in ['int64', 'float64'] else None,
                            'min': filtered_df[col].min() if filtered_df[col].dtype in ['int64', 'float64'] else None,
                            'max': filtered_df[col].max() if filtered_df[col].dtype in ['int64', 'float64'] else None
                        }
                except Exception as e:
                    analysis['basic_stats'][col] = {'error': str(e)}
        
        # Add comprehensive key metrics analysis (same as main dataset_review)
        key_metric_columns = [
            'stat_area_name', 'tin_value', 'county_name', 
            'primary_taxonomy_desc', 'enumeration_type', 'organization_name',
            'proc_class', 'proc_group', 'code'
        ]
        
        for col in key_metric_columns:
            if col in filtered_df.columns:
                try:
                    # Get unique values and their counts
                    value_counts = filtered_df[col].value_counts().head(20)
                    
                    metrics_data = []
                    for value, count in value_counts.items():
                        # Filter data for this specific value
                        value_data = filtered_df[filtered_df[col] == value]
                        
                        if len(value_data) > 0:
                            metric_item = {
                                'value': str(value),
                                'count': int(count),
                                'percentage': round((count / len(filtered_df)) * 100, 2)
                            }
                            
                            # Add financial metrics if negotiated_rate exists
                            if 'negotiated_rate' in filtered_df.columns:
                                rates = value_data['negotiated_rate'].dropna()
                                if len(rates) > 0:
                                    metric_item['avg_negotiated_rate'] = round(rates.mean(), 2)
                                    metric_item['median_negotiated_rate'] = round(rates.median(), 2)
                                    metric_item['min_negotiated_rate'] = round(rates.min(), 2)
                                    metric_item['max_negotiated_rate'] = round(rates.max(), 2)
                            
                            # Add Medicare benchmark comparisons if available
                            if 'medicare_professional_rate' in filtered_df.columns:
                                prof_rates = value_data['medicare_professional_rate'].dropna()
                                if len(prof_rates) > 0:
                                    metric_item['avg_medicare_professional_rate'] = round(prof_rates.mean(), 2)
                                    
                                    # Calculate percentage of Medicare
                                    if 'avg_negotiated_rate' in metric_item:
                                        prof_pct = (metric_item['avg_negotiated_rate'] / prof_rates.mean()) * 100
                                        metric_item['avg_negotiated_rate_pct_of_medicare_professional'] = round(prof_pct, 2)
                            
                            if 'medicare_asc_stateavg' in filtered_df.columns:
                                asc_rates = value_data['medicare_asc_stateavg'].dropna()
                                if len(asc_rates) > 0:
                                    metric_item['avg_medicare_asc_stateavg'] = round(asc_rates.mean(), 2)
                                    
                                    if 'avg_negotiated_rate' in metric_item:
                                        asc_pct = (metric_item['avg_negotiated_rate'] / asc_rates.mean()) * 100
                                        metric_item['avg_negotiated_rate_pct_of_medicare_asc'] = round(asc_pct, 2)
                            
                            if 'medicare_opps_stateavg' in filtered_df.columns:
                                opps_rates = value_data['medicare_opps_stateavg'].dropna()
                                if len(opps_rates) > 0:
                                    metric_item['avg_medicare_opps_stateavg'] = round(opps_rates.mean(), 2)
                                    
                                    if 'avg_negotiated_rate' in metric_item:
                                        opps_pct = (metric_item['avg_negotiated_rate'] / opps_rates.mean()) * 100
                                        metric_item['avg_negotiated_rate_pct_of_medicare_opps'] = round(opps_pct, 2)
                            
                            metrics_data.append(metric_item)
                    
                    # Add formatting flags for frontend
                    key_metrics_info = {
                        'total_unique_values': len(filtered_df[col].dropna().unique()),
                        'top_values': metrics_data[:10]  # Top 10 for display
                    }
                    
                    # Add formatting information for frontend
                    formatting_info = {
                        'avg_negotiated_rate': {'currency_format': True},
                        'avg_medicare_professional_rate': {'currency_format': True},
                        'avg_medicare_asc_stateavg': {'currency_format': True},
                        'avg_medicare_opps_stateavg': {'currency_format': True},
                        'avg_negotiated_rate_pct_of_medicare_professional': {'percentage_format': True},
                        'avg_negotiated_rate_pct_of_medicare_asc': {'percentage_format': True},
                        'avg_negotiated_rate_pct_of_medicare_opps': {'percentage_format': True}
                    }
                    
                    key_metrics_info['formatting_info'] = formatting_info
                    
                    analysis['key_metrics'][col] = key_metrics_info
                    
                except Exception as e:
                    logger.warning(f"Could not analyze key metric {col}: {e}")
                    analysis['key_metrics'][col] = {'error': str(e)}
        
        # Sample data (handle NaN values)
        if not filtered_df.empty:
            # Replace NaN values with None for JSON serialization
            sample_df = filtered_df.head(100).copy()
            sample_df = sample_df.where(pd.notnull(sample_df), None)
            sample_data = sample_df.to_dict('records')
        else:
            sample_data = []
        
        # Get updated available filter options from filtered data
        available_filters = {}
        if not filtered_df.empty:
            available_filters = {
                'available_taxonomy_descs': sorted(filtered_df['primary_taxonomy_desc'].dropna().unique().tolist())[:500],
                'available_proc_classes': sorted(filtered_df['proc_class'].dropna().unique().tolist())[:500],
                'available_proc_groups': sorted(filtered_df['proc_group'].dropna().unique().tolist())[:500],
                'available_codes': sorted(filtered_df['code'].dropna().unique().tolist())[:500],
                'available_county_names': sorted(filtered_df['county_name'].dropna().unique().tolist())[:500],
                'available_stat_area_names': sorted(filtered_df['stat_area_name'].dropna().unique().tolist())[:500],
                'available_payer_slugs': sorted(filtered_df['payer_slug'].dropna().unique().tolist())[:500]
            }
        
        # Convert numpy types to Python types for JSON serialization
        def convert_numpy_types(obj):
            import numpy as np
            import math
            
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                # Handle NaN and infinity values
                if np.isnan(obj) or math.isnan(obj):
                    return None
                elif np.isinf(obj) or math.isinf(obj):
                    return None
                else:
                    return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif hasattr(obj, 'item'):  # numpy scalar
                result = obj.item()
                # Handle NaN values from .item()
                if isinstance(result, float) and (np.isnan(result) or math.isnan(result)):
                    return None
                return result
            elif isinstance(obj, dict):
                return {k: convert_numpy_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(item) for item in obj]
            elif isinstance(obj, float):
                # Handle regular Python float NaN values
                if math.isnan(obj):
                    return None
            return obj
        
        # Convert sample data to ensure JSON serialization
        sample_data_serializable = []
        for record in sample_data:
            sample_data_serializable.append(convert_numpy_types(record))
        
        # Convert analysis data
        analysis_serializable = convert_numpy_types(analysis)
        available_filters_serializable = convert_numpy_types(available_filters)
        
        # Use Django's JSONResponse with custom encoder to handle NaN values
        from django.core.serializers.json import DjangoJSONEncoder
        
        class NaNHandlingJSONEncoder(DjangoJSONEncoder):
            def encode(self, obj):
                # Convert NaN values to None before encoding
                if isinstance(obj, float) and (obj != obj):  # NaN check
                    return None
                return super().encode(obj)
        
        return JsonResponse({
            'has_data': True,
            'analysis': analysis_serializable,
            'sample_data': sample_data_serializable,
            'available_filters': available_filters_serializable,
            'combined_df_info': {
                'shape': [int(filtered_df.shape[0]), int(filtered_df.shape[1])],
                'columns': list(filtered_df.columns)
            }
        }, encoder=NaNHandlingJSONEncoder)
        
    except Exception as e:
        logger.error(f"Error in dataset_review_filtered: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({
            'error': str(e),
            'has_data': False
        })


@login_required
def data_availability_overview(request):
    """
    Data Availability Overview Page
    Shows comprehensive data availability metrics based on partition navigation database
    """
    try:
        # Initialize partition navigator
        navigator = PartitionNavigator(
            db_path='core/data/partition_navigation.db',
            s3_bucket='partitioned-data'
        )
        
        # Get comprehensive data availability metrics
        availability_metrics = navigator.get_data_availability_metrics()
        
        context = {
            'availability_metrics': availability_metrics,
            'has_data': True
        }
        
    except Exception as e:
        logger.error(f"Error in data_availability_overview view: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while loading data availability metrics.',
            'availability_metrics': {}
        }
    
    return render(request, 'core/data_availability_overview.html', context)


@login_required
def data_availability_test(request):
    """
    Test view for data availability with simple template
    """
    try:
        # Initialize partition navigator
        navigator = PartitionNavigator(
            db_path='core/data/partition_navigation.db',
            s3_bucket='partitioned-data'
        )
        
        # Get comprehensive data availability metrics
        availability_metrics = navigator.get_data_availability_metrics()
        
        context = {
            'availability_metrics': availability_metrics,
            'has_data': True
        }
        
    except Exception as e:
        logger.error(f"Error in data_availability_test view: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while loading data availability metrics.',
            'availability_metrics': {}
        }
    
    return render(request, 'core/test_data_availability.html', context)


@login_required
def transparency_dashboard(request, state):
    """
    Transparency Dashboard for specific state
    Shows Workers' Comp rate transparency analysis
    """
    try:
        # Validate state code
        state = state.upper()
        available_states = ParquetDataManager.get_available_states()
        
        if state not in available_states or available_states[state] != 'available':
            context = {
                'has_data': False,
                'error_message': f'Sorry, {state} data is not available yet. Please try another state.',
                'state_code': state,
                'state_name': ParquetDataManager.get_state_name(state)
            }
            return render(request, 'core/transparency_dashboard.html', context)
        
        # Get NPI type from request parameters
        npi_type = request.GET.get('npi_type')
        
        # Initialize data manager
        data_manager = ParquetDataManager(state=state, npi_type=npi_type)
        
        # Get overview statistics
        overview_stats = data_manager.get_overview_statistics()
        
        context = {
            'has_data': True,
            'state_code': state,
            'state_name': ParquetDataManager.get_state_name(state),
            'overview_stats': overview_stats,
            'npi_type': npi_type
        }
        
    except Exception as e:
        logger.error(f"Error in transparency_dashboard view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while loading the transparency dashboard.',
            'state_code': state,
            'state_name': ParquetDataManager.get_state_name(state) if 'state' in locals() else 'Unknown'
        }
    
    return render(request, 'core/transparency_dashboard.html', context)


@login_required
def rate_analysis(request, state):
    """
    Rate Analysis for specific state
    Shows detailed rate analysis and benchmarking
    """
    try:
        # Validate state code
        state = state.upper()
        available_states = ParquetDataManager.get_available_states()
        
        if state not in available_states or available_states[state] != 'available':
            context = {
                'has_data': False,
                'error_message': f'Sorry, {state} data is not available yet. Please try another state.',
                'state_code': state,
                'state_name': ParquetDataManager.get_state_name(state)
            }
            return render(request, 'core/rate_analysis.html', context)
        
        # Get NPI type from request parameters
        npi_type = request.GET.get('npi_type')
        
        # Initialize data manager
        data_manager = ParquetDataManager(state=state, npi_type=npi_type)
        
        # Get active filters
        active_filters = {
            'payer': request.GET.getlist('payer'),
            'procedure_set': request.GET.getlist('procedure_set'),
            'procedure_class': request.GET.getlist('procedure_class'),
            'org_name': request.GET.getlist('org_name'),
            'tin_value': request.GET.getlist('tin_value'),
            'billing_code': request.GET.getlist('billing_code')
        }
        
        # Remove empty filters
        active_filters = {k: v for k, v in active_filters.items() if v}
        
        # Get filtered options and statistics
        filters = {
            'payers': data_manager.get_unique_values('payer', active_filters),
            'procedure_sets': data_manager.get_unique_values('procedure_set', active_filters),
            'procedure_classes': data_manager.get_unique_values('procedure_class', active_filters),
            'organizations': data_manager.get_unique_values('org_name', active_filters),
            'tin_values': data_manager.get_unique_values('tin_value', active_filters),
            'billing_codes': data_manager.get_unique_values('billing_code', active_filters),
        }
        
        stats = data_manager.get_aggregated_stats(active_filters)
        sample_records = data_manager.get_sample_records(active_filters, limit=10)
        
        context = {
            'has_data': True,
            'state_code': state,
            'state_name': ParquetDataManager.get_state_name(state),
            'filters': filters,
            'stats': stats,
            'active_filters': active_filters,
            'sample_records': sample_records,
            'npi_type': npi_type
        }
        
    except Exception as e:
        logger.error(f"Error in rate_analysis view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while loading rate analysis.',
            'state_code': state,
            'state_name': ParquetDataManager.get_state_name(state) if 'state' in locals() else 'Unknown'
        }
    
    return render(request, 'core/rate_analysis.html', context)


@login_required
def benchmark_comparison(request):
    """
    Benchmark Comparison Dashboard
    Shows multi-benchmark comparison analysis
    """
    try:
        # Get comparison parameters
        payer = request.GET.get('payer')
        state = request.GET.get('state')
        billing_class = request.GET.get('billing_class')
        
        context = {
            'has_data': True,
            'payer': payer,
            'state': state,
            'billing_class': billing_class
        }
        
    except Exception as e:
        logger.error(f"Error in benchmark_comparison view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while loading benchmark comparison.'
        }
    
    return render(request, 'core/benchmark_comparison.html', context)


@login_required
def steerage_preview(request):
    """
    Steerage Preview Dashboard (Stage 3 Foundation)
    Shows preview of steerage guidance capabilities
    """
    try:
        context = {
            'has_data': True
        }
        
    except Exception as e:
        logger.error(f"Error in steerage_preview view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while loading steerage preview.'
        }
    
    return render(request, 'core/steerage_preview.html', context)


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



