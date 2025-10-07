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
from .models import UserActivity, TinRecord
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
        
        # Filter out partitions with null values in key fields
        logger.info(f"Found {len(partitions_df)} partitions before null filtering")
        
        # Filter out partitions with null taxonomy_code, procedure_set, or other key fields
        null_filter_conditions = (
            (partitions_df['taxonomy_code'].notna()) &
            (partitions_df['taxonomy_code'] != '') &
            (partitions_df['taxonomy_code'] != '__NULL__') &
            (partitions_df['procedure_set'].notna()) &
            (partitions_df['procedure_set'] != '') &
            (partitions_df['procedure_set'] != '__NULL__') &
            (partitions_df['stat_area_name'].notna()) &
            (partitions_df['stat_area_name'] != '') &
            (partitions_df['stat_area_name'] != '__NULL__')
        )
        
        partitions_df = partitions_df[null_filter_conditions]
        
        if partitions_df.empty:
            return render(request, 'core/error.html', {
                'error_message': 'No partitions with complete data found. All partitions contain null values in key fields.'
            })
        
        logger.info(f"After null filtering: {len(partitions_df)} partitions with complete data")
        
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
        # Import cache system
        from .cache import DatasetCache
        
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
        
        # Get analysis parameters
        max_rows = int(request.GET.get('max_rows', 100000))  # Default to 100k rows
        max_partitions = int(request.GET.get('max_partitions', 500))  # Default to 500 partitions max
        analysis_type = request.GET.get('analysis_type', 'comprehensive')
        
        # Generate cache key
        cache_key = DatasetCache.generate_cache_key(all_filters, max_rows, max_partitions)
        
        # Try to get cached dataset info
        cached_dataset = DatasetCache.get_cached_dataset(cache_key)
        
        if cached_dataset:
            # Use cached dataset info
            partitions_df = pd.DataFrame(cached_dataset['partitions_info'])
            s3_paths = cached_dataset['s3_paths']
            logger.info(f"Using cached dataset with {len(s3_paths)} partitions")
        else:
            # Load fresh data
            logger.info("Loading fresh dataset from S3")
            
            # Search for partitions
            partitions_df = navigator.search_partitions(filters)
            
            # Filter out partitions with null values in key fields
            if not partitions_df.empty:
                logger.info(f"Found {len(partitions_df)} partitions before null filtering")
                
                # Filter out partitions with null taxonomy_code, procedure_set, or other key fields
                null_filter_conditions = (
                    (partitions_df['taxonomy_code'].notna()) &
                    (partitions_df['taxonomy_code'] != '') &
                    (partitions_df['taxonomy_code'] != '__NULL__') &
                    (partitions_df['procedure_set'].notna()) &
                    (partitions_df['procedure_set'] != '') &
                    (partitions_df['procedure_set'] != '__NULL__') &
                    (partitions_df['stat_area_name'].notna()) &
                    (partitions_df['stat_area_name'] != '') &
                    (partitions_df['stat_area_name'] != '__NULL__')
                )
                
                partitions_df = partitions_df[null_filter_conditions]
                logger.info(f"After null filtering: {len(partitions_df)} partitions with complete data")
        
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
                'max_rows': max_rows,
                'analysis_type': analysis_type,
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
        
        # Get S3 paths for combination (only if not cached)
        if not cached_dataset:
            s3_paths = [f"s3://{row['s3_bucket']}/{row['s3_key']}" for _, row in partitions_df.iterrows()]
            
            # Limit partitions to prevent timeouts
            if len(s3_paths) > max_partitions:
                logger.warning(f"Limiting partitions from {len(s3_paths)} to {max_partitions} to prevent timeout")
                s3_paths = s3_paths[:max_partitions]
            
            # Cache the dataset info
            dataset_info = {
                'partitions_info': partitions_df.to_dict('records'),
                's3_paths': s3_paths,
                'max_rows': max_rows,
                'max_partitions': max_partitions,
                'load_timestamp': time.time(),
                'filters': all_filters
            }
            DatasetCache.cache_dataset(cache_key, dataset_info)
        
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
        
        # Store cache key in session for filtered endpoint
        request.session['dataset_cache_key'] = cache_key
        request.session['dataset_filters'] = all_filters
        
        # Combine partitions for analysis
        logger.info(f"Starting to combine {len(s3_paths)} partitions (max_rows: {max_rows})")
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
        
        # Generate comprehensive analysis
        logger.info("Starting comprehensive analysis...")
        analysis = navigator.get_comprehensive_analysis(combined_df)
        logger.info("Comprehensive analysis completed")
        
        # Add key metrics analysis (this was missing from the comprehensive analysis)
        analysis['key_metrics'] = {}
        key_metric_columns = [
            'stat_area_name', 'tin_value', 'county_name', 
            'primary_taxonomy_desc', 'enumeration_type', 'organization_name',
            'proc_class', 'proc_group', 'code'
        ]
        
        for col in key_metric_columns:
            if col in combined_df.columns:
                try:
                    # Get unique values and their counts
                    value_counts = combined_df[col].value_counts().head(20)
                    
                    metrics_data = []
                    for value, count in value_counts.items():
                        # Filter data for this specific value
                        value_data = combined_df[combined_df[col] == value]
                        
                        if len(value_data) > 0:
                            metric_item = {
                                'value': str(value),
                                'count': int(count),
                                'percentage': round((count / len(combined_df)) * 100, 2)
                            }
                            
                            # Add financial metrics if negotiated_rate exists
                            if 'negotiated_rate' in combined_df.columns:
                                rates = value_data['negotiated_rate'].dropna()
                                if len(rates) > 0:
                                    metric_item['avg_negotiated_rate'] = round(rates.mean(), 2)
                                    metric_item['median_negotiated_rate'] = round(rates.median(), 2)
                                    metric_item['min_negotiated_rate'] = round(rates.min(), 2)
                                    metric_item['max_negotiated_rate'] = round(rates.max(), 2)
                            
                            # Add Medicare benchmark comparisons if available
                            if 'medicare_professional_rate' in combined_df.columns:
                                prof_rates = value_data['medicare_professional_rate'].dropna()
                                if len(prof_rates) > 0:
                                    metric_item['avg_medicare_professional_rate'] = round(prof_rates.mean(), 2)
                                    
                                    # Calculate percentage of Medicare
                                    if 'avg_negotiated_rate' in metric_item:
                                        prof_pct = (metric_item['avg_negotiated_rate'] / prof_rates.mean()) * 100
                                        metric_item['avg_negotiated_rate_pct_of_medicare_professional'] = round(prof_pct, 2)
                            
                            if 'medicare_asc_stateavg' in combined_df.columns:
                                asc_rates = value_data['medicare_asc_stateavg'].dropna()
                                if len(asc_rates) > 0:
                                    metric_item['avg_medicare_asc_stateavg'] = round(asc_rates.mean(), 2)
                                    
                                    if 'avg_negotiated_rate' in metric_item:
                                        asc_pct = (metric_item['avg_negotiated_rate'] / asc_rates.mean()) * 100
                                        metric_item['avg_negotiated_rate_pct_of_medicare_asc'] = round(asc_pct, 2)
                            
                            if 'medicare_opps_stateavg' in combined_df.columns:
                                opps_rates = value_data['medicare_opps_stateavg'].dropna()
                                if len(opps_rates) > 0:
                                    metric_item['avg_medicare_opps_stateavg'] = round(opps_rates.mean(), 2)
                                    
                                    if 'avg_negotiated_rate' in metric_item:
                                        opps_pct = (metric_item['avg_negotiated_rate'] / opps_rates.mean()) * 100
                                        metric_item['avg_negotiated_rate_pct_of_medicare_opps'] = round(opps_pct, 2)
                            
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
                    'available_taxonomy_descs': sorted(combined_df['primary_taxonomy_desc'].dropna().unique().tolist())[:500],
                    'available_proc_classes': sorted(combined_df['proc_class'].dropna().unique().tolist())[:500],
                    'available_proc_groups': sorted(combined_df['proc_group'].dropna().unique().tolist())[:500],
                    'available_codes': sorted(combined_df['code'].dropna().unique().tolist())[:500],
                    'available_county_names': sorted(combined_df['county_name'].dropna().unique().tolist())[:500],
                    'available_stat_area_names': sorted(combined_df['stat_area_name'].dropna().unique().tolist())[:500],
                    'available_payer_slugs': sorted(combined_df['payer_slug'].dropna().unique().tolist())[:500]
                }
            except Exception as e:
                logger.warning(f"Could not get available filter options: {e}")
                available_filters = {
                    'available_taxonomy_descs': [],
                    'available_proc_classes': [],
                    'available_proc_groups': [],
                    'available_codes': [],
                    'available_county_names': [],
                    'available_stat_area_names': [],
                    'available_payer_slugs': []
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
def dataset_review_data(request):
    """Single endpoint that loads data once and returns as JSON for PowerBI-style client-side filtering"""
    try:
        # Get filters from request
        all_filters = {
            'payer_slug': request.GET.getlist('payer_slug'),
            'state': request.GET.get('state'),
            'billing_class': request.GET.get('billing_class'),
            'procedure_set': request.GET.get('procedure_set'),
            'taxonomy_code': request.GET.get('taxonomy_code'),
            'taxonomy_desc': request.GET.getlist('taxonomy_desc') or request.GET.get('taxonomy_desc'),
            'stat_area_name': request.GET.getlist('stat_area_name') or request.GET.get('stat_area_name'),
            'year': request.GET.get('year'),
            'month': request.GET.get('month')
        }
        
        # Remove empty filters
        all_filters = {k: v for k, v in all_filters.items() if v and (not isinstance(v, list) or len(v) > 0)}
        
        # Get parameters with reduced defaults for better performance
        max_rows = int(request.GET.get('max_rows', 100000))  # Increased to 100k rows
        max_partitions = int(request.GET.get('max_partitions', 200))  # Reduced default
        
        # Initialize navigator
        navigator = PartitionNavigator(db_path='core/data/partition_navigation.db')
        
        # Search for partitions
        partition_filters = {k: v for k, v in all_filters.items() 
                           if k in ['payer_slug', 'state', 'billing_class', 'procedure_set', 
                                   'taxonomy_code', 'taxonomy_desc', 'stat_area_name', 'year', 'month']}
        
        partitions_df = navigator.search_partitions(partition_filters)
        
        if partitions_df.empty:
            return JsonResponse({
                'error': 'No data available with the selected filters.',
                'has_data': False
            })
        
        # Get S3 paths
        s3_paths = [f"s3://{row['s3_bucket']}/{row['s3_key']}" for _, row in partitions_df.iterrows()]
        
        # Limit partitions
        if len(s3_paths) > max_partitions:
            s3_paths = s3_paths[:max_partitions]
        
        # Combine partitions
        combined_df = navigator.combine_partitions_for_analysis(s3_paths, max_rows)
        
        if combined_df is None or combined_df.empty:
            return JsonResponse({
                'error': 'Failed to load data from partitions.',
                'has_data': False
            })
        
        # Convert to JSON-serializable format with proper NaN handling
        def convert_numpy_types(obj):
            import numpy as np
            import math
            
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                if np.isnan(obj) or math.isnan(obj):
                    return None
                elif np.isinf(obj) or math.isinf(obj):
                    return None
                else:
                    return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif hasattr(obj, 'item'):
                result = obj.item()
                if isinstance(result, float) and (np.isnan(result) or math.isnan(result)):
                    return None
                return result
            elif isinstance(obj, float):
                if math.isnan(obj):
                    return None
            return obj
        
        # Convert DataFrame to records with proper NaN handling
        data_records = []
        for _, row in combined_df.head(max_rows).iterrows():
            record = {}
            for col, value in row.items():
                record[col] = convert_numpy_types(value)
            data_records.append(record)
        
        # Get filter options from the data with proper NaN handling
        filter_options = {}
        key_columns = ['proc_class', 'proc_group', 'code', 'county_name', 'primary_taxonomy_desc', 'stat_area_name']
        
        for col in key_columns:
            if col in combined_df.columns:
                unique_values = combined_df[col].dropna().unique().tolist()
                filter_options[col] = [convert_numpy_types(val) for val in unique_values[:100]]
        
        # Store in session for potential export
        request.session['current_dataset'] = {
            'filters': all_filters,
            'total_rows': len(combined_df),
            's3_paths': s3_paths
        }
        
        # Use custom JSON encoder to handle any remaining NaN values
        from django.core.serializers.json import DjangoJSONEncoder
        
        class NaNHandlingJSONEncoder(DjangoJSONEncoder):
            def encode(self, obj):
                # Convert NaN values to None before encoding
                if isinstance(obj, float) and (obj != obj):  # NaN check
                    return None
                return super().encode(obj)
        
        return JsonResponse({
            'has_data': True,
            'data': data_records,
            'filter_options': filter_options,
            'metadata': {
                'total_rows': len(combined_df),
                'loaded_rows': len(data_records),
                'partitions_used': len(s3_paths),
                'columns': list(combined_df.columns)
            }
        }, encoder=NaNHandlingJSONEncoder)
        
    except Exception as e:
        logger.error(f"Error in dataset_review_data: {str(e)}")
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


@login_required
def tin_provider_lookup(request):
    """
    TIN and Provider Lookup view
    Allows users to search for providers by TIN or organization name
    """
    context = {
        'search_results': None,
        'provider_summary': None,
        'search_type': None,
        'search_query': None,
        'error_message': None
    }
    
    if request.method == 'POST':
        search_type = request.POST.get('search_type')
        search_query = request.POST.get('search_query', '').strip()
        
        if not search_query:
            context['error_message'] = 'Please enter a search term.'
        else:
            try:
                if search_type == 'tin':
                    # Search by TIN
                    provider_summary = TinRecord.get_provider_summary(search_query)
                    if provider_summary:
                        context['provider_summary'] = provider_summary
                        context['search_type'] = 'tin'
                        context['search_query'] = search_query
                        
                        # Log the search activity
                        UserActivity.log_activity(
                            user=request.user,
                            action='tin_lookup',
                            page_url=request.get_full_path(),
                            page_title='TIN and Provider Lookup',
                            tin_value=search_query
                        )
                    else:
                        context['error_message'] = f'No provider found with TIN: {search_query}'
                        
                elif search_type == 'organization':
                    # Search by organization name
                    search_results = TinRecord.search_by_organization_name(search_query)
                    if search_results:
                        context['search_results'] = search_results
                        context['search_type'] = 'organization'
                        context['search_query'] = search_query
                        
                        # Log the search activity
                        UserActivity.log_activity(
                            user=request.user,
                            action='organization_lookup',
                            page_url=request.get_full_path(),
                            page_title='TIN and Provider Lookup',
                            organization_name=search_query
                        )
                    else:
                        context['error_message'] = f'No organizations found matching: {search_query}'
                        
            except Exception as e:
                logger.error(f"Error in TIN/Provider lookup: {str(e)}")
                context['error_message'] = 'An error occurred while searching. Please try again.'
    
    return render(request, 'core/tin_provider_lookup.html', context)


@login_required
def tin_provider_lookup_ajax(request):
    """
    AJAX endpoint for organization name search with autocomplete
    """
    if request.method == 'GET':
        query = request.GET.get('q', '').strip()
        
        if len(query) < 2:
            return JsonResponse({'results': []})
        
        try:
            results = TinRecord.search_by_organization_name(query, limit=20)
            
            # Format results for autocomplete
            formatted_results = []
            for result in results:
                formatted_results.append({
                    'id': result['tin_value'],
                    'text': result['organization_name'],
                    'tin_value': result['tin_value'],
                    'tin_type': result['tin_type'],
                    'payer_slug': result['payer_slug'],
                    'state': result['state']
                })
            
            return JsonResponse({'results': formatted_results})
            
        except Exception as e:
            logger.error(f"Error in AJAX organization search: {str(e)}")
            return JsonResponse({'error': 'Search failed'}, status=500)
    
    return JsonResponse({'error': 'Invalid request'}, status=400)


@login_required
def rate_lookup_home(request):
    """
    Market Rate Lookup Home Page
    Shows overview of available data and navigation options
    """
    try:
        from .utils.market_rate_lookup import MarketRateLookup
        
        lookup = MarketRateLookup()
        data_summary = lookup.get_data_summary()
        
        context = {
            'data_summary': data_summary,
            'has_data': True
        }
        
    except Exception as e:
        logger.error(f"Error in market_rate_lookup_home view: {str(e)}")
        context = {
            'has_data': False,
            'error_message': 'An error occurred while loading the market rate lookup data.',
            'data_summary': {}
        }
    
    return render(request, 'core/rate_lookup_home.html', context)


@login_required
def rate_lookup_tin_lookup(request):
    """
    TIN Lookup view for Market Rate Lookup
    Allows users to search for TINs and see S3 tile availability
    """
    context = {
        'search_results': None,
        'tin_data': None,
        's3_availability': None,
        'search_query': None,
        'search_type': None,
        'error_message': None
    }
    
    if request.method == 'POST':
        search_type = request.POST.get('search_type')
        search_query = request.POST.get('search_query', '').strip()
        
        if not search_query:
            context['error_message'] = 'Please enter a search term.'
        else:
            try:
                from .utils.market_rate_lookup import MarketRateLookup
                
                lookup = MarketRateLookup()
                context['search_type'] = search_type
                context['search_query'] = search_query
                
                if search_type == 'tin':
                    # Search by TIN
                    results = lookup.search_tin_lookup(tin_value=search_query)
                    if results:
                        context['search_results'] = results
                        context['tin_data'] = results[0]  # Use first result as primary
                        
                        # Check S3 availability for the TIN
                        s3_data = lookup.check_s3_tiles_availability(search_query)
                        context['s3_availability'] = s3_data
                    else:
                        context['error_message'] = f'No TIN found matching: {search_query}'
                        
                elif search_type == 'organization':
                    # Search by organization name
                    results = lookup.search_tin_lookup(organization_name=search_query)
                    if results:
                        context['search_results'] = results
                    else:
                        context['error_message'] = f'No organizations found matching: {search_query}'
                        
            except Exception as e:
                logger.error(f"Error in TIN lookup: {str(e)}")
                context['error_message'] = 'An error occurred while searching. Please try again.'
    
    return render(request, 'core/rate_lookup_tin_lookup.html', context)


@login_required
def rate_lookup_episodes_care(request):
    """
    Episodes of Care / Cost Bundles view for Market Rate Lookup
    Shows episode templates and allows filtering by codes
    """
    context = {
        'episode_templates': None,
        'search_results': None,
        'selected_episode': None,
        'selected_codes': None,
        'error_message': None
    }
    
    try:
        from .utils.market_rate_lookup import MarketRateLookup
        
        lookup = MarketRateLookup()
        
        # Get all episode templates
        episode_templates = lookup.get_episode_templates()
        context['episode_templates'] = episode_templates
        
        # Handle episode selection and code search
        if request.method == 'POST':
            episode_id = request.POST.get('episode_id')
            episode_type = request.POST.get('episode_type')
            selected_codes = request.POST.getlist('selected_codes')
            
            if episode_id and episode_type:
                # Find the selected episode
                selected_episode = None
                if episode_type in episode_templates:
                    for episode in episode_templates[episode_type]:
                        if episode['episode_id'] == episode_id:
                            selected_episode = episode
                            break
                
                if selected_episode:
                    context['selected_episode'] = selected_episode
                    
                    # Get all codes from the episode
                    all_codes = []
                    if selected_episode.get('codes_required'):
                        all_codes.extend(selected_episode['codes_required'])
                    if selected_episode.get('codes_optional'):
                        all_codes.extend(selected_episode['codes_optional'])
                    if selected_episode.get('codes'):
                        all_codes.extend(selected_episode['codes'])
                    
                    # Search S3 tiles for these codes
                    if all_codes:
                        search_results = lookup.search_s3_tiles_by_codes(billing_codes=all_codes)
                        context['search_results'] = search_results
                        context['selected_codes'] = all_codes
        
        # Handle direct code search
        if request.method == 'GET':
            billing_codes = request.GET.getlist('billing_codes')
            taxonomy_codes = request.GET.getlist('taxonomy_codes')
            
            if billing_codes or taxonomy_codes:
                search_results = lookup.search_s3_tiles_by_codes(
                    billing_codes=billing_codes if billing_codes else None,
                    taxonomy_codes=taxonomy_codes if taxonomy_codes else None
                )
                context['search_results'] = search_results
                context['selected_codes'] = {
                    'billing_codes': billing_codes,
                    'taxonomy_codes': taxonomy_codes
                }
        
    except Exception as e:
        logger.error(f"Error in episodes of care view: {str(e)}")
        context['error_message'] = 'An error occurred while loading episode data.'
    
    return render(request, 'core/rate_lookup_episodes_care.html', context)


@login_required
def rate_lookup_data_explorer(request):
    """
    Data Explorer view for Market Rate Lookup
    Shows random tiles by default for easy exploration
    """
    import random
    import sqlite3
    
    context = {
        'search_results': None,
        'error_message': None,
        'is_random': False
    }
    
    try:
        # Get random tiles from s3_tiles
        dims_db_path = Path(__file__).resolve().parent / 'data' / 'dims.sqlite'
        conn = sqlite3.connect(str(dims_db_path))
        cursor = conn.cursor()
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM s3_tiles')
        total_tiles = cursor.fetchone()[0]
        context['total_tiles_available'] = total_tiles
        
        # Get 12 random tiles
        cursor.execute('''
            SELECT 
                payer_slug, billing_class, negotiation_arrangement, negotiated_type,
                tin_value, proc_set, proc_class, proc_group, s3_prefix,
                parts_count, row_count, billing_codes_json, taxonomy_codes_json,
                created_at_utc
            FROM s3_tiles
            ORDER BY RANDOM()
            LIMIT 12
        ''')
        
        tiles = cursor.fetchall()
        conn.close()
        
        # Convert to dict format
        search_results = []
        for tile in tiles:
            import json
            tile_dict = {
                'payer_slug': tile[0],
                'billing_class': tile[1],
                'negotiation_arrangement': tile[2],
                'negotiated_type': tile[3],
                'tin_value': tile[4],
                'proc_set': tile[5],
                'proc_class': tile[6],
                'proc_group': tile[7],
                's3_prefix': tile[8],
                'parts_count': tile[9],
                'row_count': tile[10],
                'billing_codes_json': json.loads(tile[11]) if tile[11] else [],
                'taxonomy_codes_json': json.loads(tile[12]) if tile[12] else [],
                'created_at_utc': tile[13]
            }
            
            # Get taxonomy display names
            if tile_dict['taxonomy_codes_json']:
                conn = sqlite3.connect(str(dims_db_path))
                cursor = conn.cursor()
                placeholders = ','.join(['?' for _ in tile_dict['taxonomy_codes_json']])
                query = f'SELECT "Display Name" FROM dim_taxonomy WHERE Code IN ({placeholders})'
                cursor.execute(query, tile_dict['taxonomy_codes_json'])
                tile_dict['taxonomy_display_names'] = [row[0] for row in cursor.fetchall()]
                conn.close()
            else:
                tile_dict['taxonomy_display_names'] = []
            
            search_results.append(tile_dict)
        
        context['search_results'] = search_results
        context['is_random'] = True
        
    except Exception as e:
        logger.error(f"Error in data explorer view: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        context['error_message'] = f'An error occurred while loading data explorer: {str(e)}'
    
    return render(request, 'core/rate_lookup_data_explorer.html', context)


@login_required
def rate_lookup_tin_details(request, tin_value):
    """
    Detailed view for a specific TIN showing all available data
    """
    context = {
        'tin_value': tin_value,
        'tin_data': None,
        's3_availability': None,
        'error_message': None
    }
    
    try:
        from .utils.market_rate_lookup import MarketRateLookup
        
        lookup = MarketRateLookup()
        
        # Get TIN data
        tin_results = lookup.search_tin_lookup(tin_value=tin_value)
        if tin_results:
            context['tin_data'] = tin_results[0]
        
        # Get S3 availability
        s3_data = lookup.check_s3_tiles_availability(tin_value)
        context['s3_availability'] = s3_data
        
        # Get filter options for the tiles
        filter_options = {}
        if s3_data and s3_data.get('tiles'):
            tiles = s3_data['tiles']
            filter_options = {
                'payer_slugs': sorted(list(set(tile['payer_slug'] for tile in tiles))),
                'proc_sets': sorted(list(set(tile['proc_set'] for tile in tiles))),
                'proc_classes': sorted(list(set(tile['proc_class'] for tile in tiles))),
                'proc_groups': sorted(list(set(tile['proc_group'] for tile in tiles))),
                'billing_classes': sorted(list(set(tile['billing_class'] for tile in tiles))),
                'negotiation_arrangements': sorted(list(set(tile['negotiation_arrangement'] for tile in tiles))),
                'negotiated_types': sorted(list(set(tile['negotiated_type'] for tile in tiles))),
                'billing_codes': sorted(list(set(code for tile in tiles for code in tile.get('billing_codes_json', [])))[:50]),  # Limit to 50 most common
                'taxonomy_display_names': sorted(list(set(name for tile in tiles for name in tile.get('taxonomy_display_names', []))))
            }
        context['filter_options'] = filter_options
        
    except Exception as e:
        logger.error(f"Error in TIN details view: {str(e)}")
        context['error_message'] = 'An error occurred while loading TIN details.'
    
    return render(request, 'core/rate_lookup_tin_details.html', context)


@login_required
def rate_analyzer(request):
    """
    Analyze a healthcare market by finding TINs within a geographic radius of a zip code
    """
    # Get distinct values for filter dropdowns from s3_tiles
    filter_options = {}
    try:
        from .utils.market_rate_lookup import MarketRateLookup
        lookup = MarketRateLookup()
        filter_options = lookup.get_s3_filter_options()
    except Exception as e:
        logger.error(f"Error fetching filter options: {str(e)}")
        filter_options = {}
    
    context = {
        'zip_code': '',
        'radius_miles': 25,
        'market_data': None,
        'error_message': None,
        'filter_options': filter_options
    }
    
    if request.method == 'POST':
        zip_code = request.POST.get('zip_code', '').strip()
        radius_miles = float(request.POST.get('radius_miles', 25))
        
        # Extract filter parameters
        filters = {}
        if request.POST.get('billing_class'):
            filters['billing_class'] = request.POST.get('billing_class')
        if request.POST.get('negotiation_arrangement'):
            filters['negotiation_arrangement'] = request.POST.get('negotiation_arrangement')
        if request.POST.get('negotiated_type'):
            filters['negotiated_type'] = request.POST.get('negotiated_type')
        if request.POST.get('proc_set'):
            filters['proc_set'] = request.POST.get('proc_set')
        if request.POST.get('proc_class'):
            filters['proc_class'] = request.POST.get('proc_class')
        if request.POST.get('proc_group'):
            filters['proc_group'] = request.POST.get('proc_group')
        if request.POST.get('billing_code'):
            filters['billing_code'] = request.POST.get('billing_code')
        
        if zip_code:
            try:
                from .utils.market_analyzer import MarketAnalyzer
                
                analyzer = MarketAnalyzer()
                market_data = analyzer.analyze_market_with_s3_data(zip_code, radius_miles, filters)
                
                if 'error' in market_data:
                    context['error_message'] = market_data['error']
                else:
                    context['market_data'] = market_data
                    context['zip_code'] = zip_code
                    context['radius_miles'] = radius_miles
                    context['filters'] = filters
                    
                    # Generate statistics
                    context['market_stats'] = analyzer.get_market_statistics(market_data)
                    
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"Error in market analyzer: {str(e)}")
                logger.error(f"Full traceback: {error_details}")
                context['error_message'] = f'An error occurred while analyzing the market: {str(e)}'
        else:
            context['error_message'] = 'Please enter a valid zip code.'
    
    return render(request, 'core/rate_analyzer.html', context)


@login_required
def tile_analyzer(request):
    """
    Tile Analyzer view for parquet file analysis with metrics
    """
    from .utils.partition_navigator import PartitionNavigator
    from pathlib import Path
    from botocore.exceptions import ClientError
    
    s3_prefix = request.GET.get('s3_prefix', '')
    
    context = {
        's3_prefix': s3_prefix,
        'tin_value': request.GET.get('tin_value', ''),
        'payer_slug': request.GET.get('payer_slug', ''),
        'billing_class': request.GET.get('billing_class', ''),
        'proc_set': request.GET.get('proc_set', ''),
        'proc_class': request.GET.get('proc_class', ''),
        'proc_group': request.GET.get('proc_group', ''),
        'metrics': None,
        'error_message': None,
    }
    
    # Load metrics if s3_prefix is provided
    if s3_prefix:
        try:
            db_path = Path(__file__).resolve().parent / 'data' / 'partition_navigation.db'
            navigator = PartitionNavigator(db_path=str(db_path))
            
            # Parse S3 prefix
            if s3_prefix.startswith('s3://'):
                s3_prefix_clean = s3_prefix[5:]
            else:
                s3_prefix_clean = s3_prefix
            
            bucket_name, prefix = s3_prefix_clean.split('/', 1)
            
            # List parquet files
            s3_client = navigator.connect_s3()
            parquet_files = []
            
            if not prefix.endswith('/'):
                prefix += '/'
            
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.parquet'):
                        parquet_files.append(f"s3://{bucket_name}/{key}")
            
            if parquet_files:
                # Fetch data
                df = navigator.combine_partitions_for_analysis(
                    partition_paths=parquet_files,
                    max_rows=100000
                )
                
                if df is not None and not df.empty:
                    # Calculate metrics with actual distinct values
                    metrics = {}
                    
                    # Simple distinct values for scalar fields
                    scalar_fields = [
                        'payer_slug', 'tin_value', 'network_pattern_id', 
                        'negotiation_arrangement', 'negotiated_type', 'reporting_entity_type',
                        'tin_org_name', 'tin_state', 'tin_zip', 'tin_city', 'tin_address',
                        'proc_set', 'proc_group', 'proc_class'
                    ]
                    
                    for field in scalar_fields:
                        if field in df.columns:
                            unique_values = df[field].dropna().unique().tolist()
                            metrics[field] = {
                                'count': len(unique_values),
                                'values': sorted([str(v) for v in unique_values])[:100]  # Limit to 100 values
                            }
                        else:
                            metrics[field] = {'count': 0, 'values': []}
                    
                    # NPI array field
                    if 'npi' in df.columns:
                        try:
                            exploded = df['npi'].explode()
                            unique_npis = exploded.dropna().unique().tolist()
                            metrics['npi'] = {
                                'count': len(unique_npis),
                                'values': sorted([str(v) for v in unique_npis])[:100]
                            }
                        except:
                            metrics['npi'] = {'count': 0, 'values': []}
                    else:
                        metrics['npi'] = {'count': 0, 'values': []}
                    
                    # Taxonomy codes - cross-reference with dim_taxonomy
                    if 'primary_taxonomy_code' in df.columns:
                        try:
                            # Get unique taxonomy codes
                            exploded = df['primary_taxonomy_code'].explode()
                            unique_codes = exploded.dropna().unique().tolist()
                            
                            # Load dim_taxonomy to get display names
                            import sqlite3
                            dims_db_path = Path(__file__).resolve().parent / 'data' / 'dims.sqlite'
                            conn = sqlite3.connect(str(dims_db_path))
                            cursor = conn.cursor()
                            
                            # Build query to get display names
                            placeholders = ','.join(['?' for _ in unique_codes])
                            query = f'''
                                SELECT Code, "Display Name" 
                                FROM dim_taxonomy 
                                WHERE Code IN ({placeholders})
                            '''
                            cursor.execute(query, unique_codes)
                            taxonomy_map = {row[0]: row[1] for row in cursor.fetchall()}
                            conn.close()
                            
                            # Create list of display names with codes
                            taxonomy_display = []
                            for code in sorted(unique_codes):
                                display_name = taxonomy_map.get(code, code)  # Fallback to code if not found
                                taxonomy_display.append({
                                    'code': code,
                                    'display_name': display_name
                                })
                            
                            metrics['primary_taxonomy_code'] = {
                                'count': len(unique_codes),
                                'values': taxonomy_display[:100]  # Limit to 100
                            }
                        except Exception as e:
                            logger.error(f"Error processing taxonomy codes: {e}")
                            metrics['primary_taxonomy_code'] = {'count': 0, 'values': []}
                    else:
                        metrics['primary_taxonomy_code'] = {'count': 0, 'values': []}
                    
                    # Add total rows
                    metrics['total_rows'] = len(df)
                    metrics['max_rows_limit'] = 100000
                    
                    # Prepare chart data grouped by network_pattern_id
                    chart_data = {}
                    if 'network_pattern_id' in df.columns and 'billing_code' in df.columns:
                        import sqlite3
                        import pandas as pd
                        import json
                        dims_db_path = Path(__file__).resolve().parent / 'data' / 'dims.sqlite'
                        
                        # Get distinct network patterns
                        network_patterns = sorted(df['network_pattern_id'].dropna().unique())
                        
                        for network_id in network_patterns:
                            network_df = df[df['network_pattern_id'] == network_id].copy()
                            
                            # Get top 5 billing codes by frequency
                            top_codes = network_df['billing_code'].value_counts().head(5).index.tolist()
                            
                            # Get all unique billing codes for dropdown
                            all_codes = sorted(network_df['billing_code'].dropna().unique().tolist())
                            
                            # Prepare data for each billing code
                            code_data = []
                            for code in top_codes:
                                code_df = network_df[network_df['billing_code'] == code]
                                
                                # Get the name field (first non-null value for this code)
                                code_name = None
                                if 'name' in code_df.columns:
                                    code_name = code_df['name'].dropna().iloc[0] if len(code_df['name'].dropna()) > 0 else None
                                
                                # Group by taxonomy to find rate variations
                                if 'primary_taxonomy_code' in code_df.columns:
                                    # Convert array to hashable tuple for grouping
                                    code_df_copy = code_df.copy()
                                    code_df_copy['taxonomy_key'] = code_df_copy['primary_taxonomy_code'].apply(
                                        lambda x: tuple(sorted(x)) if isinstance(x, (list, pd.Series)) else (tuple(x) if hasattr(x, '__iter__') and not isinstance(x, str) else (x,))
                                    )
                                    
                                    # Get unique taxonomy combinations and their rates
                                    taxonomy_groups = code_df_copy.groupby('taxonomy_key').agg({
                                        'negotiated_rate': 'mean',
                                        'prof_medicare': lambda x: x.dropna().mean() if x.notna().any() else None,
                                        'asc_medicare': lambda x: x.dropna().mean() if x.notna().any() else None,
                                        'opps_medicare': lambda x: x.dropna().mean() if x.notna().any() else None,
                                    }).reset_index()
                                    
                                    # Get taxonomy display names from the tuple keys
                                    all_taxonomy_codes = set()
                                    for taxonomy_tuple in taxonomy_groups['taxonomy_key']:
                                        if taxonomy_tuple and taxonomy_tuple != (None,):
                                            all_taxonomy_codes.update(taxonomy_tuple)
                                    
                                    taxonomy_codes = list(all_taxonomy_codes)
                                    if taxonomy_codes:
                                        conn = sqlite3.connect(str(dims_db_path))
                                        cursor = conn.cursor()
                                        placeholders = ','.join(['?' for _ in taxonomy_codes])
                                        query = f'SELECT Code, "Display Name" FROM dim_taxonomy WHERE Code IN ({placeholders})'
                                        cursor.execute(query, taxonomy_codes)
                                        taxonomy_map = {row[0]: row[1] for row in cursor.fetchall()}
                                        conn.close()
                                    else:
                                        taxonomy_map = {}
                                    
                                    # Convert to list of dicts
                                    for _, row in taxonomy_groups.iterrows():
                                        taxonomy_tuple = row['taxonomy_key']
                                        
                                        # Create display name from tuple of taxonomy codes
                                        if taxonomy_tuple and taxonomy_tuple != (None,):
                                            display_parts = [taxonomy_map.get(code, code) for code in taxonomy_tuple if code]
                                            taxonomy_display = ', '.join(display_parts[:3])  # Limit to 3 for readability
                                            if len(display_parts) > 3:
                                                taxonomy_display += f' +{len(display_parts)-3} more'
                                            taxonomy_code_str = ', '.join(str(c) for c in taxonomy_tuple if c)
                                        else:
                                            taxonomy_display = 'Unknown'
                                            taxonomy_code_str = None
                                        
                                        code_data.append({
                                            'billing_code': code,
                                            'name': code_name,
                                            'taxonomy_code': taxonomy_code_str,
                                            'taxonomy_display': taxonomy_display,
                                            'negotiated_rate': float(row['negotiated_rate']) if pd.notna(row['negotiated_rate']) else None,
                                            'prof_medicare': float(row['prof_medicare']) if pd.notna(row['prof_medicare']) else None,
                                            'asc_medicare': float(row['asc_medicare']) if pd.notna(row['asc_medicare']) else None,
                                            'opps_medicare': float(row['opps_medicare']) if pd.notna(row['opps_medicare']) else None,
                                        })
                                else:
                                    # No taxonomy differentiation
                                    avg_rates = code_df.agg({
                                        'negotiated_rate': 'mean',
                                        'prof_medicare': lambda x: x.dropna().mean() if x.notna().any() else None,
                                        'asc_medicare': lambda x: x.dropna().mean() if x.notna().any() else None,
                                        'opps_medicare': lambda x: x.dropna().mean() if x.notna().any() else None,
                                    })
                                    
                                    code_data.append({
                                        'billing_code': code,
                                        'name': code_name,
                                        'taxonomy_code': None,
                                        'taxonomy_display': 'All Taxonomies',
                                        'negotiated_rate': float(avg_rates['negotiated_rate']) if pd.notna(avg_rates['negotiated_rate']) else None,
                                        'prof_medicare': float(avg_rates['prof_medicare']) if pd.notna(avg_rates['prof_medicare']) else None,
                                        'asc_medicare': float(avg_rates['asc_medicare']) if pd.notna(avg_rates['asc_medicare']) else None,
                                        'opps_medicare': float(avg_rates['opps_medicare']) if pd.notna(avg_rates['opps_medicare']) else None,
                                    })
                            
                            chart_data[str(network_id)] = {
                                'network_id': str(network_id),
                                'top_codes': top_codes,
                                'all_codes': all_codes,
                                'data': code_data
                            }
                    
                    # Pass chart_data directly (Django templates can handle Python dicts/lists)
                    context['metrics'] = metrics
                    context['chart_data'] = chart_data
                    context['billing_class'] = request.GET.get('billing_class', '')
                    logger.info(f"Calculated metrics for tile: analyzed {len(df)} rows with {len(chart_data)} network patterns")
                    
        except Exception as e:
            logger.error(f"Error loading tile metrics: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            context['error_message'] = f"Error loading metrics: {str(e)}"
    
    return render(request, 'core/tile_analyzer.html', context)


@login_required
def tile_analyzer_download_csv(request):
    """
    Download tile data as CSV from S3 partition
    """
    import csv
    import io
    from django.http import HttpResponse
    from .utils.partition_navigator import PartitionNavigator
    from pathlib import Path
    import boto3
    from botocore.exceptions import ClientError
    
    s3_prefix = request.GET.get('s3_prefix', '')
    
    if not s3_prefix:
        return HttpResponse("No S3 prefix provided", status=400)
    
    try:
        # Initialize PartitionNavigator
        db_path = Path(__file__).resolve().parent / 'data' / 'partition_navigation.db'
        navigator = PartitionNavigator(db_path=str(db_path))
        
        # Parse S3 prefix to get bucket and prefix
        if s3_prefix.startswith('s3://'):
            s3_prefix_clean = s3_prefix[5:]
        else:
            s3_prefix_clean = s3_prefix
        
        bucket_name, prefix = s3_prefix_clean.split('/', 1)
        
        # List all parquet files in the S3 prefix
        logger.info(f"Listing parquet files in S3 prefix: {s3_prefix}")
        s3_client = navigator.connect_s3()
        
        parquet_files = []
        try:
            # Ensure prefix ends with /
            if not prefix.endswith('/'):
                prefix += '/'
            
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.parquet'):
                        parquet_files.append(f"s3://{bucket_name}/{key}")
                        logger.info(f"Found parquet file: {key}")
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            return HttpResponse(f"Error accessing S3: {str(e)}", status=500)
        
        if not parquet_files:
            logger.warning(f"No parquet files found in {s3_prefix}")
            return HttpResponse("No parquet files found in the specified partition", status=404)
        
        # Fetch data from S3 partition files
        logger.info(f"Fetching data from {len(parquet_files)} parquet file(s)")
        df = navigator.combine_partitions_for_analysis(
            partition_paths=parquet_files,
            max_rows=100000  # Limit to 100k rows for performance
        )
        
        if df is None or df.empty:
            return HttpResponse("No data found in partition", status=404)
        
        # Remove internal metadata columns
        metadata_cols = [col for col in df.columns if col.startswith('_')]
        if metadata_cols:
            df = df.drop(columns=metadata_cols)
        
        # Create CSV response
        response = HttpResponse(content_type='text/csv')
        
        # Generate filename from partition parameters
        tin_value = request.GET.get('tin_value', 'unknown')
        payer_slug = request.GET.get('payer_slug', 'unknown')
        proc_group = request.GET.get('proc_group', 'unknown')
        
        filename = f"tile_data_{payer_slug}_{tin_value}_{proc_group}.csv"
        filename = filename.replace(' ', '_').replace('/', '-')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        
        # Write CSV data
        df.to_csv(response, index=False)
        
        logger.info(f"Successfully downloaded {len(df)} rows as CSV")
        return response
        
    except Exception as e:
        logger.error(f"Error downloading tile data as CSV: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return HttpResponse(f"Error downloading data: {str(e)}", status=500)


