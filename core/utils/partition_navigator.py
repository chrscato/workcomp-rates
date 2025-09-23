import sqlite3
import pandas as pd
import boto3
import io
from typing import Dict, List, Optional, Any
import logging
from pathlib import Path
from django.conf import settings
import time
import numpy as np

logger = logging.getLogger(__name__)

# Import Medicare benchmark lookup
try:
    from .medicare_benchmarks import MedicareBenchmarkLookup
except ImportError:
    logger.warning("MedicareBenchmarkLookup not available - benchmark calculations will be skipped")
    MedicareBenchmarkLookup = None

class PartitionNavigator:
    """
    Partition Navigation System for Healthcare Data
    Implements hierarchical filtering and partition discovery for S3-stored parquet files
    """
    
    def __init__(self, db_path: str, s3_bucket: str = None, aws_region: str = None):
        self.db_path = db_path
        # Use Django settings for AWS configuration
        self.s3_bucket = s3_bucket or getattr(settings, 'AWS_S3_BUCKET', 'partitioned-data')
        self.aws_region = aws_region or getattr(settings, 'AWS_DEFAULT_REGION', 'us-east-1')
        self.conn = None
        self.s3_client = None
        
        # Initialize Medicare benchmark lookup
        self.medicare_lookup = None
        if MedicareBenchmarkLookup:
            try:
                self.medicare_lookup = MedicareBenchmarkLookup()
                logger.info("Medicare benchmark lookup initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize Medicare benchmark lookup: {e}")
                self.medicare_lookup = None
        
        # Hierarchical filter configuration
        self.required_filters = ['payer_slug', 'state', 'billing_class']
        self.optional_filters = ['procedure_set', 'taxonomy_code', 'taxonomy_desc', 'stat_area_name']
        self.temporal_filters = ['year', 'month']
    
    def connect_db(self):
        """Connect to navigation database"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def connect_s3(self):
        """Connect to S3 client with proper credentials"""
        if self.s3_client is None and self.s3_bucket:
            try:
                # Use credentials from Django settings
                aws_access_key = getattr(settings, 'AWS_ACCESS_KEY_ID', None)
                aws_secret_key = getattr(settings, 'AWS_SECRET_ACCESS_KEY', None)
                aws_session_token = getattr(settings, 'AWS_SESSION_TOKEN', None)
                
                if aws_access_key and aws_secret_key:
                    # Use explicit credentials
                    session_kwargs = {
                        'aws_access_key_id': aws_access_key,
                        'aws_secret_access_key': aws_secret_key,
                        'region_name': self.aws_region
                    }
                    if aws_session_token:
                        session_kwargs['aws_session_token'] = aws_session_token
                    
                    self.s3_client = boto3.client('s3', **session_kwargs)
                    logger.info("S3 client connected with explicit credentials")
                else:
                    # Use default credential chain (IAM roles, environment variables, etc.)
                    self.s3_client = boto3.client('s3', region_name=self.aws_region)
                    logger.info("S3 client connected using default credential chain")
                    
            except Exception as e:
                logger.error(f"Failed to connect to S3: {e}")
                self.s3_client = None
                
        return self.s3_client
    
    def get_filter_options(self) -> Dict[str, List[str]]:
        """Get all available filter options from dimension tables"""
        conn = self.connect_db()
        
        filter_options = {}
        
        # Get required filter options
        for filter_name in self.required_filters:
            try:
                if filter_name == 'payer_slug':
                    # Get from dim_payers table
                    query = "SELECT DISTINCT payer_slug, payer_display_name FROM dim_payers ORDER BY payer_display_name"
                    df = pd.read_sql_query(query, conn)
                    options = [f"{row['payer_slug']}|{row['payer_display_name']}" for _, row in df.iterrows()]
                elif filter_name == 'state':
                    # Get from partitions table
                    query = "SELECT DISTINCT state FROM partitions WHERE state IS NOT NULL AND state != '' AND state != 'None' ORDER BY state"
                    df = pd.read_sql_query(query, conn)
                    options = df['state'].dropna().astype(str).tolist()
                    # Filter out string "None" values
                    options = [opt for opt in options if opt.strip() and opt.lower() != 'none' and opt != '']
                elif filter_name == 'billing_class':
                    # Get from partitions table
                    query = "SELECT DISTINCT billing_class FROM partitions WHERE billing_class IS NOT NULL AND billing_class != '' AND billing_class != 'None' ORDER BY billing_class"
                    df = pd.read_sql_query(query, conn)
                    options = df['billing_class'].dropna().astype(str).tolist()
                    # Filter out string "None" values
                    options = [opt for opt in options if opt.strip() and opt.lower() != 'none' and opt != '']
                else:
                    # Fallback to dimension table
                    table_name = f"dim_{filter_name.replace('_slug', 's')}"
                    query = f"SELECT DISTINCT {filter_name} FROM {table_name} ORDER BY {filter_name}"
                    df = pd.read_sql_query(query, conn)
                    options = df[filter_name].dropna().tolist()
                    # Filter out string "None" values
                    options = [opt for opt in options if str(opt).strip() and str(opt).lower() != 'none']
                
                filter_options[filter_name] = options
                logger.info(f"Loaded {len(options)} options for {filter_name}")
                
            except Exception as e:
                logger.warning(f"Could not load options for {filter_name}: {e}")
                filter_options[filter_name] = []
        
        # Get optional filter options
        for filter_name in self.optional_filters:
            try:
                if filter_name == 'procedure_set':
                    # Get from partitions table
                    query = "SELECT DISTINCT procedure_set FROM partitions WHERE procedure_set IS NOT NULL AND procedure_set != '' AND procedure_set != 'None' ORDER BY procedure_set"
                    df = pd.read_sql_query(query, conn)
                    options = df['procedure_set'].dropna().astype(str).tolist()
                    # Filter out string "None" values
                    options = [opt for opt in options if opt.strip() and opt.lower() != 'none' and opt != '']
                elif filter_name == 'taxonomy_code':
                    # Get from dim_taxonomies table - return just codes
                    query = "SELECT DISTINCT taxonomy_code FROM dim_taxonomies WHERE taxonomy_code IS NOT NULL AND taxonomy_code != '' AND taxonomy_code != 'None' ORDER BY taxonomy_code"
                    df = pd.read_sql_query(query, conn)
                    options = []
                    for _, row in df.iterrows():
                        code = str(row['taxonomy_code']).strip()
                        if code and code.lower() != 'none' and code != '':
                            options.append(code)
                elif filter_name == 'taxonomy_desc':
                    # Get from dim_taxonomies table - return just descriptions
                    query = "SELECT DISTINCT taxonomy_desc FROM dim_taxonomies WHERE taxonomy_desc IS NOT NULL AND taxonomy_desc != '' AND taxonomy_desc != 'None' ORDER BY taxonomy_desc"
                    df = pd.read_sql_query(query, conn)
                    options = []
                    for _, row in df.iterrows():
                        desc = str(row['taxonomy_desc']).strip()
                        if desc and desc.lower() != 'none' and desc != '':
                            options.append(desc)
                elif filter_name == 'stat_area_name':
                    # Get from partitions table
                    query = "SELECT DISTINCT stat_area_name FROM partitions WHERE stat_area_name IS NOT NULL AND stat_area_name != '' AND stat_area_name != 'None' ORDER BY stat_area_name"
                    df = pd.read_sql_query(query, conn)
                    options = df['stat_area_name'].dropna().astype(str).tolist()
                    # Filter out string "None" values
                    options = [opt for opt in options if opt.strip() and opt.lower() != 'none' and opt != '']
                else:
                    # Fallback to dimension table
                    table_name = f"dim_{filter_name.replace('_code', 'ies').replace('_name', 's')}"
                    query = f"SELECT DISTINCT {filter_name} FROM {table_name} ORDER BY {filter_name}"
                    df = pd.read_sql_query(query, conn)
                    options = df[filter_name].dropna().tolist()
                    # Filter out string "None" values
                    options = [opt for opt in options if str(opt).strip() and str(opt).lower() != 'none']
                
                filter_options[filter_name] = options
                logger.info(f"Loaded {len(options)} options for {filter_name}")
                
            except Exception as e:
                logger.warning(f"Could not load options for {filter_name}: {e}")
                filter_options[filter_name] = []
        
        # Get temporal filter options
        for filter_name in self.temporal_filters:
            try:
                query = f"SELECT DISTINCT {filter_name} FROM partitions WHERE {filter_name} IS NOT NULL AND {filter_name} != '' AND {filter_name} != 'None' ORDER BY {filter_name} DESC"
                df = pd.read_sql_query(query, conn)
                options = df[filter_name].dropna().astype(str).tolist()
                # Filter out string "None" values
                options = [opt for opt in options if opt.strip() and opt.lower() != 'none' and opt != '']
                filter_options[filter_name] = options
                logger.info(f"Loaded {len(options)} options for {filter_name}")
            except Exception as e:
                logger.warning(f"Could not load options for {filter_name}: {e}")
                filter_options[filter_name] = []
        
        return filter_options
    
    def search_partitions(self, filters: Dict[str, Any], require_top_levels: bool = True) -> pd.DataFrame:
        """Search partitions with hierarchical requirements"""
        conn = self.connect_db()
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        # Apply required filters
        for filter_key in self.required_filters:
            if filters.get(filter_key):
                # Handle multiple values for payer_slug
                if filter_key == 'payer_slug' and isinstance(filters[filter_key], list):
                    if len(filters[filter_key]) > 0:
                        placeholders = ','.join(['?' for _ in filters[filter_key]])
                        where_conditions.append(f"p.{filter_key} IN ({placeholders})")
                        params.extend(filters[filter_key])
                    elif require_top_levels:
                        return pd.DataFrame()  # Return empty if required filters missing
                else:
                    where_conditions.append(f"p.{filter_key} = ?")
                    params.append(filters[filter_key])
            elif require_top_levels:
                return pd.DataFrame()  # Return empty if required filters missing
        
        # Apply optional filters
        for filter_key in self.optional_filters:
            if filters.get(filter_key):
                where_conditions.append(f"p.{filter_key} = ?")
                params.append(filters[filter_key])
        
        # Apply temporal filters
        for filter_key in self.temporal_filters:
            if filters.get(filter_key):
                where_conditions.append(f"p.{filter_key} = ?")
                params.append(filters[filter_key])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT p.*, dp.payer_display_name
            FROM partitions p
            LEFT JOIN dim_payers dp ON p.payer_slug = dp.payer_slug
            {where_clause}
            ORDER BY p.file_size_mb DESC
            LIMIT 1000
        """
        
        return pd.read_sql_query(query, conn, params=params)
    
    def _extract_zip_code_5dig(self, matched_address: str) -> Optional[str]:
        """Extract 5-digit ZIP code from matched_address field."""
        if pd.isna(matched_address) or not matched_address:
            return None
        
        # Convert to string and clean
        address_str = str(matched_address).strip()
        if not address_str:
            return None
        
        # Extract first 5 digits from the string
        import re
        zip_match = re.search(r'\b(\d{5})\b', address_str)
        if zip_match:
            return zip_match.group(1)
        
        # Fallback: try to extract first 5 consecutive digits
        digits = re.findall(r'\d', address_str)
        if len(digits) >= 5:
            return ''.join(digits[:5])
        
        return None
    
    def _convert_numpy_types(self, obj):
        """Convert numpy types to native Python types for JSON serialization."""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {key: self._convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_numpy_types(item) for item in obj]
        return obj
    
    def _add_benchmark_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Medicare benchmark columns to the DataFrame."""
        if not self.medicare_lookup or df.empty:
            return df
        
        logger.info(f"Adding Medicare benchmark columns to {len(df)} records")
        start_time = time.time()
        
        # Initialize new columns with None
        df['zip_code_5dig'] = None
        df['medicare_professional_rate'] = None
        df['medicare_asc_stateavg'] = None
        df['medicare_opps_stateavg'] = None
        df['negotiated_rate_pct_of_medicare_professional'] = None
        df['negotiated_rate_pct_of_medicare_asc'] = None
        df['negotiated_rate_pct_of_medicare_opps'] = None
        
        # Extract ZIP codes for all records
        if 'matched_address' in df.columns:
            df['zip_code_5dig'] = df['matched_address'].apply(self._extract_zip_code_5dig)
        
        # Get unique combinations for batch processing
        unique_combinations = set()
        
        # Professional rate combinations (code + zip)
        if 'code' in df.columns and 'zip_code_5dig' in df.columns:
            prof_combinations = df[['code', 'zip_code_5dig']].dropna()
            prof_combinations = prof_combinations.drop_duplicates()
            for _, row in prof_combinations.iterrows():
                if row['zip_code_5dig']:
                    unique_combinations.add(('prof', row['code'], row['zip_code_5dig']))
        
        # Institutional rate combinations (code + state)
        if 'code' in df.columns and 'state' in df.columns:
            inst_combinations = df[['code', 'state']].dropna()
            inst_combinations = inst_combinations.drop_duplicates()
            for _, row in inst_combinations.iterrows():
                if row['state']:
                    unique_combinations.add(('inst', row['code'], row['state']))
        
        logger.info(f"Processing {len(unique_combinations)} unique combinations for benchmark lookups")
        
        # Batch process professional rates
        prof_rates_cache = {}
        inst_rates_cache = {}
        
        for combo_type, code, location in unique_combinations:
            try:
                if combo_type == 'prof':
                    rate = self.medicare_lookup.get_professional_rate(code, location)
                    prof_rates_cache[(code, location)] = rate
                elif combo_type == 'inst':
                    rates = self.medicare_lookup.get_institutional_rates(code, location)
                    inst_rates_cache[(code, location)] = rates
            except Exception as e:
                logger.warning(f"Error getting benchmark rates for {code} in {location}: {e}")
                if combo_type == 'prof':
                    prof_rates_cache[(code, location)] = None
                elif combo_type == 'inst':
                    inst_rates_cache[(code, location)] = {'medicare_asc_stateavg': None, 'medicare_opps_stateavg': None}
        
        # Apply cached rates to DataFrame
        def get_prof_rate(row):
            if pd.isna(row['code']) or pd.isna(row['zip_code_5dig']):
                return None
            return prof_rates_cache.get((row['code'], row['zip_code_5dig']), None)
        
        def get_inst_rates(row):
            if pd.isna(row['code']) or pd.isna(row['state']):
                return {'medicare_asc_stateavg': None, 'medicare_opps_stateavg': None}
            return inst_rates_cache.get((row['code'], row['state']), {'medicare_asc_stateavg': None, 'medicare_opps_stateavg': None})
        
        # Apply professional rates
        df['medicare_professional_rate'] = df.apply(get_prof_rate, axis=1)
        
        # Apply institutional rates
        inst_rates_series = df.apply(get_inst_rates, axis=1)
        df['medicare_asc_stateavg'] = inst_rates_series.apply(lambda x: x['medicare_asc_stateavg'] if x else None)
        df['medicare_opps_stateavg'] = inst_rates_series.apply(lambda x: x['medicare_opps_stateavg'] if x else None)
        
        # Calculate benchmark percentages
        def calculate_prof_pct(row):
            if pd.isna(row['negotiated_rate']) or pd.isna(row['medicare_professional_rate']):
                return None
            return self.medicare_lookup.calculate_benchmark_percentage(
                float(row['negotiated_rate']), 
                float(row['medicare_professional_rate'])
            )
        
        def calculate_asc_pct(row):
            if pd.isna(row['negotiated_rate']) or pd.isna(row['medicare_asc_stateavg']):
                return None
            return self.medicare_lookup.calculate_benchmark_percentage(
                float(row['negotiated_rate']), 
                float(row['medicare_asc_stateavg'])
            )
        
        def calculate_opps_pct(row):
            if pd.isna(row['negotiated_rate']) or pd.isna(row['medicare_opps_stateavg']):
                return None
            return self.medicare_lookup.calculate_benchmark_percentage(
                float(row['negotiated_rate']), 
                float(row['medicare_opps_stateavg'])
            )
        
        # Apply percentage calculations based on billing class
        if 'billing_class' in df.columns:
            # Professional billing class - only calculate professional Medicare percentages
            prof_mask = df['billing_class'] == 'professional'
            df.loc[prof_mask, 'negotiated_rate_pct_of_medicare_professional'] = df[prof_mask].apply(calculate_prof_pct, axis=1)
            
            # Institutional billing class - only calculate institutional Medicare percentages
            inst_mask = df['billing_class'] == 'institutional'
            df.loc[inst_mask, 'negotiated_rate_pct_of_medicare_asc'] = df[inst_mask].apply(calculate_asc_pct, axis=1)
            df.loc[inst_mask, 'negotiated_rate_pct_of_medicare_opps'] = df[inst_mask].apply(calculate_opps_pct, axis=1)
            
            # Edge case: calculate both for records where billing_class is unclear/other
            other_mask = ~prof_mask & ~inst_mask
            if other_mask.any():
                logger.info(f"Found {other_mask.sum()} records with unclear billing_class - calculating both professional and institutional percentages")
                df.loc[other_mask, 'negotiated_rate_pct_of_medicare_professional'] = df[other_mask].apply(calculate_prof_pct, axis=1)
                df.loc[other_mask, 'negotiated_rate_pct_of_medicare_asc'] = df[other_mask].apply(calculate_asc_pct, axis=1)
                df.loc[other_mask, 'negotiated_rate_pct_of_medicare_opps'] = df[other_mask].apply(calculate_opps_pct, axis=1)
        else:
            # If no billing_class column, calculate all percentages (edge case)
            logger.info("No billing_class column found - calculating all Medicare percentages")
            df['negotiated_rate_pct_of_medicare_professional'] = df.apply(calculate_prof_pct, axis=1)
            df['negotiated_rate_pct_of_medicare_asc'] = df.apply(calculate_asc_pct, axis=1)
            df['negotiated_rate_pct_of_medicare_opps'] = df.apply(calculate_opps_pct, axis=1)
        
        # Convert numeric columns to proper types
        numeric_cols = [
            'medicare_professional_rate', 'medicare_asc_stateavg', 'medicare_opps_stateavg',
            'negotiated_rate_pct_of_medicare_professional', 'negotiated_rate_pct_of_medicare_asc', 
            'negotiated_rate_pct_of_medicare_opps'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Log benchmark statistics
        benchmark_stats = {
            'total_records': len(df),
            'records_with_zip': df['zip_code_5dig'].notna().sum(),
            'records_with_prof_rates': df['medicare_professional_rate'].notna().sum(),
            'records_with_asc_rates': df['medicare_asc_stateavg'].notna().sum(),
            'records_with_opps_rates': df['medicare_opps_stateavg'].notna().sum(),
            'records_with_prof_pct': df['negotiated_rate_pct_of_medicare_professional'].notna().sum(),
            'records_with_asc_pct': df['negotiated_rate_pct_of_medicare_asc'].notna().sum(),
            'records_with_opps_pct': df['negotiated_rate_pct_of_medicare_opps'].notna().sum(),
            'processing_time_seconds': time.time() - start_time
        }
        
        logger.info(f"Benchmark calculations completed: {benchmark_stats}")
        
        return df, benchmark_stats
    
    def combine_partitions_for_analysis(self, partition_paths: List[str], max_rows: int = 10000, progress_callback=None, columns=None) -> Optional[pd.DataFrame]:
        """Combine multiple partitions in memory for analysis with progress tracking"""
        if not partition_paths:
            return None
        
        try:
            s3_client = self.connect_s3()
            if not s3_client:
                logger.error("S3 client not available")
                return None
            
            combined_dfs = []
            total_rows = 0
            successful_loads = 0
            failed_loads = 0
            start_time = time.time()
            
            logger.info(f"Starting to combine {len(partition_paths)} partitions (max_rows: {max_rows})")
            
            for i, s3_path in enumerate(partition_paths):
                if total_rows >= max_rows:
                    logger.info(f"Reached max_rows limit ({max_rows}), stopping at partition {i+1}/{len(partition_paths)}")
                    break
                
                try:
                    # Parse S3 path
                    if s3_path.startswith('s3://'):
                        s3_path = s3_path[5:]
                    
                    bucket, key = s3_path.split('/', 1)
                    
                    # Read parquet file from S3
                    logger.debug(f"Loading partition {i+1}/{len(partition_paths)}: {key}")
                    response = s3_client.get_object(Bucket=bucket, Key=key)
                    parquet_data = response['Body'].read()
                    
                    # Convert to DataFrame (only load specified columns if provided)
                    if columns:
                        df = pd.read_parquet(io.BytesIO(parquet_data), columns=columns)
                    else:
                        df = pd.read_parquet(io.BytesIO(parquet_data))
                    
                    # Add partition metadata
                    df['_partition_source'] = s3_path
                    df['_partition_index'] = i
                    df['_load_timestamp'] = time.time()
                    
                    combined_dfs.append(df)
                    total_rows += len(df)
                    successful_loads += 1
                    
                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback(i + 1, len(partition_paths), total_rows, successful_loads, failed_loads)
                    
                    logger.debug(f"Loaded partition {i+1}: {len(df)} rows (total: {total_rows})")
                    
                except Exception as e:
                    logger.error(f"Error loading partition {s3_path}: {e}")
                    failed_loads += 1
                    continue
            
            if combined_dfs:
                logger.info(f"Combining {len(combined_dfs)} DataFrames...")
                combined_df = pd.concat(combined_dfs, ignore_index=True)
                
                # Add Medicare benchmark calculations
                benchmark_stats = {}
                if self.medicare_lookup:
                    try:
                        logger.info("Adding Medicare benchmark calculations...")
                        combined_df, benchmark_stats = self._add_benchmark_columns(combined_df)
                    except Exception as e:
                        logger.error(f"Error adding benchmark calculations: {e}")
                        benchmark_stats = {'error': str(e)}
                else:
                    logger.info("Medicare benchmark lookup not available - skipping benchmark calculations")
                
                # Add summary metadata including benchmark statistics
                load_summary = {
                    'total_partitions': len(partition_paths),
                    'successful_loads': successful_loads,
                    'failed_loads': failed_loads,
                    'total_rows': len(combined_df),
                    'load_time_seconds': time.time() - start_time,
                    'columns': list(combined_df.columns),
                    'benchmark_stats': benchmark_stats
                }
                
                combined_df.attrs['_load_summary'] = load_summary
                
                logger.info(f"Successfully combined {len(combined_df)} rows from {successful_loads} partitions in {time.time() - start_time:.2f}s")
                if benchmark_stats:
                    logger.info(f"Benchmark calculations: {benchmark_stats}")
                
                return combined_df
            else:
                logger.warning("No partitions were successfully loaded")
                return None
                
        except Exception as e:
            logger.error(f"Error combining partitions: {e}")
            return None
    
    def get_partition_summary(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Get summary statistics for matching partitions"""
        results_df = self.search_partitions(filters, require_top_levels=False)
        
        if results_df.empty:
            return {
                'partition_count': 0,
                'total_size_mb': 0,
                'total_estimated_records': 0,
                'available_filters': {},
                'payer_breakdown': []
            }
        
        # Calculate summary statistics
        total_size = results_df['file_size_mb'].sum()
        total_records = results_df['estimated_records'].sum()
        
        # Get available filter options based on current results
        available_filters = {}
        for col in ['procedure_set', 'taxonomy_code', 'stat_area_name', 'year', 'month']:
            if col in results_df.columns:
                available_filters[col] = results_df[col].dropna().unique().tolist()
        
        # Get payer breakdown if payer_slug filters are applied
        payer_breakdown = []
        if filters.get('payer_slug') and 'payer_slug' in results_df.columns:
            payer_breakdown = self._get_payer_breakdown(results_df, filters['payer_slug'])
        
        return {
            'partition_count': len(results_df),
            'total_size_mb': round(total_size, 2),
            'total_estimated_records': int(total_records),
            'available_filters': available_filters,
            'partitions': results_df.to_dict('records'),
            'payer_breakdown': payer_breakdown
        }
    
    def get_data_availability_metrics(self) -> Dict[str, Any]:
        """Get comprehensive data availability metrics from the partition navigation database"""
        try:
            # Connect to database
            self.connect_db()
            
            # Get overall summary statistics
            summary_query = """
                SELECT 
                    COUNT(*) as partition_count,
                    SUM(file_size_mb) as total_size_mb,
                    SUM(estimated_records) as total_estimated_records,
                    COUNT(DISTINCT payer_slug) as unique_payers,
                    COUNT(DISTINCT state) as unique_states,
                    COUNT(DISTINCT billing_class) as unique_billing_classes,
                    COUNT(DISTINCT procedure_set) as unique_procedure_sets,
                    COUNT(DISTINCT taxonomy_code) as unique_taxonomies,
                    COUNT(DISTINCT stat_area_name) as unique_stat_areas,
                    COUNT(DISTINCT year) as unique_years,
                    COUNT(DISTINCT month) as unique_months
                FROM partitions
            """
            
            summary_df = pd.read_sql_query(summary_query, self.conn)
            summary_stats = summary_df.iloc[0].to_dict()
            
            # Get payer breakdown
            payer_query = """
                SELECT 
                    p.payer_slug,
                    dp.payer_display_name,
                    COUNT(*) as partition_count,
                    SUM(p.file_size_mb) as total_size_mb,
                    SUM(p.estimated_records) as total_estimated_records
                FROM partitions p
                LEFT JOIN dim_payers dp ON p.payer_slug = dp.payer_slug
                GROUP BY p.payer_slug, dp.payer_display_name
                ORDER BY total_estimated_records DESC
            """
            
            payer_df = pd.read_sql_query(payer_query, self.conn)
            payer_breakdown = payer_df.to_dict('records')
            
            # Get state breakdown (top 20)
            state_query = """
                SELECT 
                    state,
                    COUNT(*) as partition_count,
                    SUM(file_size_mb) as total_size_mb,
                    SUM(estimated_records) as total_estimated_records
                FROM partitions
                WHERE state IS NOT NULL
                GROUP BY state
                ORDER BY total_estimated_records DESC
                LIMIT 20
            """
            
            state_df = pd.read_sql_query(state_query, self.conn)
            state_breakdown = state_df.to_dict('records')
            
            # Get billing class breakdown
            billing_class_query = """
                SELECT 
                    billing_class,
                    COUNT(*) as partition_count,
                    SUM(file_size_mb) as total_size_mb,
                    SUM(estimated_records) as total_estimated_records
                FROM partitions
                WHERE billing_class IS NOT NULL
                GROUP BY billing_class
                ORDER BY total_estimated_records DESC
            """
            
            billing_class_df = pd.read_sql_query(billing_class_query, self.conn)
            billing_class_breakdown = billing_class_df.to_dict('records')
            
            # Get procedure set breakdown
            procedure_set_query = """
                SELECT 
                    procedure_set,
                    COUNT(*) as partition_count,
                    SUM(file_size_mb) as total_size_mb,
                    SUM(estimated_records) as total_estimated_records
                FROM partitions
                WHERE procedure_set IS NOT NULL
                GROUP BY procedure_set
                ORDER BY total_estimated_records DESC
            """
            
            procedure_set_df = pd.read_sql_query(procedure_set_query, self.conn)
            procedure_set_breakdown = procedure_set_df.to_dict('records')
            
            # Get taxonomy breakdown (top 20)
            taxonomy_query = """
                SELECT 
                    taxonomy_code,
                    taxonomy_desc,
                    COUNT(*) as partition_count,
                    SUM(file_size_mb) as total_size_mb,
                    SUM(estimated_records) as total_estimated_records
                FROM partitions
                WHERE taxonomy_code IS NOT NULL
                GROUP BY taxonomy_code, taxonomy_desc
                ORDER BY total_estimated_records DESC
                LIMIT 20
            """
            
            taxonomy_df = pd.read_sql_query(taxonomy_query, self.conn)
            taxonomy_breakdown = taxonomy_df.to_dict('records')
            
            # Get time period breakdown
            time_period_query = """
                SELECT 
                    year,
                    month,
                    COUNT(*) as partition_count,
                    SUM(file_size_mb) as total_size_mb,
                    SUM(estimated_records) as total_estimated_records
                FROM partitions
                WHERE year IS NOT NULL AND month IS NOT NULL
                GROUP BY year, month
                ORDER BY year DESC, month DESC
            """
            
            time_period_df = pd.read_sql_query(time_period_query, self.conn)
            time_period_breakdown = time_period_df.to_dict('records')
            
            # Get statistical area breakdown (top 20)
            stat_area_query = """
                SELECT 
                    stat_area_name,
                    COUNT(*) as partition_count,
                    SUM(file_size_mb) as total_size_mb,
                    SUM(estimated_records) as total_estimated_records
                FROM partitions
                WHERE stat_area_name IS NOT NULL
                GROUP BY stat_area_name
                ORDER BY total_estimated_records DESC
                LIMIT 20
            """
            
            stat_area_df = pd.read_sql_query(stat_area_query, self.conn)
            stat_area_breakdown = stat_area_df.to_dict('records')
            
            return {
                'summary_stats': summary_stats,
                'payer_breakdown': payer_breakdown,
                'state_breakdown': state_breakdown,
                'billing_class_breakdown': billing_class_breakdown,
                'procedure_set_breakdown': procedure_set_breakdown,
                'taxonomy_breakdown': taxonomy_breakdown,
                'time_period_breakdown': time_period_breakdown,
                'stat_area_breakdown': stat_area_breakdown
            }
            
        except Exception as e:
            logger.error(f"Error getting data availability metrics: {e}")
            return {
                'summary_stats': {},
                'payer_breakdown': [],
                'state_breakdown': [],
                'billing_class_breakdown': [],
                'procedure_set_breakdown': [],
                'taxonomy_breakdown': [],
                'time_period_breakdown': [],
                'stat_area_breakdown': []
            }

    def _get_payer_breakdown(self, results_df: pd.DataFrame, selected_payer_slugs: List[str]) -> List[Dict[str, Any]]:
        """Get breakdown of records by selected payer slugs"""
        if results_df.empty or not selected_payer_slugs:
            return []
        
        # Ensure we have the required columns
        if 'payer_slug' not in results_df.columns or 'estimated_records' not in results_df.columns:
            return []
        
        # Filter to only selected payer slugs
        filtered_df = results_df[results_df['payer_slug'].isin(selected_payer_slugs)]
        
        if filtered_df.empty:
            return []
        
        # Group by payer_slug and calculate statistics
        payer_stats = filtered_df.groupby('payer_slug').agg({
            'estimated_records': 'sum',
            'file_size_mb': 'sum',
            'payer_display_name': 'first'  # Get the display name
        }).reset_index()
        
        # Sort by estimated records descending
        payer_stats = payer_stats.sort_values('estimated_records', ascending=False)
        
        # Convert to list of dictionaries
        breakdown = []
        for _, row in payer_stats.iterrows():
            breakdown.append({
                'payer_slug': row['payer_slug'],
                'payer_display_name': row['payer_display_name'] if pd.notna(row['payer_display_name']) else row['payer_slug'],
                'estimated_records': int(row['estimated_records']),
                'file_size_mb': round(row['file_size_mb'], 2),
                'partition_count': len(filtered_df[filtered_df['payer_slug'] == row['payer_slug']])
            })
        
        return breakdown
    
    def analyze_combined_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze combined partition data"""
        if df is None or df.empty:
            return {'error': 'No data to analyze'}
        
        analysis = {
            'shape': {
                'rows': len(df),
                'columns': len(df.columns)
            },
            'columns': list(df.columns),
            'sample_data': df.head(10).to_dict('records'),
            'numeric_summary': {},
            'categorical_summary': {}
        }
        
        # Numeric summary
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            analysis['numeric_summary'] = df[numeric_cols].describe().to_dict()
        
        # Categorical summary
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols[:5]:  # Limit to first 5 categorical columns
            try:
                unique_count = df[col].nunique()
                if unique_count < 20:  # Only show if reasonable number of unique values
                    try:
                        analysis['categorical_summary'][col] = df[col].value_counts().head(10).to_dict()
                    except:
                        pass  # Skip problematic columns
            except:
                pass  # Skip columns with unhashable types
        
        return analysis
    
    def export_data(self, df: pd.DataFrame, format: str = 'csv') -> bytes:
        """Export data in specified format"""
        if format.lower() == 'csv':
            return df.to_csv(index=False).encode('utf-8')
        elif format.lower() == 'parquet':
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            return buffer.getvalue()
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def get_comprehensive_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get comprehensive analysis of the combined dataset"""
        if df is None or df.empty:
            return {'error': 'No data to analyze'}
        
        analysis = {
            'dataset_summary': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
                'load_summary': getattr(df, 'attrs', {}).get('_load_summary', {})
            },
            'data_quality': self._analyze_data_quality(df),
            'column_analysis': self._analyze_columns(df),
            'statistical_summary': self._get_statistical_summary(df),
            'business_insights': self._get_business_insights(df),
            'recommendations': self._get_recommendations(df)
        }
        
        return analysis
    
    def _analyze_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data quality metrics"""
        quality_metrics = {}
        
        for col in df.columns:
            if col.startswith('_'):  # Skip metadata columns
                continue
                
            col_data = df[col]
            
            # Handle unhashable types (like numpy arrays) gracefully
            try:
                unique_count = col_data.nunique()
                unique_percentage = round((unique_count / len(col_data)) * 100, 2)
            except TypeError:
                # For unhashable types, estimate unique count differently
                try:
                    # Try to convert to string and count unique
                    unique_count = col_data.astype(str).nunique()
                    unique_percentage = round((unique_count / len(col_data)) * 100, 2)
                except:
                    unique_count = "N/A"
                    unique_percentage = "N/A"
            
            # Get sample values safely
            try:
                if col_data.dtype == 'object':
                    sample_values = col_data.dropna().head(5).tolist()
                else:
                    sample_values = None
            except:
                sample_values = None
            
            quality_metrics[col] = {
                'total_values': len(col_data),
                'null_count': col_data.isnull().sum(),
                'null_percentage': round((col_data.isnull().sum() / len(col_data)) * 100, 2),
                'unique_count': unique_count,
                'unique_percentage': unique_percentage,
                'data_type': str(col_data.dtype),
                'sample_values': sample_values
            }
        
        return quality_metrics
    
    def _analyze_columns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze column characteristics"""
        column_analysis = {
            'numeric_columns': [],
            'categorical_columns': [],
            'datetime_columns': [],
            'text_columns': [],
            'high_cardinality_columns': []
        }
        
        for col in df.columns:
            if col.startswith('_'):  # Skip metadata columns
                continue
                
            col_data = df[col]
            
            # Handle unhashable types (like numpy arrays) gracefully
            try:
                unique_count = col_data.nunique()
            except TypeError:
                try:
                    unique_count = col_data.astype(str).nunique()
                except:
                    unique_count = 0  # Skip problematic columns
            
            total_count = len(col_data)
            
            # Categorize columns
            if pd.api.types.is_numeric_dtype(col_data):
                column_analysis['numeric_columns'].append({
                    'name': col,
                    'unique_count': unique_count,
                    'null_count': col_data.isnull().sum(),
                    'min': col_data.min() if not col_data.empty else None,
                    'max': col_data.max() if not col_data.empty else None,
                    'mean': col_data.mean() if not col_data.empty else None
                })
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                column_analysis['datetime_columns'].append(col)
            else:
                # Text/categorical
                if unique_count < total_count * 0.5:  # Less than 50% unique
                    try:
                        top_values = col_data.value_counts().head(10).to_dict()
                    except:
                        top_values = {}
                    
                    column_analysis['categorical_columns'].append({
                        'name': col,
                        'unique_count': unique_count,
                        'top_values': top_values
                    })
                else:
                    column_analysis['text_columns'].append({
                        'name': col,
                        'unique_count': unique_count,
                        'avg_length': col_data.astype(str).str.len().mean() if not col_data.empty else 0
                    })
            
            # Identify high cardinality columns
            if unique_count > total_count * 0.8:  # More than 80% unique
                column_analysis['high_cardinality_columns'].append({
                    'name': col,
                    'unique_count': unique_count,
                    'cardinality_percentage': round((unique_count / total_count) * 100, 2)
                })
        
        return column_analysis
    
    def _get_statistical_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get statistical summary of numeric columns with Medicare focus"""
        numeric_cols = df.select_dtypes(include=['number']).columns
        
        if len(numeric_cols) == 0:
            return {'message': 'No numeric columns found'}
        
        summary = {}
        
        # Prioritize Medicare-related columns for key metrics
        medicare_priority_cols = [
            'medicare_professional_rate', 'negotiated_rate_pct_of_medicare_professional',
            'medicare_asc_stateavg', 'medicare_opps_stateavg',
            'negotiated_rate_pct_of_medicare_asc', 'negotiated_rate_pct_of_medicare_opps',
            'rate'
        ]
        
        # Process Medicare priority columns first
        for col in medicare_priority_cols:
            if col in numeric_cols:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    summary[col] = {
                        'count': len(col_data),
                        'mean': round(col_data.mean(), 2),
                        'std': round(col_data.std(), 2),
                        'min': round(col_data.min(), 2),
                        'max': round(col_data.max(), 2),
                        'median': round(col_data.median(), 2),
                        'quartiles': {
                            'q25': round(col_data.quantile(0.25), 2),
                            'q75': round(col_data.quantile(0.75), 2)
                        }
                    }
                    
                    # Add currency formatting info for rate columns
                    if 'rate' in col.lower() and 'pct' not in col.lower():
                        summary[col]['currency_format'] = True
                    elif 'pct' in col.lower():
                        summary[col]['percentage_format'] = True
        
        # Process remaining numeric columns
        for col in numeric_cols:
            if col.startswith('_'):  # Skip metadata columns
                continue
            
            if col not in medicare_priority_cols:  # Skip already processed columns
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    summary[col] = {
                        'count': len(col_data),
                        'mean': round(col_data.mean(), 2),
                        'std': round(col_data.std(), 2),
                        'min': round(col_data.min(), 2),
                        'max': round(col_data.max(), 2),
                        'median': round(col_data.median(), 2),
                        'quartiles': {
                            'q25': round(col_data.quantile(0.25), 2),
                            'q75': round(col_data.quantile(0.75), 2)
                        }
                    }
        
        # Convert numpy types for JSON serialization
        return self._convert_numpy_types(summary)
    
    def _get_business_insights(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get business-specific insights from healthcare data with Medicare benchmarks"""
        insights = {}
        
        # Analyze rates if present
        if 'rate' in df.columns:
            rate_data = df['rate'].dropna()
            if len(rate_data) > 0:
                insights['rate_analysis'] = {
                    'total_rate_records': len(rate_data),
                    'avg_rate': round(rate_data.mean(), 2),
                    'median_rate': round(rate_data.median(), 2),
                    'rate_range': {
                        'min': round(rate_data.min(), 2),
                        'max': round(rate_data.max(), 2)
                    },
                    'high_value_threshold': round(rate_data.quantile(0.95), 2)
                }
        
        # Medicare Professional Analysis
        if 'medicare_professional_rate' in df.columns:
            prof_medicare_data = df['medicare_professional_rate'].dropna()
            if len(prof_medicare_data) > 0:
                insights['medicare_professional_analysis'] = {
                    'total_records_with_medicare_prof': len(prof_medicare_data),
                    'avg_medicare_professional_rate': round(prof_medicare_data.mean(), 2),
                    'median_medicare_professional_rate': round(prof_medicare_data.median(), 2),
                    'medicare_prof_range': {
                        'min': round(prof_medicare_data.min(), 2),
                        'max': round(prof_medicare_data.max(), 2)
                    }
                }
        
        # Medicare Professional vs Negotiated Rate Analysis
        if 'negotiated_rate_pct_of_medicare_professional' in df.columns:
            prof_pct_data = df['negotiated_rate_pct_of_medicare_professional'].dropna()
            if len(prof_pct_data) > 0:
                insights['medicare_professional_comparison'] = {
                    'total_records_with_comparison': len(prof_pct_data),
                    'avg_negotiated_rate_pct_of_medicare_prof': round(prof_pct_data.mean(), 2),
                    'median_negotiated_rate_pct_of_medicare_prof': round(prof_pct_data.median(), 2),
                    'pct_range': {
                        'min': round(prof_pct_data.min(), 2),
                        'max': round(prof_pct_data.max(), 2)
                    },
                    'above_medicare_threshold': round((prof_pct_data > 100).sum() / len(prof_pct_data) * 100, 1),
                    'below_medicare_threshold': round((prof_pct_data < 100).sum() / len(prof_pct_data) * 100, 1)
                }
        
        # Medicare Institutional Analysis (ASC)
        if 'medicare_asc_stateavg' in df.columns:
            asc_medicare_data = df['medicare_asc_stateavg'].dropna()
            if len(asc_medicare_data) > 0:
                insights['medicare_asc_analysis'] = {
                    'total_records_with_medicare_asc': len(asc_medicare_data),
                    'avg_medicare_asc_stateavg': round(asc_medicare_data.mean(), 2),
                    'median_medicare_asc_stateavg': round(asc_medicare_data.median(), 2),
                    'medicare_asc_range': {
                        'min': round(asc_medicare_data.min(), 2),
                        'max': round(asc_medicare_data.max(), 2)
                    }
                }
        
        # Medicare Institutional Analysis (OPPS)
        if 'medicare_opps_stateavg' in df.columns:
            opps_medicare_data = df['medicare_opps_stateavg'].dropna()
            if len(opps_medicare_data) > 0:
                insights['medicare_opps_analysis'] = {
                    'total_records_with_medicare_opps': len(opps_medicare_data),
                    'avg_medicare_opps_stateavg': round(opps_medicare_data.mean(), 2),
                    'median_medicare_opps_stateavg': round(opps_medicare_data.median(), 2),
                    'medicare_opps_range': {
                        'min': round(opps_medicare_data.min(), 2),
                        'max': round(opps_medicare_data.max(), 2)
                    }
                }
        
        # Medicare ASC vs Negotiated Rate Analysis
        if 'negotiated_rate_pct_of_medicare_asc' in df.columns:
            asc_pct_data = df['negotiated_rate_pct_of_medicare_asc'].dropna()
            if len(asc_pct_data) > 0:
                insights['medicare_asc_comparison'] = {
                    'total_records_with_asc_comparison': len(asc_pct_data),
                    'avg_negotiated_rate_pct_of_medicare_asc': round(asc_pct_data.mean(), 2),
                    'median_negotiated_rate_pct_of_medicare_asc': round(asc_pct_data.median(), 2),
                    'asc_pct_range': {
                        'min': round(asc_pct_data.min(), 2),
                        'max': round(asc_pct_data.max(), 2)
                    },
                    'above_medicare_asc_threshold': round((asc_pct_data > 100).sum() / len(asc_pct_data) * 100, 1),
                    'below_medicare_asc_threshold': round((asc_pct_data < 100).sum() / len(asc_pct_data) * 100, 1)
                }
        
        # Medicare OPPS vs Negotiated Rate Analysis
        if 'negotiated_rate_pct_of_medicare_opps' in df.columns:
            opps_pct_data = df['negotiated_rate_pct_of_medicare_opps'].dropna()
            if len(opps_pct_data) > 0:
                insights['medicare_opps_comparison'] = {
                    'total_records_with_opps_comparison': len(opps_pct_data),
                    'avg_negotiated_rate_pct_of_medicare_opps': round(opps_pct_data.mean(), 2),
                    'median_negotiated_rate_pct_of_medicare_opps': round(opps_pct_data.median(), 2),
                    'opps_pct_range': {
                        'min': round(opps_pct_data.min(), 2),
                        'max': round(opps_pct_data.max(), 2)
                    },
                    'above_medicare_opps_threshold': round((opps_pct_data > 100).sum() / len(opps_pct_data) * 100, 1),
                    'below_medicare_opps_threshold': round((opps_pct_data < 100).sum() / len(opps_pct_data) * 100, 1)
                }
        
        # Analyze by billing class if present
        if 'billing_class' in df.columns:
            billing_class_counts = df['billing_class'].value_counts()
            insights['billing_class_distribution'] = billing_class_counts.to_dict()
        
        # Analyze by payer if present
        if 'payer' in df.columns:
            payer_counts = df['payer'].value_counts().head(10)
            insights['top_payers'] = payer_counts.to_dict()
        
        # Analyze by organization if present
        if 'org_name' in df.columns:
            org_counts = df['org_name'].value_counts().head(10)
            insights['top_organizations'] = org_counts.to_dict()
        
        # Convert numpy types for JSON serialization
        return self._convert_numpy_types(insights)
    
    def _get_recommendations(self, df: pd.DataFrame) -> List[str]:
        """Get recommendations for data analysis and usage"""
        recommendations = []
        
        # Data quality recommendations
        null_threshold = 0.1  # 10%
        for col in df.columns:
            if col.startswith('_'):  # Skip metadata columns
                continue
                
            null_pct = df[col].isnull().sum() / len(df)
            if null_pct > null_threshold:
                recommendations.append(f"Column '{col}' has {null_pct:.1%} missing values - consider data cleaning")
        
        # Size recommendations
        if len(df) > 100000:
            recommendations.append("Large dataset detected - consider sampling for initial analysis")
        elif len(df) < 1000:
            recommendations.append("Small dataset - results may not be statistically significant")
        
        # Column recommendations
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 20:
            recommendations.append("Many numeric columns - consider dimensional reduction techniques")
        
        categorical_cols = df.select_dtypes(include=['object']).columns
        high_cardinality = []
        for col in categorical_cols:
            try:
                if df[col].nunique() > len(df) * 0.8:
                    high_cardinality.append(col)
            except:
                pass  # Skip columns with unhashable types
        if high_cardinality:
            recommendations.append(f"High cardinality columns detected: {', '.join(high_cardinality)} - consider grouping or encoding")
        
        return recommendations
