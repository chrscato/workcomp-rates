import sqlite3
import pandas as pd
import boto3
import io
from typing import Dict, List, Optional, Any
import logging
from pathlib import Path
from django.conf import settings
import time

logger = logging.getLogger(__name__)

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
                
                # Add summary metadata
                combined_df.attrs['_load_summary'] = {
                    'total_partitions': len(partition_paths),
                    'successful_loads': successful_loads,
                    'failed_loads': failed_loads,
                    'total_rows': len(combined_df),
                    'load_time_seconds': time.time() - start_time,
                    'columns': list(combined_df.columns)
                }
                
                logger.info(f"Successfully combined {len(combined_df)} rows from {successful_loads} partitions in {time.time() - start_time:.2f}s")
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
                'available_filters': {}
            }
        
        # Calculate summary statistics
        total_size = results_df['file_size_mb'].sum()
        total_records = results_df['estimated_records'].sum()
        
        # Get available filter options based on current results
        available_filters = {}
        for col in ['procedure_set', 'taxonomy_code', 'stat_area_name', 'year', 'month']:
            if col in results_df.columns:
                available_filters[col] = results_df[col].dropna().unique().tolist()
        
        return {
            'partition_count': len(results_df),
            'total_size_mb': round(total_size, 2),
            'total_estimated_records': int(total_records),
            'available_filters': available_filters,
            'partitions': results_df.to_dict('records')
        }
    
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
        """Get statistical summary of numeric columns"""
        numeric_cols = df.select_dtypes(include=['number']).columns
        
        if len(numeric_cols) == 0:
            return {'message': 'No numeric columns found'}
        
        summary = {}
        for col in numeric_cols:
            if col.startswith('_'):  # Skip metadata columns
                continue
                
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
        
        return summary
    
    def _get_business_insights(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get business-specific insights from healthcare data"""
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
        
        return insights
    
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
