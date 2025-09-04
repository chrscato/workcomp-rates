import pandas as pd
import duckdb
import os
import glob
from pathlib import Path
import logging
from typing import Optional, Dict, Any, List
import threading
import hashlib
import json

logger = logging.getLogger(__name__)

class ParquetDataManager:
    # Class-level connection pool for better performance
    _connection_pool = {}
    _pool_lock = threading.Lock()
    
    def __init__(self, file_path: Optional[str] = None, state: Optional[str] = None):
        if file_path:
            self.file_path = file_path
        elif state:
            # State-specific file
            self.file_path = os.path.join(
                Path(__file__).resolve().parent.parent,
                'data',
                f'commercial_rates_{state.upper()}.parquet'
            )
        else:
            # Default file
            self.file_path = os.path.join(
                Path(__file__).resolve().parent.parent,
                'data',
                'commercial_rates.parquet'
            )
        
        # Check if data file exists
        self.has_data = os.path.exists(self.file_path)
        if not self.has_data:
            logger.warning(f"Data file not found: {self.file_path}. Using sample data.")
        
        # Initialize connection for this instance
        self._init_connection()
    
    def _init_connection(self):
        """Initialize or get connection from pool"""
        with self._pool_lock:
            if self.file_path not in self._connection_pool:
                try:
                    con = duckdb.connect(database=':memory:')
                    if self.has_data:
                        con.execute(f"CREATE VIEW commercial_rates AS SELECT * FROM read_parquet('{self.file_path}')")
                    else:
                        # Use sample data
                        sample_df = self._get_sample_data()
                        con.execute("CREATE TABLE commercial_rates AS SELECT * FROM sample_df", {"sample_df": sample_df})
                    
                    self._connection_pool[self.file_path] = con
                    logger.info(f"Created new connection for {self.file_path}")
                except Exception as e:
                    logger.error(f"Failed to create connection for {self.file_path}: {str(e)}")
                    self._connection_pool[self.file_path] = None
            
            self.connection = self._connection_pool[self.file_path]
    
    def _get_connection(self):
        """Get connection, reinitialize if needed"""
        if self.connection is None:
            self._init_connection()
        
        # Test the connection to make sure it's still valid
        try:
            if self.connection:
                # Simple test query to verify connection is working
                self.connection.execute("SELECT 1").fetchone()
        except Exception as e:
            logger.warning(f"Connection test failed, reinitializing: {str(e)}")
            # Connection is corrupted, reinitialize
            with self._pool_lock:
                if self.file_path in self._connection_pool:
                    try:
                        self._connection_pool[self.file_path].close()
                    except:
                        pass
                    del self._connection_pool[self.file_path]
            self._init_connection()
        
        return self.connection
    
    @classmethod
    def cleanup_connections(cls):
        """Clean up all connections in the pool"""
        with cls._pool_lock:
            for file_path, con in cls._connection_pool.items():
                if con:
                    try:
                        con.close()
                    except:
                        pass
            cls._connection_pool.clear()
            logger.info("Cleaned up all database connections")
    
    @staticmethod
    def generate_cache_key(state_code: str, filters: Dict[str, Any]) -> str:
        """Generate a consistent cache key for filters"""
        # Sort filters to ensure consistent ordering
        sorted_filters = {}
        for key in sorted(filters.keys()):
            if filters[key]:  # Only include non-empty filters
                if isinstance(filters[key], list):
                    sorted_filters[key] = sorted(filters[key])
                else:
                    sorted_filters[key] = filters[key]
        
        # Create a deterministic hash
        filter_str = json.dumps(sorted_filters, sort_keys=True)
        filter_hash = hashlib.md5(filter_str.encode()).hexdigest()
        
        return f"insights_{state_code}_{filter_hash}"

    @staticmethod
    def get_available_states() -> Dict[str, str]:
        """
        Scan the data folder for available state parquet files.
        Returns a dict with state codes as keys and status as values.
        """
        data_folder = os.path.join(
            Path(__file__).resolve().parent.parent,
            'data'
        )
        
        # Define all US states with their codes
        all_states = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
            'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
            'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
            'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
            'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
            'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
            'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
            'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
        }
        
        # Scan for available state files
        available_states = {}
        for state_code in all_states.keys():
            state_file = os.path.join(data_folder, f'commercial_rates_{state_code}.parquet')
            if os.path.exists(state_file):
                available_states[state_code] = 'available'
            else:
                available_states[state_code] = 'not_ready'
        
        return available_states

    @staticmethod
    def get_state_name(state_code: str) -> str:
        """Get full state name from state code."""
        state_names = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
            'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
            'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
            'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
            'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
            'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
            'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
            'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
        }
        return state_names.get(state_code.upper(), state_code)

    def _get_sample_data(self) -> pd.DataFrame:
        """Generate sample data for demonstration purposes."""
        import numpy as np
        
        # Create sample data
        np.random.seed(42)
        n_records = 1000
        
        sample_data = pd.DataFrame({
            'payer': np.random.choice(['Aetna', 'Blue Cross', 'Cigna', 'UnitedHealth'], n_records),
            'org_name': np.random.choice(['Hospital A', 'Hospital B', 'Clinic C', 'Center D'], n_records),
            'procedure_set': np.random.choice(['Cardiology', 'Orthopedics', 'Neurology', 'General'], n_records),
            'procedure_class': np.random.choice(['Surgery', 'Consultation', 'Diagnostic', 'Treatment'], n_records),
            'procedure_group': np.random.choice(['Cardiac', 'Spine', 'Brain', 'General'], n_records),
            'cbsa': np.random.choice(['12060', '35620', '31080', '19100'], n_records),
            'billing_code': np.random.choice(['99213', '99214', '99215', '99232'], n_records),
            'billing_class': np.random.choice(['professional', 'institutional'], n_records),
            'rate': np.random.uniform(50, 500, n_records),
            'code_desc': np.random.choice(['Office visit', 'Surgery', 'Consultation', 'Diagnostic test'], n_records),
            'primary_taxonomy_desc': np.random.choice(['Cardiology', 'Orthopedics', 'Neurology', 'General Practice'], n_records),
            'primary_taxonomy_code': np.random.choice(['207RC0000X', '207T00000X', '208D00000X', '207Q00000X'], n_records),
            'tin_value': np.random.choice(['123456789', '987654321', '456789123', '789123456'], n_records),
            'GA_PROF_MAR': np.random.uniform(40, 400, n_records),
            'medicare_prof': np.random.uniform(30, 300, n_records),
            'GA_OP_MAR': np.random.uniform(200, 2000, n_records),
            'GA_ASC_MAR': np.random.uniform(300, 3000, n_records),
            'medicare_opps_mar_stateavg': np.random.uniform(150, 1500, n_records),
            'medicare_asc_mar_stateavg': np.random.uniform(250, 2500, n_records),
        })
        
        return sample_data

    def build_where_clause(self, filters: Dict[str, Any]) -> str:
        """Build WHERE clause from filters."""
        where_clauses = []
        if filters:
            for col, val in filters.items():
                if val and val != '':
                    if isinstance(val, list):
                        # Handle multiple values with IN clause
                        if val:  # Check if list is not empty
                            values = [f"'{v}'" for v in val if v]  # Filter out empty values
                            if values:
                                where_clauses.append(f"{col} IN ({', '.join(values)})")
                    else:
                        # Handle single value
                        where_clauses.append(f"{col} = '{val}'")
        return " AND ".join(where_clauses) if where_clauses else "1=1"

    def get_unique_values(self, column: str, filters: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Get unique values for a column with optional filters."""
        try:
            con = self._get_connection()
            if not con:
                logger.error("No database connection available")
                return []
            
            # Build WHERE clause from filters
            where_sql = self.build_where_clause(filters or {})
            
            query = f"""
                SELECT DISTINCT {column}
                FROM commercial_rates
                WHERE {where_sql}
                AND {column} IS NOT NULL
                ORDER BY {column}
            """
            
            result = con.execute(query).fetchall()
            return [r[0] for r in result if r[0]]  # Filter out None/empty values
            
        except Exception as e:
            logger.error(f"Error getting unique values for {column}: {str(e)}")
            return []

    def get_aggregated_stats(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get aggregated statistics with optional filters."""
        try:
            con = self._get_connection()
            if not con:
                logger.error("No database connection available")
                return {
                    'professional': {'avg_rate': 0, 'ga_prof_mar': 0, 'ga_prof_pct': 0, 'medicare_prof_mar': 0, 'medicare_prof_pct': 0, 'record_count': 0},
                    'facility': {'avg_rate': 0, 'ga_op_mar': 0, 'ga_op_pct': 0, 'ga_asc_mar': 0, 'ga_asc_pct': 0, 'medicare_op_mar_stateavg': 0, 'medicare_op_pct': 0, 'medicare_asc_mar_stateavg': 0, 'medicare_asc_pct': 0, 'record_count': 0}
                }
            
            # Build WHERE clause from filters
            where_sql = self.build_where_clause(filters or {})
            
            # Professional rates (billing_class = 'professional')
            prof_query = f"""
                SELECT 
                    rate,
                    GA_PROF_MAR,
                    medicare_prof
                FROM commercial_rates
                WHERE {where_sql}
                AND billing_class = 'professional'
                AND rate IS NOT NULL
            """
            
            prof_data = con.execute(prof_query).fetchall()
            
            # Calculate professional statistics
            prof_rates = []
            prof_ga_mar = []
            prof_medicare = []
            
            for row in prof_data:
                try:
                    rate = float(row[0]) if row[0] else 0
                    ga_mar = float(row[1]) if row[1] else 0
                    medicare = float(row[2]) if row[2] else 0
                    
                    if rate > 0:
                        prof_rates.append(rate)
                    if ga_mar > 0:
                        prof_ga_mar.append(ga_mar)
                    if medicare > 0:
                        prof_medicare.append(medicare)
                except (ValueError, TypeError):
                    continue
            
            # Calculate averages and percentages
            avg_prof_rate = sum(prof_rates) / len(prof_rates) if prof_rates else 0
            avg_prof_ga_mar = sum(prof_ga_mar) / len(prof_ga_mar) if prof_ga_mar else 0
            avg_prof_medicare = sum(prof_medicare) / len(prof_medicare) if prof_medicare else 0
            
            # Calculate percentages
            prof_ga_pct = 0
            prof_medicare_pct = 0
            
            if prof_rates and prof_ga_mar:
                prof_ga_pct = (avg_prof_rate / avg_prof_ga_mar) * 100 if avg_prof_ga_mar > 0 else 0
            
            if prof_rates and prof_medicare:
                prof_medicare_pct = (avg_prof_rate / avg_prof_medicare) * 100 if avg_prof_medicare > 0 else 0
            
            # Facility rates (billing_class = 'institutional')
            facility_query = f"""
                SELECT 
                    rate,
                    GA_OP_MAR,
                    GA_ASC_MAR,
                    medicare_opps_mar_stateavg,
                    medicare_asc_mar_stateavg,
                    primary_taxonomy_desc
                FROM commercial_rates
                WHERE {where_sql}
                AND billing_class = 'institutional'
                AND rate IS NOT NULL
            """
            
            facility_data = con.execute(facility_query).fetchall()
            
            # Calculate facility statistics
            facility_rates = []
            ga_op_mar = []
            ga_asc_mar = []
            medicare_opps_mar = []
            medicare_asc_mar = []
            
            for row in facility_data:
                try:
                    rate = float(row[0]) if row[0] else 0
                    ga_op = float(row[1]) if row[1] else 0
                    ga_asc = float(row[2]) if row[2] else 0
                    medicare_opps = float(row[3]) if row[3] else 0
                    medicare_asc = float(row[4]) if row[4] else 0
                    primary_taxonomy_desc = row[5] if row[5] else ''
                    
                    if rate > 0:
                        facility_rates.append(rate)
                    # Only include GA WCFS OP MAR calculations when taxonomy contains 'Hospital'
                    if ga_op > 0 and 'Hospital' in primary_taxonomy_desc:
                        ga_op_mar.append(ga_op)
                    # Only include GA WCFS ASC MAR calculations when taxonomy does NOT contain 'Hospital'
                    if ga_asc > 0 and 'Hospital' not in primary_taxonomy_desc:
                        ga_asc_mar.append(ga_asc)
                    # Only include Medicare OP MAR calculations when taxonomy contains 'Hospital'
                    if medicare_opps > 0 and 'Hospital' in primary_taxonomy_desc:
                        medicare_opps_mar.append(medicare_opps)
                    # Only include Medicare ASC MAR calculations when taxonomy does NOT contain 'Hospital'
                    if medicare_asc > 0 and 'Hospital' not in primary_taxonomy_desc:
                        medicare_asc_mar.append(medicare_asc)
                except (ValueError, TypeError):
                    continue
            
            # Calculate facility averages
            avg_facility_rate = sum(facility_rates) / len(facility_rates) if facility_rates else 0
            avg_ga_op_mar = sum(ga_op_mar) / len(ga_op_mar) if ga_op_mar else 0
            avg_ga_asc_mar = sum(ga_asc_mar) / len(ga_asc_mar) if ga_asc_mar else 0
            avg_medicare_opps_mar = sum(medicare_opps_mar) / len(medicare_opps_mar) if medicare_opps_mar else 0
            avg_medicare_asc_mar = sum(medicare_asc_mar) / len(medicare_asc_mar) if medicare_asc_mar else 0
            
            # Calculate facility percentages
            facility_ga_op_pct = 0
            facility_ga_asc_pct = 0
            facility_medicare_op_pct = 0
            facility_medicare_asc_pct = 0
            
            if facility_rates and ga_op_mar:
                facility_ga_op_pct = (avg_facility_rate / avg_ga_op_mar) * 100 if avg_ga_op_mar > 0 else 0
            
            if facility_rates and ga_asc_mar:
                facility_ga_asc_pct = (avg_facility_rate / avg_ga_asc_mar) * 100 if avg_ga_asc_mar > 0 else 0
            
            if facility_rates and medicare_opps_mar:
                facility_medicare_op_pct = (avg_facility_rate / avg_medicare_opps_mar) * 100 if avg_medicare_opps_mar > 0 else 0
            
            if facility_rates and medicare_asc_mar:
                facility_medicare_asc_pct = (avg_facility_rate / avg_medicare_asc_mar) * 100 if avg_medicare_asc_mar > 0 else 0
            
            return {
                'professional': {
                    'avg_rate': round(avg_prof_rate, 2),
                    'ga_prof_mar': round(avg_prof_ga_mar, 2),
                    'ga_prof_pct': round(prof_ga_pct, 2),
                    'medicare_prof_mar': round(avg_prof_medicare, 2),
                    'medicare_prof_pct': round(prof_medicare_pct, 2),
                    'record_count': len(prof_data),
                },
                'facility': {
                    'avg_rate': round(avg_facility_rate, 2),
                    'ga_op_mar': round(avg_ga_op_mar, 2),
                    'ga_op_pct': round(facility_ga_op_pct, 2),
                    'ga_asc_mar': round(avg_ga_asc_mar, 2),
                    'ga_asc_pct': round(facility_ga_asc_pct, 2),
                    'medicare_op_mar_stateavg': round(avg_medicare_opps_mar, 2),
                    'medicare_op_pct': round(facility_medicare_op_pct, 2),
                    'medicare_asc_mar_stateavg': round(avg_medicare_asc_mar, 2),
                    'medicare_asc_pct': round(facility_medicare_asc_pct, 2),
                    'record_count': len(facility_data),
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting aggregated stats: {str(e)}")
            return {
                'professional': {'avg_rate': 0, 'ga_prof_mar': 0, 'ga_prof_pct': 0, 'medicare_prof_mar': 0, 'medicare_prof_pct': 0, 'record_count': 0},
                'facility': {'avg_rate': 0, 'ga_op_mar': 0, 'ga_op_pct': 0, 'ga_asc_mar': 0, 'ga_asc_pct': 0, 'medicare_op_mar_stateavg': 0, 'medicare_op_pct': 0, 'medicare_asc_mar_stateavg': 0, 'medicare_asc_pct': 0, 'record_count': 0}
            }

    def get_sample_records(self, filters: Optional[Dict[str, Any]] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Get sample records with optional filters."""
        try:
            con = self._get_connection()
            if not con:
                logger.error("No database connection available")
                return []
            
            # Build WHERE clause from filters
            where_sql = self.build_where_clause(filters or {})
            
            query = f"""
                SELECT 
                    payer, org_name, procedure_set, procedure_class, procedure_group,
                    cbsa, billing_code, billing_class, rate, code_desc, primary_taxonomy_desc,
                    primary_taxonomy_code, tin_value, GA_PROF_MAR, medicare_prof
                FROM commercial_rates
                WHERE {where_sql}
                LIMIT {limit}
            """
            
            result = con.execute(query).fetchall()
            return [
                {
                    'payer': row[0],
                    'org_name': row[1],
                    'procedure_set': row[2],
                    'procedure_class': row[3],
                    'procedure_group': row[4],
                    'cbsa': row[5],
                    'billing_code': row[6],
                    'billing_class': row[7],
                    'rate': float(row[8]) if row[8] else 0,
                    'code_desc': row[9] if row[9] else '',
                    'primary_taxonomy_desc': row[10] if row[10] else '',
                    'primary_taxonomy_code': row[11] if row[11] else '',
                    'tin_value': row[12] if row[12] else '',
                    'ga_prof_mar': float(row[13]) if row[13] else 0,
                    'medicare_prof_mar': float(row[14]) if row[14] else 0
                }
                for row in result
            ]
            
        except Exception as e:
            logger.error(f"Error getting sample records: {str(e)}")
            return []

    def get_comparison_stats(self, orgs: List[str], payers: List[str]) -> Dict[str, Any]:
        """Get comparison statistics for selected organizations and payers."""
        try:
            con = duckdb.connect(database=':memory:')
            
            if self.has_data:
                con.execute(f"CREATE VIEW commercial_rates AS SELECT * FROM read_parquet('{self.file_path}')")
            else:
                # Use sample data
                sample_df = self._get_sample_data()
                con.execute("CREATE TABLE commercial_rates AS SELECT * FROM sample_df", {"sample_df": sample_df})
            
            # Build WHERE clause for selected entities
            org_conditions = " OR ".join([f"org_name = '{org}'" for org in orgs]) if orgs else "1=1"
            payer_conditions = " OR ".join([f"payer = '{payer}'" for payer in payers]) if payers else "1=1"
            
            query = f"""
                SELECT 
                    org_name,
                    payer,
                    billing_class,
                    AVG(TRY_CAST(rate AS DOUBLE)) as avg_rate,
                    COUNT(*) as record_count
                FROM commercial_rates
                WHERE ({org_conditions}) AND ({payer_conditions})
                AND rate IS NOT NULL
                GROUP BY org_name, payer, billing_class
                ORDER BY org_name, payer, billing_class
            """
            
            result = con.execute(query).fetchall()
            
            comparison_data = {}
            for row in result:
                org = row[0]
                payer = row[1]
                billing_class = row[2]
                avg_rate = float(row[3]) if row[3] else 0
                record_count = int(row[4]) if row[4] else 0
                
                if org not in comparison_data:
                    comparison_data[org] = {}
                if payer not in comparison_data[org]:
                    comparison_data[org][payer] = {}
                
                comparison_data[org][payer][billing_class] = {
                    'avg_rate': avg_rate,
                    'record_count': record_count
                }
            
            return comparison_data
            
        except Exception as e:
            logger.error(f"Error getting comparison stats: {str(e)}")
            return {}

    def get_overview_statistics(self) -> Dict[str, Any]:
        """Get overview statistics for the dataset without heavy processing."""
        try:
            con = duckdb.connect(database=':memory:')
            
            if self.has_data:
                con.execute(f"CREATE VIEW commercial_rates AS SELECT * FROM read_parquet('{self.file_path}')")
            else:
                # Use sample data
                sample_df = self._get_sample_data()
                con.execute("CREATE TABLE commercial_rates AS SELECT * FROM sample_df", {"sample_df": sample_df})
            
            # Get distinct counts for key fields
            overview_query = """
                SELECT 
                    COUNT(DISTINCT payer) as distinct_payers,
                    COUNT(DISTINCT org_name) as distinct_organizations,
                    COUNT(DISTINCT procedure_set) as distinct_procedure_sets,
                    COUNT(DISTINCT procedure_class) as distinct_procedure_classes,
                    COUNT(DISTINCT procedure_group) as distinct_procedure_groups,
                    COUNT(DISTINCT cbsa) as distinct_cbsa_regions,
                    COUNT(DISTINCT billing_code) as distinct_billing_codes,
                    COUNT(DISTINCT primary_taxonomy_code) as distinct_taxonomy_codes,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN rate IS NOT NULL THEN 1 END) as records_with_rates,
                    COUNT(CASE WHEN billing_class = 'professional' THEN 1 END) as professional_records,
                    COUNT(CASE WHEN billing_class = 'institutional' THEN 1 END) as institutional_records
                FROM commercial_rates
            """
            
            result = con.execute(overview_query).fetchone()
            
            # Get top 10 values for key fields
            top_payers_query = """
                SELECT payer, COUNT(*) as count
                FROM commercial_rates
                GROUP BY payer
                ORDER BY count DESC
                LIMIT 10
            """
            
            top_orgs_query = """
                SELECT org_name, COUNT(*) as count
                FROM commercial_rates
                GROUP BY org_name
                ORDER BY count DESC
                LIMIT 10
            """
            
            top_procedure_sets_query = """
                SELECT procedure_set, COUNT(*) as count
                FROM commercial_rates
                GROUP BY procedure_set
                ORDER BY count DESC
                LIMIT 10
            """
            
            top_payers = con.execute(top_payers_query).fetchall()
            top_orgs = con.execute(top_orgs_query).fetchall()
            top_procedure_sets = con.execute(top_procedure_sets_query).fetchall()
            
            return {
                'summary': {
                    'distinct_payers': int(result[0]) if result[0] else 0,
                    'distinct_organizations': int(result[1]) if result[1] else 0,
                    'distinct_procedure_sets': int(result[2]) if result[2] else 0,
                    'distinct_procedure_classes': int(result[3]) if result[3] else 0,
                    'distinct_procedure_groups': int(result[4]) if result[4] else 0,
                    'distinct_cbsa_regions': int(result[5]) if result[5] else 0,
                    'distinct_billing_codes': int(result[6]) if result[6] else 0,
                    'distinct_taxonomy_codes': int(result[7]) if result[7] else 0,
                    'total_records': int(result[8]) if result[8] else 0,
                    'records_with_rates': int(result[9]) if result[9] else 0,
                    'professional_records': int(result[10]) if result[10] else 0,
                    'institutional_records': int(result[11]) if result[11] else 0,
                },
                'top_payers': [{'name': row[0], 'count': int(row[1])} for row in top_payers],
                'top_organizations': [{'name': row[0], 'count': int(row[1])} for row in top_orgs],
                'top_procedure_sets': [{'name': row[0], 'count': int(row[1])} for row in top_procedure_sets],
                'data_coverage': {
                    'rate_coverage_pct': round((int(result[9]) / int(result[8])) * 100, 1) if result[8] and result[8] > 0 else 0,
                    'professional_pct': round((int(result[10]) / int(result[8])) * 100, 1) if result[8] and result[8] > 0 else 0,
                    'institutional_pct': round((int(result[11]) / int(result[8])) * 100, 1) if result[8] and result[8] > 0 else 0,
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting overview statistics: {str(e)}")
            return {
                'summary': {},
                'top_payers': [],
                'top_organizations': [],
                'top_procedure_sets': [],
                'data_coverage': {}
            }

    def get_base_statistics(self, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get base statistics for comparison and analysis views."""
        try:
            con = duckdb.connect(database=':memory:')
            
            if self.has_data:
                con.execute(f"CREATE VIEW commercial_rates AS SELECT * FROM read_parquet('{self.file_path}')")
            else:
                # Use sample data
                sample_df = self._get_sample_data()
                con.execute("CREATE TABLE commercial_rates AS SELECT * FROM sample_df", {"sample_df": sample_df})
            
            # Apply filters if provided
            where_clause = self.build_where_clause(filters) if filters else "1=1"
            
            # Get professional and facility statistics
            stats_query = f"""
                SELECT 
                    billing_class,
                    COUNT(*) as record_count,
                    AVG(rate) as avg_rate,
                    AVG(CASE WHEN rate > 0 THEN rate END) as avg_positive_rate
                FROM commercial_rates
                WHERE {where_clause}
                GROUP BY billing_class
            """
            
            result = con.execute(stats_query).fetchall()
            
            # Initialize stats
            stats = {
                'professional': {'record_count': 0, 'avg_rate': 0, 'ga_prof_pct': 0, 'medicare_prof_pct': 0, 'ga_prof_mar': 0, 'medicare_prof_mar': 0},
                'facility': {'record_count': 0, 'avg_rate': 0, 'ga_op_pct': 0, 'ga_asc_pct': 0, 'medicare_op_pct': 0, 'medicare_asc_pct': 0, 'ga_op_mar': 0, 'ga_asc_mar': 0, 'medicare_op_mar': 0, 'medicare_asc_mar': 0}
            }
            
            # Process results
            for row in result:
                billing_class = row[0]
                if billing_class == 'professional':
                    stats['professional']['record_count'] = int(row[1])
                    stats['professional']['avg_rate'] = float(row[2]) if row[2] else 0
                elif billing_class == 'institutional':
                    stats['facility']['record_count'] = int(row[1])
                    stats['facility']['avg_rate'] = float(row[2]) if row[2] else 0
            
            # Get actual GA WCFS OP MAR data with Hospital taxonomy filter
            ga_op_query = f"""
                SELECT 
                    AVG(CASE WHEN primary_taxonomy_desc LIKE '%Hospital%' THEN ga_op_mar END) as ga_op_mar_avg
                FROM commercial_rates
                WHERE {where_clause}
                AND billing_class = 'institutional'
                AND primary_taxonomy_desc LIKE '%Hospital%'
                AND ga_op_mar IS NOT NULL
            """
            
            ga_op_result = con.execute(ga_op_query).fetchone()
            ga_op_mar_avg = float(ga_op_result[0]) if ga_op_result and ga_op_result[0] else 0
            
            # Get actual GA WCFS ASC MAR data excluding Hospital taxonomy
            ga_asc_query = f"""
                SELECT 
                    AVG(CASE WHEN primary_taxonomy_desc NOT LIKE '%Hospital%' THEN ga_asc_mar END) as ga_asc_mar_avg
                FROM commercial_rates
                WHERE {where_clause}
                AND billing_class = 'institutional'
                AND primary_taxonomy_desc NOT LIKE '%Hospital%'
                AND ga_asc_mar IS NOT NULL
            """
            
            ga_asc_result = con.execute(ga_asc_query).fetchone()
            ga_asc_mar_avg = float(ga_asc_result[0]) if ga_asc_result and ga_asc_result[0] else 0
            
            # Get actual Medicare OP MAR data with Hospital taxonomy filter
            medicare_op_query = f"""
                SELECT 
                    AVG(CASE WHEN primary_taxonomy_desc LIKE '%Hospital%' THEN medicare_op_mar END) as medicare_op_mar_avg
                FROM commercial_rates
                WHERE {where_clause}
                AND billing_class = 'institutional'
                AND primary_taxonomy_desc LIKE '%Hospital%'
                AND medicare_op_mar IS NOT NULL
            """
            
            medicare_op_result = con.execute(medicare_op_query).fetchone()
            medicare_op_mar_avg = float(medicare_op_result[0]) if medicare_op_result and medicare_op_result[0] else 0
            
            # Get actual Medicare ASC MAR data excluding Hospital taxonomy
            medicare_asc_query = f"""
                SELECT 
                    AVG(CASE WHEN primary_taxonomy_desc NOT LIKE '%Hospital%' THEN medicare_asc_mar END) as medicare_asc_mar_avg
                FROM commercial_rates
                WHERE {where_clause}
                AND billing_class = 'institutional'
                AND primary_taxonomy_desc NOT LIKE '%Hospital%'
                AND medicare_asc_mar IS NOT NULL
            """
            
            medicare_asc_result = con.execute(medicare_asc_query).fetchone()
            medicare_asc_mar_avg = float(medicare_asc_result[0]) if medicare_asc_result and medicare_asc_result[0] else 0
            
            # Calculate percentages and margins (simplified for now)
            stats['professional']['ga_prof_pct'] = 85.0  # Placeholder
            stats['professional']['medicare_prof_pct'] = 120.0  # Placeholder
            stats['professional']['ga_prof_mar'] = 15.0  # Placeholder
            stats['professional']['medicare_prof_mar'] = 20.0  # Placeholder
            
            stats['facility']['ga_op_pct'] = 90.0  # Placeholder
            stats['facility']['ga_asc_pct'] = 95.0  # Placeholder
            stats['facility']['medicare_op_pct'] = 110.0  # Placeholder
            stats['facility']['medicare_asc_pct'] = 105.0  # Placeholder
            stats['facility']['ga_op_mar'] = ga_op_mar_avg  # Actual calculated value with Hospital filter
            stats['facility']['ga_asc_mar'] = ga_asc_mar_avg  # Actual calculated value excluding Hospital
            stats['facility']['medicare_op_mar'] = medicare_op_mar_avg  # Actual calculated value with Hospital filter
            stats['facility']['medicare_asc_mar'] = medicare_asc_mar_avg  # Actual calculated value excluding Hospital
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting base statistics: {str(e)}")
            return {
                'professional': {'record_count': 0, 'avg_rate': 0, 'ga_prof_pct': 0, 'medicare_prof_pct': 0, 'ga_prof_mar': 0, 'medicare_prof_mar': 0},
                'facility': {'record_count': 0, 'avg_rate': 0, 'ga_op_pct': 0, 'ga_asc_pct': 0, 'medicare_op_pct': 0, 'medicare_asc_pct': 0, 'ga_op_mar': 0, 'ga_asc_mar': 0, 'medicare_op_mar': 0, 'medicare_asc_mar': 0}
            }

    def get_comparison_data(self, filters: Dict[str, Any] = None, selected_orgs: List[str] = None, selected_payers: List[str] = None) -> List[Dict[str, Any]]:
        """Get comparison data for selected organizations and payers."""
        try:
            con = duckdb.connect(database=':memory:')
            
            if self.has_data:
                con.execute(f"CREATE VIEW commercial_rates AS SELECT * FROM read_parquet('{self.file_path}')")
            else:
                # Use sample data
                sample_df = self._get_sample_data()
                con.execute("CREATE TABLE commercial_rates AS SELECT * FROM sample_df", {"sample_df": sample_df})
            
            # Apply filters if provided
            where_clause = self.build_where_clause(filters) if filters else "1=1"
            
            comparison_data = []
            
            # Get data for selected organizations
            if selected_orgs:
                for org in selected_orgs:
                    org_where = f"{where_clause} AND org_name = '{org}'"
                    
                    # Get professional rates
                    prof_query = f"""
                        SELECT 
                            COUNT(*) as record_count,
                            AVG(rate) as avg_rate,
                            AVG(ga_prof_mar) as ga_prof_mar,
                            AVG(medicare_prof_mar) as medicare_prof_mar
                        FROM commercial_rates
                        WHERE {org_where} AND procedure_class = 'Professional'
                    """
                    
                    # Get facility rates
                    fac_query = f"""
                        SELECT 
                            COUNT(*) as record_count,
                            AVG(rate) as avg_rate,
                            AVG(CASE WHEN primary_taxonomy_desc LIKE '%Hospital%' THEN ga_op_mar END) as ga_op_mar,
                            AVG(CASE WHEN primary_taxonomy_desc NOT LIKE '%Hospital%' THEN ga_asc_mar END) as ga_asc_mar,
                            AVG(CASE WHEN primary_taxonomy_desc LIKE '%Hospital%' THEN medicare_op_mar END) as medicare_op_mar,
                            AVG(CASE WHEN primary_taxonomy_desc NOT LIKE '%Hospital%' THEN medicare_asc_mar END) as medicare_asc_mar
                        FROM commercial_rates
                        WHERE {org_where} AND procedure_class = 'Facility'
                    """
                    
                    prof_result = con.execute(prof_query).fetchone()
                    fac_result = con.execute(fac_query).fetchone()
                    
                    if prof_result or fac_result:
                        comparison_data.append({
                            'name': org,
                            'type': 'organization',
                            'stats': {
                                'professional': {
                                    'record_count': int(prof_result[0]) if prof_result and prof_result[0] else 0,
                                    'avg_rate': float(prof_result[1]) if prof_result and prof_result[1] else 0,
                                    'ga_prof_pct': float(prof_result[2]) if prof_result and prof_result[2] else 0,
                                    'medicare_prof_pct': float(prof_result[3]) if prof_result and prof_result[3] else 0,
                                    'ga_prof_mar': float(prof_result[2]) if prof_result and prof_result[2] else 0,
                                    'medicare_prof_mar': float(prof_result[3]) if prof_result and prof_result[3] else 0
                                },
                                'facility': {
                                    'record_count': int(fac_result[0]) if fac_result and fac_result[0] else 0,
                                    'avg_rate': float(fac_result[1]) if fac_result and fac_result[1] else 0,
                                    'ga_op_pct': float(fac_result[2]) if fac_result and fac_result[2] else 0,
                                    'ga_asc_pct': float(fac_result[3]) if fac_result and fac_result[3] else 0,
                                    'medicare_op_pct': float(fac_result[4]) if fac_result and fac_result[4] else 0,
                                    'medicare_asc_pct': float(fac_result[5]) if fac_result and fac_result[5] else 0,
                                    'ga_op_mar': float(fac_result[2]) if fac_result and fac_result[2] else 0,
                                    'ga_asc_mar': float(fac_result[3]) if fac_result and fac_result[3] else 0,
                                    'medicare_op_mar': float(fac_result[4]) if fac_result and fac_result[4] else 0,
                                    'medicare_asc_mar': float(fac_result[5]) if fac_result and fac_result[5] else 0
                                }
                            }
                        })
            
            # Get data for selected payers
            if selected_payers:
                for payer in selected_payers:
                    payer_where = f"{where_clause} AND payer = '{payer}'"
                    
                    # Get professional rates
                    prof_query = f"""
                        SELECT 
                            COUNT(*) as record_count,
                            AVG(rate) as avg_rate,
                            AVG(ga_prof_mar) as ga_prof_mar,
                            AVG(medicare_prof_mar) as medicare_prof_mar
                        FROM commercial_rates
                        WHERE {payer_where} AND procedure_class = 'Professional'
                    """
                    
                    # Get facility rates
                    fac_query = f"""
                        SELECT 
                            COUNT(*) as record_count,
                            AVG(rate) as avg_rate,
                            AVG(CASE WHEN primary_taxonomy_desc LIKE '%Hospital%' THEN ga_op_mar END) as ga_op_mar,
                            AVG(CASE WHEN primary_taxonomy_desc NOT LIKE '%Hospital%' THEN ga_asc_mar END) as ga_asc_mar,
                            AVG(CASE WHEN primary_taxonomy_desc LIKE '%Hospital%' THEN medicare_op_mar END) as medicare_op_mar,
                            AVG(CASE WHEN primary_taxonomy_desc NOT LIKE '%Hospital%' THEN medicare_asc_mar END) as medicare_asc_mar
                        FROM commercial_rates
                        WHERE {payer_where} AND procedure_class = 'Facility'
                    """
                    
                    prof_result = con.execute(prof_query).fetchone()
                    fac_result = con.execute(fac_query).fetchone()
                    
                    if prof_result or fac_result:
                        comparison_data.append({
                            'name': payer,
                            'type': 'payer',
                            'stats': {
                                'professional': {
                                    'record_count': int(prof_result[0]) if prof_result and prof_result[0] else 0,
                                    'avg_rate': float(prof_result[1]) if prof_result and prof_result[1] else 0,
                                    'ga_prof_pct': float(prof_result[2]) if prof_result and prof_result[2] else 0,
                                    'medicare_prof_pct': float(prof_result[3]) if prof_result and prof_result[3] else 0,
                                    'ga_prof_mar': float(prof_result[2]) if prof_result and prof_result[2] else 0,
                                    'medicare_prof_mar': float(prof_result[3]) if prof_result and prof_result[3] else 0
                                },
                                'facility': {
                                    'record_count': int(fac_result[0]) if fac_result and fac_result[0] else 0,
                                    'avg_rate': float(fac_result[1]) if fac_result and fac_result[1] else 0,
                                    'ga_op_pct': float(fac_result[2]) if fac_result and fac_result[2] else 0,
                                    'ga_asc_pct': float(fac_result[3]) if fac_result and fac_result[3] else 0,
                                    'medicare_op_pct': float(fac_result[4]) if fac_result and fac_result[4] else 0,
                                    'medicare_asc_pct': float(fac_result[5]) if fac_result and fac_result[5] else 0,
                                    'ga_op_mar': float(fac_result[2]) if fac_result and fac_result[2] else 0,
                                    'ga_asc_mar': float(fac_result[3]) if fac_result and fac_result[3] else 0,
                                    'medicare_op_mar': float(fac_result[4]) if fac_result and fac_result[4] else 0,
                                    'medicare_asc_mar': float(fac_result[5]) if fac_result and fac_result[5] else 0
                                }
                            }
                        })
            
            return comparison_data
            
        except Exception as e:
            logger.error(f"Error getting comparison data: {str(e)}")
            return []

    def get_network_performance_metrics(self, custom_tins: List[str], filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get network performance metrics for custom TIN list."""
        try:
            con = duckdb.connect(database=':memory:')
            
            if self.has_data:
                con.execute(f"CREATE VIEW commercial_rates AS SELECT * FROM read_parquet('{self.file_path}')")
            else:
                # Use sample data
                sample_df = self._get_sample_data()
                con.execute("CREATE TABLE commercial_rates AS SELECT * FROM sample_df", {"sample_df": sample_df})
            
            # Apply filters and custom TINs
            base_filters = filters.copy() if filters else {}
            base_filters['tin_value'] = custom_tins
            where_clause = self.build_where_clause(base_filters)
            
            # Get network metrics
            metrics_query = f"""
                SELECT 
                    COUNT(DISTINCT tin_value) as network_size,
                    AVG(rate) as avg_rate,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN rate IS NOT NULL THEN 1 END) as records_with_rates
                FROM commercial_rates
                WHERE {where_clause}
            """
            
            result = con.execute(metrics_query).fetchone()
            
            # Get total state records for coverage calculation
            total_state_query = "SELECT COUNT(*) FROM commercial_rates"
            total_state_records = con.execute(total_state_query).fetchone()[0]
            
            if result and result[0]:
                network_size = int(result[0])
                avg_rate = float(result[1]) if result[1] else 0
                total_records = int(result[2])
                records_with_rates = int(result[3])
                
                # Calculate coverage percentage
                coverage_pct = round((total_records / total_state_records) * 100, 1) if total_state_records > 0 else 0
                
                # Calculate efficiency score (simplified)
                efficiency_score = round((records_with_rates / total_records) * 100, 1) if total_records > 0 else 0
                
                return {
                    'network_size': network_size,
                    'avg_rate': avg_rate,
                    'total_records': total_records,
                    'records_with_rates': records_with_rates,
                    'coverage_pct': coverage_pct,
                    'efficiency_score': efficiency_score
                }
            else:
                return {
                    'network_size': 0,
                    'avg_rate': 0,
                    'total_records': 0,
                    'records_with_rates': 0,
                    'coverage_pct': 0,
                    'efficiency_score': 0
                }
                
        except Exception as e:
            logger.error(f"Error getting network performance metrics: {str(e)}")
            return {
                'network_size': 0,
                'avg_rate': 0,
                'total_records': 0,
                'records_with_rates': 0,
                'coverage_pct': 0,
                'efficiency_score': 0
            }
