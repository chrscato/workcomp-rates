import pandas as pd
import duckdb
import os
import glob
from pathlib import Path
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

class ParquetDataManager:
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
                    where_clauses.append(f"{col} = '{val}'")
        return " AND ".join(where_clauses) if where_clauses else "1=1"

    def get_unique_values(self, column: str, filters: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Get unique values for a column with optional filters."""
        try:
            con = duckdb.connect(database=':memory:')
            
            if self.has_data:
                con.execute(f"CREATE VIEW commercial_rates AS SELECT * FROM read_parquet('{self.file_path}')")
            else:
                # Use sample data
                sample_df = self._get_sample_data()
                con.execute("CREATE TABLE commercial_rates AS SELECT * FROM sample_df", {"sample_df": sample_df})
            
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
            con = duckdb.connect(database=':memory:')
            
            if self.has_data:
                con.execute(f"CREATE VIEW commercial_rates AS SELECT * FROM read_parquet('{self.file_path}')")
            else:
                # Use sample data
                sample_df = self._get_sample_data()
                con.execute("CREATE TABLE commercial_rates AS SELECT * FROM sample_df", {"sample_df": sample_df})
            
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
                    medicare_asc_mar_stateavg
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
                    
                    if rate > 0:
                        facility_rates.append(rate)
                    if ga_op > 0:
                        ga_op_mar.append(ga_op)
                    if ga_asc > 0:
                        ga_asc_mar.append(ga_asc)
                    if medicare_opps > 0:
                        medicare_opps_mar.append(medicare_opps)
                    if medicare_asc > 0:
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
            con = duckdb.connect(database=':memory:')
            
            if self.has_data:
                con.execute(f"CREATE VIEW commercial_rates AS SELECT * FROM read_parquet('{self.file_path}')")
            else:
                # Use sample data
                sample_df = self._get_sample_data()
                con.execute("CREATE TABLE commercial_rates AS SELECT * FROM sample_df", {"sample_df": sample_df})
            
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
