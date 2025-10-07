import sqlite3
import json
import os
from django.conf import settings
from typing import List, Dict, Optional, Any


class MarketRateLookup:
    """
    Utility class for Market Rate lookup functionality using dims.sqlite database
    """
    
    def __init__(self):
        self.db_path = os.path.join(settings.BASE_DIR, 'core', 'data', 'dims.sqlite')
    
    def get_db_connection(self):
        """Get connection to dims.sqlite database"""
        return sqlite3.connect(self.db_path)
    
    def search_tin_lookup(self, tin_value: str = None, organization_name: str = None) -> List[Dict]:
        """
        Search TIN lookup using dim_tin, dim_tin_location tables
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    dt.tin_value,
                    dt.organization_name,
                    dtl.address_1,
                    dtl.address_2,
                    dtl.city,
                    dtl.state,
                    dtl.zip_norm,
                    dtl.latitude,
                    dtl.longitude,
                    dtl.support_npi_count,
                    dtl.primary_flag,
                    dtl.primary_basis,
                    dtl.npi_list_json,
                    dtl.last_updated
                FROM dim_tin dt
                LEFT JOIN dim_tin_location dtl ON dt.tin_value = dtl.tin_value
                WHERE 1=1
            """
            params = []
            
            if tin_value:
                query += " AND dt.tin_value LIKE ?"
                params.append(f'%{tin_value}%')
            
            if organization_name:
                query += " AND dt.organization_name LIKE ?"
                params.append(f'%{organization_name}%')
            
            query += " ORDER BY dt.organization_name, dtl.primary_flag DESC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                result = {
                    'tin_value': row[0],
                    'organization_name': row[1],
                    'address_1': row[2],
                    'address_2': row[3],
                    'city': row[4],
                    'state': row[5],
                    'zip_norm': row[6],
                    'latitude': row[7],
                    'longitude': row[8],
                    'support_npi_count': row[9],
                    'primary_flag': row[10],
                    'primary_basis': row[11],
                    'npi_list_json': json.loads(row[12]) if row[12] else [],
                    'last_updated': row[13]
                }
                results.append(result)
            
            return results
            
        finally:
            conn.close()
    
    def check_s3_tiles_availability(self, tin_value: str) -> Dict:
        """
        Check if TIN has data available in s3_tiles table
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    payer_slug,
                    billing_class,
                    negotiation_arrangement,
                    negotiated_type,
                    proc_set,
                    proc_class,
                    proc_group,
                    s3_prefix,
                    parts_count,
                    row_count,
                    billing_codes_json,
                    taxonomy_codes_json,
                    created_at_utc
                FROM s3_tiles
                WHERE tin_value = ?
                ORDER BY created_at_utc DESC
            """
            
            cursor.execute(query, (tin_value,))
            rows = cursor.fetchall()
            
            if not rows:
                return {'available': False, 'tiles': []}
            
            tiles = []
            for row in rows:
                # Parse taxonomy codes JSON
                taxonomy_codes_raw = json.loads(row[11]) if row[11] else []
                
                # Get taxonomy display names
                taxonomy_display_names = []
                if taxonomy_codes_raw:
                    placeholders = ','.join(['?' for _ in taxonomy_codes_raw])
                    taxonomy_query = f"""
                        SELECT Code, "Display Name" 
                        FROM dim_taxonomy 
                        WHERE Code IN ({placeholders})
                    """
                    cursor.execute(taxonomy_query, taxonomy_codes_raw)
                    taxonomy_rows = cursor.fetchall()
                    
                    # Create mapping of code to display name
                    taxonomy_mapping = {row_tax[0]: row_tax[1] for row_tax in taxonomy_rows}
                    
                    # Build display names list, keeping original codes for unmapped ones
                    for code in taxonomy_codes_raw:
                        display_name = taxonomy_mapping.get(code, code)
                        taxonomy_display_names.append(display_name)
                
                tile = {
                    'payer_slug': row[0],
                    'billing_class': row[1],
                    'negotiation_arrangement': row[2],
                    'negotiated_type': row[3],
                    'proc_set': row[4],
                    'proc_class': row[5],
                    'proc_group': row[6],
                    's3_prefix': row[7],
                    'parts_count': row[8],
                    'row_count': row[9],
                    'billing_codes_json': json.loads(row[10]) if row[10] else [],
                    'taxonomy_codes_json': taxonomy_codes_raw,  # Keep original codes
                    'taxonomy_display_names': taxonomy_display_names,  # Add display names
                    'created_at_utc': row[12]
                }
                tiles.append(tile)
            
            return {
                'available': True,
                'tiles': tiles,
                'total_tiles': len(tiles),
                'total_rows': sum(tile['row_count'] for tile in tiles)
            }
            
        finally:
            conn.close()
    
    def get_episode_templates(self) -> Dict[str, List[Dict]]:
        """
        Get all episode templates from dim_emg_episode, dim_imaging_episode, dim_msk_episode
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            episodes = {}
            
            # EMG Episodes
            cursor.execute("""
                SELECT episode_id, episode_name, clinical_intent, 
                       codes_required, codes_optional, selection_rules
                FROM dim_emg_episode
                ORDER BY episode_name
            """)
            emg_rows = cursor.fetchall()
            
            episodes['emg'] = []
            for row in emg_rows:
                episode = {
                    'episode_id': row[0],
                    'episode_name': row[1],
                    'clinical_intent': row[2],
                    'codes_required': json.loads(row[3]) if row[3] else [],
                    'codes_optional': json.loads(row[4]) if row[4] else [],
                    'selection_rules': row[5],
                    'type': 'emg'
                }
                episodes['emg'].append(episode)
            
            # Imaging Episodes
            cursor.execute("""
                SELECT imaging_id, imaging_name, body_region, modality,
                       codes, selection_rules
                FROM dim_imaging_episode
                ORDER BY imaging_name
            """)
            imaging_rows = cursor.fetchall()
            
            episodes['imaging'] = []
            for row in imaging_rows:
                episode = {
                    'episode_id': row[0],
                    'episode_name': row[1],
                    'body_region': row[2],
                    'modality': row[3],
                    'codes': json.loads(row[4]) if row[4] else [],
                    'selection_rules': row[5],
                    'type': 'imaging'
                }
                episodes['imaging'].append(episode)
            
            # MSK Episodes
            cursor.execute("""
                SELECT episode_id, episode_name, body_region, typical_setting,
                       clinical_intent, codes_required, codes_optional, 
                       rehab_codes, selection_rules
                FROM dim_msk_episode
                ORDER BY episode_name
            """)
            msk_rows = cursor.fetchall()
            
            episodes['msk'] = []
            for row in msk_rows:
                episode = {
                    'episode_id': row[0],
                    'episode_name': row[1],
                    'body_region': row[2],
                    'typical_setting': row[3],
                    'clinical_intent': row[4],
                    'codes_required': json.loads(row[5]) if row[5] else [],
                    'codes_optional': json.loads(row[6]) if row[6] else [],
                    'rehab_codes': json.loads(row[7]) if row[7] else [],
                    'selection_rules': row[8],
                    'type': 'msk'
                }
                episodes['msk'].append(episode)
            
            return episodes
            
        finally:
            conn.close()
    
    def search_s3_tiles_by_codes(self, billing_codes: List[str] = None, 
                                taxonomy_codes: List[str] = None) -> List[Dict]:
        """
        Search s3_tiles table by billing codes and/or taxonomy codes
        Returns taxonomy display names instead of raw codes
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    run_id,
                    payer_slug,
                    billing_class,
                    negotiation_arrangement,
                    negotiated_type,
                    tin_value,
                    proc_set,
                    proc_class,
                    proc_group,
                    s3_prefix,
                    parts_count,
                    row_count,
                    billing_codes_json,
                    taxonomy_codes_json,
                    created_at_utc
                FROM s3_tiles
                WHERE 1=1
            """
            params = []
            
            # Filter by billing codes if provided
            if billing_codes:
                billing_conditions = []
                for code in billing_codes:
                    billing_conditions.append("billing_codes_json LIKE ?")
                    params.append(f'%"{code}"%')
                
                if billing_conditions:
                    query += f" AND ({' OR '.join(billing_conditions)})"
            
            # Filter by taxonomy codes if provided
            if taxonomy_codes:
                taxonomy_conditions = []
                for code in taxonomy_codes:
                    taxonomy_conditions.append("taxonomy_codes_json LIKE ?")
                    params.append(f'%"{code}"%')
                
                if taxonomy_conditions:
                    query += f" AND ({' OR '.join(taxonomy_conditions)})"
            
            query += " ORDER BY created_at_utc DESC, row_count DESC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                # Parse taxonomy codes JSON
                taxonomy_codes_raw = json.loads(row[13]) if row[13] else []
                
                # Get taxonomy display names
                taxonomy_display_names = []
                if taxonomy_codes_raw:
                    placeholders = ','.join(['?' for _ in taxonomy_codes_raw])
                    taxonomy_query = f"""
                        SELECT Code, "Display Name" 
                        FROM dim_taxonomy 
                        WHERE Code IN ({placeholders})
                    """
                    cursor.execute(taxonomy_query, taxonomy_codes_raw)
                    taxonomy_rows = cursor.fetchall()
                    
                    # Create mapping of code to display name
                    taxonomy_mapping = {row_tax[0]: row_tax[1] for row_tax in taxonomy_rows}
                    
                    # Build display names list, keeping original codes for unmapped ones
                    for code in taxonomy_codes_raw:
                        display_name = taxonomy_mapping.get(code, code)
                        taxonomy_display_names.append(display_name)
                
                result = {
                    'run_id': row[0],
                    'payer_slug': row[1],
                    'billing_class': row[2],
                    'negotiation_arrangement': row[3],
                    'negotiated_type': row[4],
                    'tin_value': row[5],
                    'proc_set': row[6],
                    'proc_class': row[7],
                    'proc_group': row[8],
                    's3_prefix': row[9],
                    'parts_count': row[10],
                    'row_count': row[11],
                    'billing_codes_json': json.loads(row[12]) if row[12] else [],
                    'taxonomy_codes_json': taxonomy_codes_raw,  # Keep original codes
                    'taxonomy_display_names': taxonomy_display_names,  # Add display names
                    'created_at_utc': row[14]
                }
                results.append(result)
            
            return results
            
        finally:
            conn.close()
    
    def get_available_payers(self) -> List[str]:
        """Get list of available payers from s3_tiles"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT DISTINCT payer_slug 
                FROM s3_tiles 
                WHERE payer_slug IS NOT NULL 
                ORDER BY payer_slug
            """)
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def get_available_procedure_sets(self) -> List[str]:
        """Get list of available procedure sets from s3_tiles"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT DISTINCT proc_set 
                FROM s3_tiles 
                WHERE proc_set IS NOT NULL 
                ORDER BY proc_set
            """)
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def get_available_taxonomy_codes(self) -> List[Dict]:
        """Get list of available taxonomy codes with display names from s3_tiles"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Get distinct taxonomy codes from s3_tiles
            cursor.execute("""
                SELECT DISTINCT taxonomy_code
                FROM (
                    SELECT json_extract(value, '$') as taxonomy_code
                    FROM s3_tiles, json_each(taxonomy_codes_json)
                    WHERE taxonomy_codes_json IS NOT NULL
                )
                WHERE taxonomy_code IS NOT NULL
                ORDER BY taxonomy_code
            """)
            taxonomy_codes = [row[0] for row in cursor.fetchall()]
            
            # Get display names for these codes
            taxonomy_options = []
            if taxonomy_codes:
                placeholders = ','.join(['?' for _ in taxonomy_codes])
                cursor.execute(f"""
                    SELECT Code, "Display Name" 
                    FROM dim_taxonomy 
                    WHERE Code IN ({placeholders})
                    ORDER BY "Display Name"
                """, taxonomy_codes)
                
                for row in cursor.fetchall():
                    taxonomy_options.append({
                        'code': row[0],
                        'display_name': row[1]
                    })
            
            return taxonomy_options
        finally:
            conn.close()
    
    def get_s3_filter_options(self) -> Dict:
        """Get distinct values for all S3 filter dropdowns"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            options = {}
            
            # Get distinct billing classes
            cursor.execute("SELECT DISTINCT billing_class FROM s3_tiles WHERE billing_class IS NOT NULL ORDER BY billing_class")
            options['billing_classes'] = [row[0] for row in cursor.fetchall()]
            
            # Get distinct negotiation arrangements
            cursor.execute("SELECT DISTINCT negotiation_arrangement FROM s3_tiles WHERE negotiation_arrangement IS NOT NULL ORDER BY negotiation_arrangement")
            options['negotiation_arrangements'] = [row[0] for row in cursor.fetchall()]
            
            # Get distinct negotiated types
            cursor.execute("SELECT DISTINCT negotiated_type FROM s3_tiles WHERE negotiated_type IS NOT NULL ORDER BY negotiated_type")
            options['negotiated_types'] = [row[0] for row in cursor.fetchall()]
            
            # Get distinct procedure sets
            cursor.execute("SELECT DISTINCT proc_set FROM s3_tiles WHERE proc_set IS NOT NULL ORDER BY proc_set")
            options['proc_sets'] = [row[0] for row in cursor.fetchall()]
            
            # Get distinct procedure classes
            cursor.execute("SELECT DISTINCT proc_class FROM s3_tiles WHERE proc_class IS NOT NULL ORDER BY proc_class")
            options['proc_classes'] = [row[0] for row in cursor.fetchall()]
            
            # Get distinct procedure groups
            cursor.execute("SELECT DISTINCT proc_group FROM s3_tiles WHERE proc_group IS NOT NULL ORDER BY proc_group")
            options['proc_groups'] = [row[0] for row in cursor.fetchall()]
            
            # Get distinct billing codes from JSON arrays
            cursor.execute("""
                SELECT DISTINCT billing_code
                FROM (
                    SELECT json_extract(value, '$') as billing_code
                    FROM s3_tiles, json_each(billing_codes_json)
                    WHERE billing_codes_json IS NOT NULL
                )
                WHERE billing_code IS NOT NULL
                ORDER BY billing_code
            """)
            options['billing_codes'] = [row[0] for row in cursor.fetchall()]
            
            return options
        finally:
            conn.close()
    
    def get_data_summary(self) -> Dict:
        """Get overall data availability summary"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            summary = {}
            
            # TIN summary
            cursor.execute("SELECT COUNT(*) FROM dim_tin")
            summary['total_tins'] = cursor.fetchone()[0]
            
            # TIN locations summary
            cursor.execute("SELECT COUNT(*) FROM dim_tin_location")
            summary['total_tin_locations'] = cursor.fetchone()[0]
            
            # S3 tiles summary
            cursor.execute("SELECT COUNT(*) FROM s3_tiles")
            summary['total_s3_tiles'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(row_count) FROM s3_tiles")
            summary['total_rows_in_tiles'] = cursor.fetchone()[0] or 0
            
            # Episode summaries
            cursor.execute("SELECT COUNT(*) FROM dim_emg_episode")
            summary['total_emg_episodes'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dim_imaging_episode")
            summary['total_imaging_episodes'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dim_msk_episode")
            summary['total_msk_episodes'] = cursor.fetchone()[0]
            
            # Unique payers and procedure sets
            cursor.execute("SELECT COUNT(DISTINCT payer_slug) FROM s3_tiles")
            summary['unique_payers'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT proc_set) FROM s3_tiles")
            summary['unique_procedure_sets'] = cursor.fetchone()[0]
            
            return summary
            
        finally:
            conn.close()
