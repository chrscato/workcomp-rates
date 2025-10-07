import json
import sqlite3
import math
from typing import List, Dict, Optional, Tuple
from django.conf import settings
import os
import urllib.request
import urllib.parse
import urllib.error


class MarketAnalyzer:
    """
    Analyzes healthcare markets by finding TINs within a geographic radius of a given zip code.
    Leverages the geo.py functionality for geocoding and dim_tin_location for TIN coordinates.
    """
    
    def __init__(self):
        self.db_path = os.path.join(settings.BASE_DIR, 'core', 'data', 'dims.sqlite')
        self.geocoding_endpoint = "https://geocoding-api.open-meteo.com/v1/search"
        
    def get_db_connection(self):
        """Get SQLite database connection"""
        return sqlite3.connect(self.db_path)
    
    def geocode_zip_code(self, zip_code: str) -> Optional[Tuple[float, float]]:
        """
        Geocode a zip code to get latitude and longitude coordinates.
        Uses the Open-Meteo geocoding API with built-in urllib instead of requests.
        """
        try:
            # Normalize zip code to 5 digits
            zip_code = str(zip_code).strip()
            if len(zip_code) == 10 and '-' in zip_code:
                # Handle ZIP+4 format (12345-6789)
                zip_code = zip_code.split('-')[0]
            elif len(zip_code) > 5:
                # Take first 5 digits
                zip_code = zip_code[:5]
            
            # Ensure it's numeric
            if not zip_code.isdigit():
                return None
            
            # Use Open-Meteo geocoding API with urllib
            url = f"{self.geocoding_endpoint}?name={zip_code}&count=1"
            
            # Create request with proper headers
            req = urllib.request.Request(url, headers={'User-Agent': 'MarketAnalyzer/1.0'})
            
            # Make the request
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    
                    if "results" in data and len(data["results"]) > 0:
                        result = data["results"][0]
                        lat = result.get("latitude")
                        lon = result.get("longitude")
                        
                        if lat and lon:
                            return float(lat), float(lon)
            
            return None
            
        except Exception as e:
            print(f"Error geocoding zip code {zip_code}: {str(e)}")
            return None
    
    
    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate the great circle distance between two points on Earth in miles.
        Uses the Haversine formula.
        """
        # Convert latitude and longitude from degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Radius of earth in miles
        r = 3959
        return c * r
    
    def find_nearby_tins(self, zip_code: str, radius_miles: float = 25.0) -> Dict:
        """
        Find all TINs within a specified radius of a given zip code.
        
        Args:
            zip_code: The zip code to search from
            radius_miles: Search radius in miles (default 25 miles)
            
        Returns:
            Dictionary containing:
            - search_location: The geocoded coordinates of the zip code
            - radius_miles: The search radius used
            - nearby_tins: List of TINs within the radius with distance info
            - total_tins: Total number of TINs found
        """
        # Geocode the zip code
        coordinates = self.geocode_zip_code(zip_code)
        if not coordinates:
            return {
                'error': f'Could not geocode zip code: {zip_code}',
                'search_location': None,
                'radius_miles': radius_miles,
                'nearby_tins': [],
                'total_tins': 0
            }
        
        search_lat, search_lng = coordinates
        
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Get all TIN locations with coordinates
            cursor.execute("""
                SELECT 
                    dtl.tin_value,
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
                    dtl.npi_list_json,
                    dtl.last_updated
                FROM dim_tin_location dtl
                JOIN dim_tin dt ON dtl.tin_value = dt.tin_value
                WHERE dtl.latitude IS NOT NULL 
                AND dtl.longitude IS NOT NULL
            """)
            
            rows = cursor.fetchall()
            
            nearby_tins = []
            
            for row in rows:
                tin_lat = row[7]
                tin_lng = row[8]
                
                if tin_lat and tin_lng:
                    # Calculate distance
                    distance = self.calculate_distance(search_lat, search_lng, tin_lat, tin_lng)
                    
                    if distance <= radius_miles:
                        # Parse NPI list JSON
                        npi_list = json.loads(row[11]) if row[11] else []
                        
                        tin_info = {
                            'tin_value': row[0],
                            'organization_name': row[1],
                            'address_1': row[2],
                            'address_2': row[3],
                            'city': row[4],
                            'state': row[5],
                            'zip_norm': row[6],
                            'latitude': tin_lat,
                            'longitude': tin_lng,
                            'distance_miles': round(distance, 2),
                            'support_npi_count': row[9],
                            'primary_flag': row[10],
                            'npi_list_json': npi_list,
                            'last_updated': row[12]
                        }
                        nearby_tins.append(tin_info)
            
            # Sort by distance (S3 availability will be sorted later)
            nearby_tins.sort(key=lambda x: x['distance_miles'])
            
            return {
                'search_location': {
                    'zip_code': zip_code,
                    'latitude': search_lat,
                    'longitude': search_lng
                },
                'radius_miles': radius_miles,
                'nearby_tins': nearby_tins,
                'total_tins': len(nearby_tins)
            }
            
        except Exception as e:
            print(f"Database error in find_nearby_tins: {str(e)}")
            return {
                'error': f'Database error: {str(e)}',
                'search_location': {
                    'zip_code': zip_code,
                    'latitude': search_lat,
                    'longitude': search_lng
                },
                'radius_miles': radius_miles,
                'nearby_tins': [],
                'total_tins': 0
            }
        finally:
            if 'conn' in locals():
                conn.close()
    
    def get_tin_s3_availability(self, tin_value: str, filters: Dict = None) -> Dict:
        """
        Check S3 data availability for a specific TIN with optional filters.
        Reuses logic from MarketRateLookup.
        """
        from .market_rate_lookup import MarketRateLookup
        lookup = MarketRateLookup()
        
        # Get base S3 availability
        s3_data = lookup.check_s3_tiles_availability(tin_value)
        
        # If no filters, return as-is
        if not filters or not s3_data.get('available'):
            return s3_data
        
        # Apply filters to the tiles
        if filters and s3_data.get('tiles'):
            filtered_tiles = []
            
            for tile in s3_data['tiles']:
                # Check if tile matches all filter criteria
                matches = True
                
                if filters.get('billing_class') and tile.get('billing_class') != filters['billing_class']:
                    matches = False
                
                if filters.get('negotiation_arrangement') and tile.get('negotiation_arrangement') != filters['negotiation_arrangement']:
                    matches = False
                
                if filters.get('negotiated_type') and tile.get('negotiated_type') != filters['negotiated_type']:
                    matches = False
                
                if filters.get('proc_set') and tile.get('proc_set') != filters['proc_set']:
                    matches = False
                
                if filters.get('proc_class') and filters['proc_class'].lower() not in (tile.get('proc_class') or '').lower():
                    matches = False
                
                if filters.get('proc_group') and filters['proc_group'].lower() not in (tile.get('proc_group') or '').lower():
                    matches = False
                
                if filters.get('billing_code'):
                    billing_codes = tile.get('billing_codes_json', [])
                    if filters['billing_code'] not in billing_codes:
                        matches = False
                
                if matches:
                    filtered_tiles.append(tile)
            
            # Update S3 data with filtered results
            s3_data['tiles'] = filtered_tiles
            s3_data['total_tiles'] = len(filtered_tiles)
            s3_data['total_rows'] = sum(tile.get('row_count', 0) for tile in filtered_tiles)
            s3_data['available'] = len(filtered_tiles) > 0
            s3_data['filters_applied'] = filters
        
        return s3_data
    
    def analyze_market_with_s3_data(self, zip_code: str, radius_miles: float = 25.0, filters: Dict = None) -> Dict:
        """
        Complete market analysis including S3 data availability for nearby TINs.
        
        Returns:
            Dictionary with nearby TINs and their S3 data availability status
        """
        # Find nearby TINs
        market_data = self.find_nearby_tins(zip_code, radius_miles)
        
        if 'error' in market_data:
            return market_data
        
        # Add S3 availability for each TIN with filters
        for tin_info in market_data['nearby_tins']:
            s3_data = self.get_tin_s3_availability(tin_info['tin_value'], filters)
            tin_info['s3_availability'] = s3_data
        
        # Sort by S3 data availability first, then by distance
        market_data['nearby_tins'].sort(key=lambda x: (not x.get('s3_availability', {}).get('available', False), x['distance_miles']))
        
        # Add summary statistics
        market_data['summary'] = {
            'tins_with_s3_data': sum(1 for tin in market_data['nearby_tins'] 
                                   if tin['s3_availability'].get('available', False)),
            'total_s3_tiles': sum(tin['s3_availability'].get('total_tiles', 0) 
                                for tin in market_data['nearby_tins']),
            'total_s3_rows': sum(tin['s3_availability'].get('total_rows', 0) 
                               for tin in market_data['nearby_tins'])
        }
        
        return market_data
    
    def get_market_statistics(self, market_data: Dict) -> Dict:
        """
        Generate comprehensive statistics for a market analysis.
        """
        if 'nearby_tins' not in market_data:
            return {}
        
        tins = market_data['nearby_tins']
        
        # Basic statistics
        stats = {
            'total_tins': len(tins),
            'tins_with_coordinates': len([t for t in tins if t.get('latitude') and t.get('longitude')]),
            'total_npis': sum(t.get('support_npi_count', 0) for t in tins),
            'primary_locations': len([t for t in tins if t.get('primary_flag')]),
        }
        
        # Distance statistics
        distances = [t.get('distance_miles', 0) for t in tins]
        if distances:
            stats['distance_stats'] = {
                'min_distance': min(distances),
                'max_distance': max(distances),
                'avg_distance': sum(distances) / len(distances)
            }
        
        # S3 data statistics
        s3_stats = {
            'tins_with_s3_data': 0,
            'total_s3_tiles': 0,
            'total_s3_rows': 0
        }
        
        for tin in tins:
            s3_data = tin.get('s3_availability', {})
            if s3_data.get('available'):
                s3_stats['tins_with_s3_data'] += 1
                s3_stats['total_s3_tiles'] += s3_data.get('total_tiles', 0)
                s3_stats['total_s3_rows'] += s3_data.get('total_rows', 0)
        
        stats['s3_data'] = s3_stats
        
        return stats
    
    def test_geocoding(self, zip_code: str) -> Dict:
        """
        Test function to debug geocoding issues.
        Returns detailed information about the geocoding process.
        """
        result = {
            'zip_code': zip_code,
            'normalized_zip': None,
            'api_result': None,
            'final_result': None,
            'errors': []
        }
        
        try:
            # Test zip code normalization
            original_zip = str(zip_code).strip()
            if len(original_zip) == 10 and '-' in original_zip:
                normalized_zip = original_zip.split('-')[0]
            elif len(original_zip) > 5:
                normalized_zip = original_zip[:5]
            else:
                normalized_zip = original_zip
            
            if not normalized_zip.isdigit():
                result['errors'].append(f"Invalid zip code format: {normalized_zip}")
                return result
            
            result['normalized_zip'] = normalized_zip
            
            # Test Open-Meteo API with urllib
            try:
                url = f"{self.geocoding_endpoint}?name={normalized_zip}&count=1"
                req = urllib.request.Request(url, headers={'User-Agent': 'MarketAnalyzer/1.0'})
                
                with urllib.request.urlopen(req, timeout=10) as response:
                    status_code = response.status
                    response_data = json.loads(response.read().decode('utf-8'))
                    
                    result['api_result'] = {
                        'status_code': status_code,
                        'url': url,
                        'response_data': response_data
                    }
                    
                    if status_code == 200:
                        if "results" in response_data and len(response_data["results"]) > 0:
                            api_result = response_data["results"][0]
                            lat = api_result.get("latitude")
                            lon = api_result.get("longitude")
                            name = api_result.get("name", "Unknown")
                            
                            result['api_result']['location_name'] = name
                            
                            if lat and lon:
                                result['final_result'] = (float(lat), float(lon))
                                return result
                        else:
                            result['errors'].append("No results found in API response")
                    else:
                        result['errors'].append(f"API returned status code: {status_code}")
                
            except Exception as e:
                result['errors'].append(f"Open-Meteo API error: {str(e)}")
            
            result['errors'].append("Geocoding failed")
            
        except Exception as e:
            result['errors'].append(f"General error: {str(e)}")
        
        return result
