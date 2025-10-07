"""
Medicare Benchmark Lookup Utility

This module provides the MedicareBenchmarkLookup class for retrieving Medicare
professional and institutional rates for benchmarking purposes.

Features:
- Professional rates from SQLite database using medicare_prof_test.sql logic
- Institutional rates from cached parquet files (ASC and OPPS)
- Proper error handling and caching for performance
- Benchmark percentage calculations
"""

import os
import sqlite3
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from functools import lru_cache

logger = logging.getLogger(__name__)


class MedicareBenchmarkLookup:
    """
    Medicare benchmark lookup utility for professional and institutional rates.
    
    This class provides methods to retrieve Medicare rates for benchmarking
    against commercial rates. It handles both professional rates (from SQLite)
    and institutional rates (from parquet files).
    """
    
    def __init__(self):
        """Initialize the Medicare benchmark lookup utility."""
        self.db_path = self._get_db_path()
        self.asc_parquet_path = self._get_asc_parquet_path()
        self.opps_parquet_path = self._get_opps_parquet_path()
        
        # Cache for parquet dataframes
        self._asc_df = None
        self._opps_df = None
        self._asc_df_loaded = False
        self._opps_df_loaded = False
        
        # Validate file existence
        self._validate_files()
        
        logger.info("MedicareBenchmarkLookup initialized successfully")
    
    def _get_db_path(self) -> str:
        """Get the path to the benchmarks SQLite database."""
        return os.path.join(
            Path(__file__).resolve().parent.parent,
            'data',
            'benchmarks',
            'benchmarks.db'
        )
    
    def _get_asc_parquet_path(self) -> str:
        """Get the path to the ASC parquet file."""
        return os.path.join(
            Path(__file__).resolve().parent.parent,
            'data',
            'benchmarks',
            'bench_medicare_asc.parquet'
        )
    
    def _get_opps_parquet_path(self) -> str:
        """Get the path to the OPPS parquet file."""
        return os.path.join(
            Path(__file__).resolve().parent.parent,
            'data',
            'benchmarks',
            'bench_medicare_opps.parquet'
        )
    
    def _validate_files(self) -> None:
        """Validate that all required files exist."""
        missing_files = []
        
        if not os.path.exists(self.db_path):
            missing_files.append(self.db_path)
        
        if not os.path.exists(self.asc_parquet_path):
            missing_files.append(self.asc_parquet_path)
        
        if not os.path.exists(self.opps_parquet_path):
            missing_files.append(self.opps_parquet_path)
        
        if missing_files:
            logger.warning(f"Missing required files: {missing_files}")
            logger.warning("Some functionality may be limited")
    
    def _load_asc_data(self) -> pd.DataFrame:
        """Load and cache ASC parquet data."""
        if not self._asc_df_loaded:
            try:
                if os.path.exists(self.asc_parquet_path):
                    self._asc_df = pd.read_parquet(self.asc_parquet_path)
                    self._asc_df_loaded = True
                    logger.info(f"Loaded ASC data with {len(self._asc_df)} records")
                else:
                    logger.error(f"ASC parquet file not found: {self.asc_parquet_path}")
                    self._asc_df = pd.DataFrame()
            except Exception as e:
                logger.error(f"Error loading ASC parquet data: {str(e)}")
                self._asc_df = pd.DataFrame()
        
        return self._asc_df
    
    def _load_opps_data(self) -> pd.DataFrame:
        """Load and cache OPPS parquet data."""
        if not self._opps_df_loaded:
            try:
                if os.path.exists(self.opps_parquet_path):
                    self._opps_df = pd.read_parquet(self.opps_parquet_path)
                    self._opps_df_loaded = True
                    logger.info(f"Loaded OPPS data with {len(self._opps_df)} records")
                else:
                    logger.error(f"OPPS parquet file not found: {self.opps_parquet_path}")
                    self._opps_df = pd.DataFrame()
            except Exception as e:
                logger.error(f"Error loading OPPS parquet data: {str(e)}")
                self._opps_df = pd.DataFrame()
        
        return self._opps_df
    
    def get_professional_rate(self, cpt_code: str, zip_code: str, year: int = 2025) -> Optional[float]:
        """
        Get Medicare professional rate for a CPT code and zip code.
        
        This method implements the logic from medicare_prof_test.sql to calculate
        the Medicare allowed amount using RVU, GPCI, and conversion factor data.
        
        Args:
            cpt_code (str): CPT procedure code (e.g., '73721')
            zip_code (str): ZIP code (e.g., '15044')
            year (int): Year for the rate calculation (default: 2025)
        
        Returns:
            Optional[float]: Medicare professional rate or None if not found
        """
        try:
            if not os.path.exists(self.db_path):
                logger.error(f"Database file not found: {self.db_path}")
                return None
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Query based on medicare_prof_test.sql logic
                query = """
                SELECT
                    mloc.zip_code,
                    mloc.state_code,
                    meta.state_name,
                    meta.fee_schedule_area,
                    gpci.locality_name,
                    meta.counties,
                    gpci.locality_code,
                    gpci.work_gpci,
                    gpci.pe_gpci,
                    gpci.mp_gpci,
                    rvu.procedure_code,
                    rvu.modifier,
                    rvu.work_rvu,
                    rvu.practice_expense_rvu,
                    rvu.malpractice_rvu,
                    rvu.total_rvu,
                    cf.conversion_factor,
                    -- The actual Medicare allowed amount calculation:
                    (
                      (
                        COALESCE(rvu.work_rvu, 0) * COALESCE(gpci.work_gpci, 0) +
                        COALESCE(rvu.practice_expense_rvu, 0) * COALESCE(gpci.pe_gpci, 0) +
                        COALESCE(rvu.malpractice_rvu, 0) * COALESCE(gpci.mp_gpci, 0)
                      ) * COALESCE(cf.conversion_factor, 0)
                    ) AS allowed_amount
                FROM
                    medicare_locality_map mloc
                JOIN
                    medicare_locality_meta meta
                    ON mloc.carrier_code = meta.mac_code
                    AND mloc.locality_code = meta.locality_code
                JOIN
                    cms_gpci gpci
                    ON TRIM(meta.fee_schedule_area) = TRIM(gpci.locality_name)
                    AND mloc.locality_code = gpci.locality_code
                JOIN
                    cms_rvu rvu
                    ON 1=1
                JOIN
                    cms_conversion_factor cf
                    ON gpci.year = cf.year
                WHERE
                    mloc.zip_code = ?
                    AND gpci.year = ?
                    AND rvu.year = ?
                    AND rvu.procedure_code = ?
                    AND (rvu.modifier IS NULL OR rvu.modifier = '')
                """
                
                cursor.execute(query, (zip_code, year, year, cpt_code))
                result = cursor.fetchone()
                
                if result:
                    allowed_amount = result[17]  # Index 17 is the allowed_amount
                    if allowed_amount is not None:
                        logger.debug(f"Found professional rate for {cpt_code} in {zip_code}: {allowed_amount}")
                        return float(allowed_amount)
                    else:
                        logger.warning(f"No allowed amount calculated for {cpt_code} in {zip_code}")
                        return None
                else:
                    logger.warning(f"No professional rate found for {cpt_code} in {zip_code}")
                    return None
                    
        except sqlite3.Error as e:
            logger.error(f"Database error in get_professional_rate: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_professional_rate: {str(e)}")
            return None
    
    def get_professional_rate_state_avg(self, cpt_code: str, state: str, year: int = 2025) -> Optional[float]:
        """
        Get Medicare professional rate state average for a CPT code and state.
        
        This method calculates the state average by averaging all locality-specific
        rates within the state for the given CPT code.
        
        Args:
            cpt_code (str): CPT procedure code (e.g., '73721')
            state (str): State code (e.g., 'GA')
            year (int): Year for the rate calculation (default: 2025)
        
        Returns:
            Optional[float]: Medicare professional rate state average or None if not found
        """
        try:
            if not os.path.exists(self.db_path):
                logger.error(f"Database file not found: {self.db_path}")
                return None
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Query to calculate state average by averaging all localities in the state
                query = """
                SELECT
                    AVG(
                        (
                          (
                            COALESCE(rvu.work_rvu, 0) * COALESCE(gpci.work_gpci, 0) +
                            COALESCE(rvu.practice_expense_rvu, 0) * COALESCE(gpci.pe_gpci, 0) +
                            COALESCE(rvu.malpractice_rvu, 0) * COALESCE(gpci.mp_gpci, 0)
                          ) * COALESCE(cf.conversion_factor, 0)
                        )
                    ) AS state_avg_allowed_amount
                FROM
                    medicare_locality_map mloc
                JOIN
                    medicare_locality_meta meta
                    ON mloc.carrier_code = meta.mac_code
                    AND mloc.locality_code = meta.locality_code
                JOIN
                    cms_gpci gpci
                    ON TRIM(meta.fee_schedule_area) = TRIM(gpci.locality_name)
                    AND mloc.locality_code = gpci.locality_code
                JOIN
                    cms_rvu rvu
                    ON 1=1
                JOIN
                    cms_conversion_factor cf
                    ON gpci.year = cf.year
                WHERE
                    mloc.state_code = ?
                    AND gpci.year = ?
                    AND rvu.year = ?
                    AND rvu.procedure_code = ?
                    AND (rvu.modifier IS NULL OR rvu.modifier = '')
                """
                
                cursor.execute(query, (state.upper(), year, year, cpt_code))
                result = cursor.fetchone()
                
                if result and result[0] is not None:
                    state_avg_amount = result[0]
                    logger.debug(f"Found state average professional rate for {cpt_code} in {state}: {state_avg_amount}")
                    return float(state_avg_amount)
                else:
                    logger.warning(f"No state average professional rate found for {cpt_code} in {state}")
                    return None
                    
        except sqlite3.Error as e:
            logger.error(f"Database error in get_professional_rate_state_avg: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_professional_rate_state_avg: {str(e)}")
            return None
    
    def get_institutional_rates(self, cpt_code: str, state: str, year: int = 2025) -> Dict[str, Optional[float]]:
        """
        Get Medicare institutional rates (ASC and OPPS) for a CPT code and state.
        
        Args:
            cpt_code (str): CPT procedure code (e.g., '73721')
            state (str): State code (e.g., 'GA')
            year (int): Year for the rate calculation (default: 2025)
        
        Returns:
            Dict[str, Optional[float]]: Dictionary with 'medicare_asc_stateavg' and 
                                      'medicare_opps_stateavg' keys
        """
        result = {
            'medicare_asc_stateavg': None,
            'medicare_opps_stateavg': None
        }
        
        try:
            # Get ASC rate
            asc_rate = self._get_asc_rate(cpt_code, state, year)
            result['medicare_asc_stateavg'] = asc_rate
            
            # Get OPPS rate
            opps_rate = self._get_opps_rate(cpt_code, state, year)
            result['medicare_opps_stateavg'] = opps_rate
            
            logger.debug(f"Found institutional rates for {cpt_code} in {state}: ASC={asc_rate}, OPPS={opps_rate}")
            
        except Exception as e:
            logger.error(f"Error getting institutional rates for {cpt_code} in {state}: {str(e)}")
        
        return result
    
    def _get_asc_rate(self, cpt_code: str, state: str, year: int) -> Optional[float]:
        """Get ASC rate from parquet data."""
        try:
            asc_df = self._load_asc_data()
            if asc_df.empty:
                return None
            
            # Filter for matching records
            filtered = asc_df[
                (asc_df['code'] == cpt_code) & 
                (asc_df['state'] == state.upper()) & 
                (asc_df['data_year'] == year)
            ]
            
            if not filtered.empty:
                # Return the first matching rate
                rate = filtered['medicare_asc_stateavg'].iloc[0]
                return float(rate) if pd.notna(rate) else None
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting ASC rate for {cpt_code} in {state}: {str(e)}")
            return None
    
    def _get_opps_rate(self, cpt_code: str, state: str, year: int) -> Optional[float]:
        """Get OPPS rate from parquet data."""
        try:
            opps_df = self._load_opps_data()
            if opps_df.empty:
                return None
            
            # Filter for matching records
            filtered = opps_df[
                (opps_df['code'] == cpt_code) & 
                (opps_df['state'] == state.upper()) & 
                (opps_df['data_year'] == year)
            ]
            
            if not filtered.empty:
                # Return the first matching rate
                rate = filtered['medicare_opps_stateavg'].iloc[0]
                return float(rate) if pd.notna(rate) else None
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting OPPS rate for {cpt_code} in {state}: {str(e)}")
            return None
    
    @staticmethod
    def calculate_benchmark_percentage(negotiated_rate: float, medicare_rate: float) -> Optional[float]:
        """
        Calculate benchmark percentage (negotiated rate as % of Medicare rate).
        
        Args:
            negotiated_rate (float): The negotiated commercial rate
            medicare_rate (float): The Medicare benchmark rate
        
        Returns:
            Optional[float]: Benchmark percentage or None if calculation not possible
        """
        try:
            if medicare_rate is None or medicare_rate <= 0:
                logger.warning("Cannot calculate benchmark percentage: invalid Medicare rate")
                return None
            
            if negotiated_rate is None or negotiated_rate < 0:
                logger.warning("Cannot calculate benchmark percentage: invalid negotiated rate")
                return None
            
            percentage = (negotiated_rate / medicare_rate) * 100
            logger.debug(f"Benchmark percentage: {negotiated_rate} / {medicare_rate} = {percentage:.2f}%")
            return round(percentage, 2)
            
        except (TypeError, ValueError, ZeroDivisionError) as e:
            logger.error(f"Error calculating benchmark percentage: {str(e)}")
            return None
    
    def get_comprehensive_rates(self, cpt_code: str, zip_code: str, state: str, year: int = 2025, use_state_avg: bool = False) -> Dict[str, Any]:
        """
        Get comprehensive Medicare rates for both professional and institutional settings.
        
        Args:
            cpt_code (str): CPT procedure code
            zip_code (str): ZIP code for professional rates (ignored if use_state_avg=True)
            state (str): State code for institutional rates and state average professional rates
            year (int): Year for rate calculations
            use_state_avg (bool): If True, use state average for professional rates instead of ZIP-specific
        
        Returns:
            Dict[str, Any]: Comprehensive rate information including benchmark percentages
        """
        result = {
            'professional_rate': None,
            'professional_rate_state_avg': None,
            'institutional_rates': {
                'medicare_asc_stateavg': None,
                'medicare_opps_stateavg': None
            },
            'benchmark_percentages': {},
            'metadata': {
                'cpt_code': cpt_code,
                'zip_code': zip_code,
                'state': state,
                'year': year,
                'use_state_avg': use_state_avg
            }
        }
        
        try:
            if use_state_avg:
                # Get state average professional rate
                prof_rate_state_avg = self.get_professional_rate_state_avg(cpt_code, state, year)
                result['professional_rate_state_avg'] = prof_rate_state_avg
                result['professional_rate'] = prof_rate_state_avg  # For backward compatibility
            else:
                # Get ZIP-specific professional rate
                prof_rate = self.get_professional_rate(cpt_code, zip_code, year)
                result['professional_rate'] = prof_rate
            
            # Get institutional rates
            inst_rates = self.get_institutional_rates(cpt_code, state, year)
            result['institutional_rates'] = inst_rates
            
            # Calculate benchmark percentages if we have a negotiated rate
            # Note: This method doesn't take a negotiated rate parameter, but provides
            # the structure for calculating percentages when a negotiated rate is available
            
            logger.info(f"Retrieved comprehensive rates for {cpt_code}: "
                       f"Professional={'State Avg' if use_state_avg else 'ZIP-specific'}={result['professional_rate']}, "
                       f"ASC={inst_rates['medicare_asc_stateavg']}, "
                       f"OPPS={inst_rates['medicare_opps_stateavg']}")
            
        except Exception as e:
            logger.error(f"Error getting comprehensive rates for {cpt_code}: {str(e)}")
        
        return result
    
    def clear_cache(self) -> None:
        """Clear the cached parquet data to force reload."""
        self._asc_df = None
        self._opps_df = None
        self._asc_df_loaded = False
        self._opps_df_loaded = False
        logger.info("Cleared parquet data cache")
    
    def get_cache_status(self) -> Dict[str, bool]:
        """Get the current cache status."""
        return {
            'asc_data_loaded': self._asc_df_loaded,
            'opps_data_loaded': self._opps_df_loaded,
            'asc_data_size': len(self._asc_df) if self._asc_df is not None else 0,
            'opps_data_size': len(self._opps_df) if self._opps_df is not None else 0
        }
