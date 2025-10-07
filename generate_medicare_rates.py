#!/usr/bin/env python3
"""
Simple script to generate Medicare professional rates table.
Uses 3 simple queries:
1. Get distinct state codes
2. Get distinct procedure codes  
3. Run the state average query for each combination
"""

import sqlite3
import pandas as pd
import os
from pathlib import Path

def get_distinct_states():
    """Get all distinct state codes from medicare_locality_map."""
    db_path = os.path.join(
        Path(__file__).resolve().parent,
        'core', 'data', 'benchmarks',
        'benchmarks.db'
    )
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT state_code FROM medicare_locality_map ORDER BY state_code")
        return [row[0] for row in cursor.fetchall()]

def get_distinct_procedure_codes():
    """Get all distinct procedure codes from cms_rvu."""
    db_path = os.path.join(
        Path(__file__).resolve().parent,
        'core', 'data', 'benchmarks',
        'benchmarks.db'
    )
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT procedure_code
            FROM cms_rvu
            WHERE procedure_code IS NOT NULL
            AND procedure_code != ''
            AND (modifier IS NULL OR modifier = '')
            ORDER BY procedure_code
        """)
        return [row[0] for row in cursor.fetchall()]

def get_state_avg_rate(procedure_code, state_code, year=2025):
    """Get state average rate for a specific procedure code and state."""
    db_path = os.path.join(
        Path(__file__).resolve().parent,
        'core', 'data', 'benchmarks',
        'benchmarks.db'
    )
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
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
        """, (state_code, year, year, procedure_code))
        
        result = cursor.fetchone()
        return result[0] if result and result[0] is not None else None

def main():
    """Generate Medicare professional rates table."""
    print("Generating Medicare Professional Rates Table")
    print("=" * 50)
    
    # Step 1: Get distinct state codes
    print("1. Getting distinct state codes...")
    states = get_distinct_states()
    print(f"   Found {len(states)} states: {', '.join(states[:10])}{'...' if len(states) > 10 else ''}")
    
    # Step 2: Get distinct procedure codes
    print("2. Getting distinct procedure codes...")
    procedure_codes = get_distinct_procedure_codes()
    print(f"   Found {len(procedure_codes)} procedure codes: {', '.join(procedure_codes[:10])}{'...' if len(procedure_codes) > 10 else ''}")
    
    # Step 3: Generate combinations and calculate rates
    print("3. Calculating state average rates...")
    results = []
    total_combinations = len(states) * len(procedure_codes)
    processed = 0
    
    for procedure_code in procedure_codes:
        for state_code in states:
            rate = get_state_avg_rate(procedure_code, state_code)
            
            results.append({
                'procedure_code': procedure_code,
                'state_code': state_code,
                'year': 2025,
                'medicare_professional_rate': rate,
                'rate_type': 'state_average'
            })
            
            processed += 1
            if processed % 1000 == 0:
                print(f"   Processed {processed}/{total_combinations} combinations...")
    
    # Create DataFrame and save to parquet
    df = pd.DataFrame(results)
    
    # Save to parquet file
    output_path = os.path.join(
        Path(__file__).resolve().parent,
        'core', 'data', 'benchmarks',
        'medicare_professional_rates.parquet'
    )
    
    df.to_parquet(output_path, index=False)
    
    # Summary
    valid_rates = df[df['medicare_professional_rate'].notna()]
    print(f"\n[SUCCESS] Generated table with {len(results)} records")
    print(f"  Valid rates: {len(valid_rates)}")
    print(f"  Output: {output_path}")
    
    if len(valid_rates) > 0:
        min_rate = valid_rates['medicare_professional_rate'].min()
        max_rate = valid_rates['medicare_professional_rate'].max()
        print(f"  Rate range: ${min_rate:.2f} - ${max_rate:.2f}")
    
    print(f"\nSample data:")
    print(valid_rates.head().to_string(index=False))

if __name__ == "__main__":
    main()
