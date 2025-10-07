#!/usr/bin/env python3
"""
Test script for Medicare professional rate state average calculation.

This script demonstrates how to use the new state average functionality
for Medicare professional rates instead of ZIP code-specific rates.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.utils.medicare_benchmarks import MedicareBenchmarkLookup

def test_state_average_calculation():
    """Test the new state average Medicare professional rate calculation."""
    
    print("Testing Medicare Professional Rate State Average Calculation")
    print("=" * 60)
    
    # Initialize the Medicare lookup
    try:
        medicare_lookup = MedicareBenchmarkLookup()
        print("✓ MedicareBenchmarkLookup initialized successfully")
    except Exception as e:
        print(f"✗ Error initializing MedicareBenchmarkLookup: {e}")
        return
    
    # Test parameters
    test_cases = [
        {
            'cpt_code': '73721',  # MRI of knee
            'state': 'GA',
            'year': 2025
        },
        {
            'cpt_code': '99213',  # Office visit
            'state': 'GA', 
            'year': 2025
        },
        {
            'cpt_code': '73721',
            'state': 'CA',
            'year': 2025
        }
    ]
    
    print(f"\nTesting {len(test_cases)} cases:")
    print("-" * 40)
    
    for i, case in enumerate(test_cases, 1):
        print(f"\nTest Case {i}: {case['cpt_code']} in {case['state']}")
        
        try:
            # Get state average professional rate
            state_avg_rate = medicare_lookup.get_professional_rate_state_avg(
                case['cpt_code'], 
                case['state'], 
                case['year']
            )
            
            if state_avg_rate is not None:
                print(f"  ✓ State Average Rate: ${state_avg_rate:.2f}")
            else:
                print(f"  ✗ No state average rate found")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    # Test comprehensive rates with state average
    print(f"\nTesting comprehensive rates with state average:")
    print("-" * 50)
    
    try:
        comprehensive_rates = medicare_lookup.get_comprehensive_rates(
            cpt_code='73721',
            zip_code='30309',  # This will be ignored when use_state_avg=True
            state='GA',
            year=2025,
            use_state_avg=True
        )
        
        print(f"Comprehensive rates for 73721 in GA (state average):")
        print(f"  Professional Rate: ${comprehensive_rates['professional_rate']:.2f}")
        print(f"  Professional Rate State Avg: ${comprehensive_rates['professional_rate_state_avg']:.2f}")
        print(f"  ASC Rate: ${comprehensive_rates['institutional_rates']['medicare_asc_stateavg']:.2f}")
        print(f"  OPPS Rate: ${comprehensive_rates['institutional_rates']['medicare_opps_stateavg']:.2f}")
        print(f"  Use State Avg: {comprehensive_rates['metadata']['use_state_avg']}")
        
    except Exception as e:
        print(f"✗ Error getting comprehensive rates: {e}")

if __name__ == "__main__":
    test_state_average_calculation()
