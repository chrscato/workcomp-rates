#!/usr/bin/env python3
"""
Test script to verify parquet data manager functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.utils.parquet_utils import ParquetDataManager

def test_parquet_manager():
    print("Testing ParquetDataManager...")
    
    # Initialize the data manager
    dm = ParquetDataManager()
    
    print(f"Data file path: {dm.file_path}")
    print(f"Has data: {dm.has_data}")
    
    if dm.has_data:
        print("✓ Parquet file found!")
        
        # Test getting unique values
        print("\nTesting unique values...")
        payers = dm.get_unique_values('payer')
        print(f"Payers: {payers[:5]}...")  # Show first 5
        
        orgs = dm.get_unique_values('org_name')
        print(f"Organizations: {orgs[:5]}...")  # Show first 5
        
        billing_classes = dm.get_unique_values('billing_class')
        print(f"Billing classes: {billing_classes}")
        
        # Test getting aggregated stats
        print("\nTesting aggregated stats...")
        stats = dm.get_aggregated_stats()
        
        print("Professional stats:")
        for key, value in stats['professional'].items():
            print(f"  {key}: {value}")
            
        print("\nFacility stats:")
        for key, value in stats['facility'].items():
            print(f"  {key}: {value}")
        
        # Test getting sample records
        print("\nTesting sample records...")
        sample_records = dm.get_sample_records(limit=3)
        print(f"Sample records: {len(sample_records)}")
        for i, record in enumerate(sample_records):
            print(f"  Record {i+1}: {record['billing_code']} - {record['org_name']} - ${record['rate']}")
            
    else:
        print("✗ Parquet file not found!")
        print("Using sample data...")
        
        # Test with sample data
        stats = dm.get_aggregated_stats()
        print("Sample data stats:")
        print(f"Professional: {stats['professional']}")
        print(f"Facility: {stats['facility']}")

if __name__ == "__main__":
    test_parquet_manager()
