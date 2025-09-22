# Create this as cleanup_benchmarks_db.py
import sqlite3
import os
import shutil
from pathlib import Path

def cleanup_benchmarks_database():
    """Clean up the benchmarks database by keeping only required tables."""
    
    db_path = "core/data/benchmarks/benchmarks.db"
    backup_path = "core/data/benchmarks/benchmarks_backup.db"
    new_db_path = "core/data/benchmarks/benchmarks_clean.db"
    
    print(f"Original database size: {os.path.getsize(db_path) / (1024*1024):.1f} MB")
    
    # Required tables for Medicare benchmarks
    required_tables = [
        'medicare_locality_map',
        'medicare_locality_meta', 
        'cms_gpci',
        'cms_rvu',
        'cms_conversion_factor'
    ]
    
    try:
        # Connect to original database
        conn_old = sqlite3.connect(db_path)
        cursor_old = conn_old.cursor()
        
        # Check what tables exist
        cursor_old.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cursor_old.fetchall()]
        
        print(f"Found {len(existing_tables)} tables in original database")
        print(f"Required tables: {required_tables}")
        
        # Check if all required tables exist
        missing_tables = [table for table in required_tables if table not in existing_tables]
        if missing_tables:
            print(f"ERROR: Missing required tables: {missing_tables}")
            return False
        
        # Create new database
        conn_new = sqlite3.connect(new_db_path)
        cursor_new = conn_new.cursor()
        
        # Copy required tables
        for table in required_tables:
            print(f"Copying table: {table}")
            
            # Get table schema
            cursor_old.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
            schema = cursor_old.fetchone()[0]
            
            # Create table in new database
            cursor_new.execute(schema)
            
            # Copy data
            cursor_old.execute(f"SELECT * FROM {table}")
            rows = cursor_old.fetchall()
            
            # Insert data
            placeholders = ','.join(['?' for _ in range(len(rows[0]) if rows else 0)])
            cursor_new.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
            
            print(f"  - Copied {len(rows)} rows")
        
        # Create indexes for better performance
        print("Creating indexes...")
        indexes = [
            "CREATE INDEX idx_medicare_locality_map_zip ON medicare_locality_map(zip_code)",
            "CREATE INDEX idx_medicare_locality_map_carrier ON medicare_locality_map(carrier_code, locality_code)",
            "CREATE INDEX idx_medicare_locality_meta_mac ON medicare_locality_meta(mac_code, locality_code)",
            "CREATE INDEX idx_cms_gpci_locality ON cms_gpci(locality_name, locality_code, year)",
            "CREATE INDEX idx_cms_rvu_procedure ON cms_rvu(procedure_code, year)",
            "CREATE INDEX idx_cms_conversion_factor_year ON cms_conversion_factor(year)"
        ]
        
        for index_sql in indexes:
            try:
                cursor_new.execute(index_sql)
                print(f"  - Created index: {index_sql.split('idx_')[1].split(' ')[0]}")
            except Exception as e:
                print(f"  - Index creation failed (may already exist): {e}")
        
        # Commit and close connections
        conn_new.commit()
        conn_new.close()
        conn_old.close()
        
        # Check sizes
        old_size = os.path.getsize(db_path) / (1024*1024)
        new_size = os.path.getsize(new_db_path) / (1024*1024)
        
        print(f"\nDatabase cleanup complete!")
        print(f"Original size: {old_size:.1f} MB")
        print(f"New size: {new_size:.1f} MB")
        print(f"Space saved: {old_size - new_size:.1f} MB ({(old_size - new_size)/old_size*100:.1f}%)")
        
        # Verify new database works
        conn_test = sqlite3.connect(new_db_path)
        cursor_test = conn_test.cursor()
        
        print(f"\nVerification:")
        for table in required_tables:
            cursor_test.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor_test.fetchone()[0]
            print(f"  - {table}: {count:,} rows")
        
        conn_test.close()
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    cleanup_benchmarks_database()