# Database Field Mapping - Partition Navigation System

## Overview
This document shows exactly which database tables each filter field is pulled from in the tile-based partition navigation system.

## Database Tables Used

### 1. `partitions` Table (Main Data Table)
**Primary table containing all partition metadata**

| Field | Source Table | Query | Description |
|-------|-------------|-------|-------------|
| `state` | `partitions` | `SELECT DISTINCT state FROM partitions WHERE state IS NOT NULL ORDER BY state` | All available states/territories |
| `billing_class` | `partitions` | `SELECT DISTINCT billing_class FROM partitions WHERE billing_class IS NOT NULL ORDER BY billing_class` | Billing class types (institutional, professional) |
| `procedure_set` | `partitions` | `SELECT DISTINCT procedure_set FROM partitions WHERE procedure_set IS NOT NULL ORDER BY procedure_set` | Procedure set categories |
| `stat_area_name` | `partitions` | `SELECT DISTINCT stat_area_name FROM partitions WHERE stat_area_name IS NOT NULL ORDER BY stat_area_name` | Statistical area names (CBSA) |
| `year` | `partitions` | `SELECT DISTINCT year FROM partitions WHERE year IS NOT NULL ORDER BY year DESC` | Available years |
| `month` | `partitions` | `SELECT DISTINCT month FROM partitions WHERE month IS NOT NULL ORDER BY month DESC` | Available months |

### 2. `dim_payers` Table (Dimension Table)
**Payer information with display names**

| Field | Source Table | Query | Description |
|-------|-------------|-------|-------------|
| `payer_slug` | `dim_payers` | `SELECT DISTINCT payer_slug, payer_display_name FROM dim_payers ORDER BY payer_display_name` | Payer slugs with display names |

### 3. `dim_taxonomies` Table (Dimension Table)
**Medical specialty taxonomy codes and descriptions**

| Field | Source Table | Query | Description |
|-------|-------------|-------|-------------|
| `taxonomy_code` | `dim_taxonomies` | `SELECT DISTINCT taxonomy_code, taxonomy_desc FROM dim_taxonomies ORDER BY taxonomy_desc` | Taxonomy codes with descriptions |

## Filter Categories

### Required Filters (Tier 1)
These filters are mandatory for searching partitions:

1. **Payer** (`payer_slug`)
   - **Table**: `dim_payers`
   - **Format**: `payer_slug|display_name`
   - **Example**: `unitedhealthcare-of-georgia-inc|UnitedHealthcare of Georgia Inc`

2. **State** (`state`)
   - **Table**: `partitions`
   - **Format**: `state_code`
   - **Example**: `GA`, `CA`, `NY`

3. **Billing Class** (`billing_class`)
   - **Table**: `partitions`
   - **Format**: `billing_class_name`
   - **Example**: `institutional`, `professional`

### Optional Filters (Tier 2)
These filters refine the search but are not required:

4. **Procedure Set** (`procedure_set`)
   - **Table**: `partitions`
   - **Format**: `procedure_set_name`
   - **Example**: `Evaluation and Management`, `Imaging`, `Procedures`

5. **Medical Specialty** (`taxonomy_code`)
   - **Table**: `dim_taxonomies`
   - **Format**: `taxonomy_code|description`
   - **Example**: `207LP3000X|Anesthesiology, Pediatric Anesthesiology`

6. **Statistical Area** (`stat_area_name`)
   - **Table**: `partitions`
   - **Format**: `area_name`
   - **Example**: `Atlanta-Sandy Springs-Alpharetta, GA MSA`

### Temporal Filters (Tier 3)
These filters specify time periods:

7. **Year** (`year`)
   - **Table**: `partitions`
   - **Format**: `year_number`
   - **Example**: `2025`

8. **Month** (`month`)
   - **Table**: `partitions`
   - **Format**: `month_number`
   - **Example**: `8`

## Data Flow

```
User Selection → Filter Validation → Database Query → Partition Search → S3 Data Loading
```

1. **User selects filters** in the UI
2. **System validates** required filters are present
3. **Database query** searches `partitions` table with WHERE conditions
4. **Results returned** as DataFrame with S3 paths
5. **S3 data loaded** and combined for analysis

## Current Data Summary

Based on the partition navigation database:

- **1 Payer**: UnitedHealthcare of Georgia Inc
- **52 States/Territories**: All US states and territories
- **2 Billing Classes**: institutional, professional
- **4 Procedure Sets**: Evaluation and Management, Imaging, Procedures, etc.
- **333 Medical Specialties**: All taxonomy codes with descriptions
- **108 Statistical Areas**: All CBSA areas
- **1 Year**: 2025
- **1 Month**: 8

## Database Schema

### `partitions` Table Structure
```sql
CREATE TABLE partitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partition_path TEXT UNIQUE NOT NULL,
    payer_slug TEXT,
    state TEXT,
    billing_class TEXT,
    procedure_set TEXT,
    procedure_class TEXT,
    taxonomy_code TEXT,
    taxonomy_desc TEXT,
    stat_area_name TEXT,
    year INTEGER,
    month INTEGER,
    file_size_bytes INTEGER,
    file_size_mb REAL,
    last_modified TEXT,
    estimated_records INTEGER,
    s3_bucket TEXT,
    s3_key TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### `dim_payers` Table Structure
```sql
CREATE TABLE dim_payers (
    payer_slug TEXT PRIMARY KEY,
    payer_display_name TEXT,
    partition_count INTEGER,
    total_size_mb REAL
);
```

### `dim_taxonomies` Table Structure
```sql
CREATE TABLE dim_taxonomies (
    taxonomy_code TEXT PRIMARY KEY,
    taxonomy_desc TEXT,
    partition_count INTEGER,
    total_size_mb REAL
);
```

## Notes

- All filter options are loaded directly from the database at page load
- Data is converted to strings for consistent template rendering
- The system uses hierarchical filtering to ensure meaningful data subsets
- S3 paths are constructed from `s3_bucket` and `s3_key` fields for data loading
