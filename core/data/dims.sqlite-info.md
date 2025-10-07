# SQLite Database Report

- **File**: `dims.sqlite`
- **Generated**: 2025-10-04T09:00:41

## Summary

- Tables: **15**
- Views: **1**

### Tables
- `_geocode_runs` (3 rows)
- `_nppes_runs` (2 rows)
- `_updater_runs` (1 rows)
- `dim_code` (8,992 rows)
- `dim_emg_episode` (6 rows)
- `dim_imaging_episode` (7 rows)
- `dim_msk_episode` (9 rows)
- `dim_npi` (2,722,458 rows)
- `dim_npi_address` (57,349 rows)
- `dim_pos` (99 rows)
- `dim_taxonomy` (883 rows)
- `dim_tin` (2,822 rows)
- `dim_tin_location` (2,347 rows)
- `dim_tin_npi` (15,747 rows)
- `s3_tiles` (8,688 rows)

### Views
- `dim_tin_primary_location`

---
## Table: `_geocode_runs`
- **Row count:** 3

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE _geocode_runs (
            run_at TEXT PRIMARY KEY,
            attempted INTEGER,
            updated INTEGER,
            skipped_empty INTEGER
          )
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | run_at | TEXT |  |  | ✓ |
| 1 | attempted | INTEGER |  |  |  |
| 2 | updated | INTEGER |  |  |  |
| 3 | skipped_empty | INTEGER |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `sqlite_autoindex__geocode_runs_1` | ✓ | `run_at` | pk |  |

### Sample rows (up to 3)

| run_at | attempted | updated | skipped_empty |
| --- | --- | --- | --- |
| 2025-10-03T18:08:20+00:00 | 5079 | 5079 | 0 |
| 2025-10-03T21:21:19+00:00 | 5811 | 5811 | 0 |
| 2025-10-03T21:43:34+00:00 | 4911 | 4911 | 0 |

---
## Table: `_nppes_runs`
- **Row count:** 2

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE _nppes_runs (
              run_at TEXT PRIMARY KEY,
              total INTEGER,
              found INTEGER,
              not_found INTEGER,
              errors INTEGER
            )
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | run_at | TEXT |  |  | ✓ |
| 1 | total | INTEGER |  |  |  |
| 2 | found | INTEGER |  |  |  |
| 3 | not_found | INTEGER |  |  |  |
| 4 | errors | INTEGER |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `sqlite_autoindex__nppes_runs_1` | ✓ | `run_at` | pk |  |

### Sample rows (up to 3)

| run_at | total | found | not_found | errors |
| --- | --- | --- | --- | --- |
| 2025-10-03T17:20:39+00:00 | 200 | 200 | 0 | 0 |
| 2025-10-03T21:19:41+00:00 | 353 | 350 | 3 | 0 |

---
## Table: `_updater_runs`
- **Row count:** 1

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE _updater_runs (
            run_at TEXT PRIMARY KEY,
            providers_count INTEGER,
            missing_npis INTEGER,
            missing_tins INTEGER
        )
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | run_at | TEXT |  |  | ✓ |
| 1 | providers_count | INTEGER |  |  |  |
| 2 | missing_npis | INTEGER |  |  |  |
| 3 | missing_tins | INTEGER |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `sqlite_autoindex__updater_runs_1` | ✓ | `run_at` | pk |  |

### Sample rows (up to 3)

| run_at | providers_count | missing_npis | missing_tins |
| --- | --- | --- | --- |
| 2025-10-03T17:03:11+00:00 | 5780 | 1335 | 2478 |

---
## Table: `dim_code`
- **Row count:** 8,992

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE "dim_code" (
"proc_cd" TEXT,
  "proc_set" TEXT,
  "proc_class" TEXT,
  "proc_group" TEXT
)
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | proc_cd | TEXT |  |  |  |
| 1 | proc_set | TEXT |  |  |  |
| 2 | proc_class | TEXT |  |  |  |
| 3 | proc_group | TEXT |  |  |  |

### Sample rows (up to 3)

| proc_cd | proc_set | proc_class | proc_group |
| --- | --- | --- | --- |
| 99201 | Evaluation and Management | Office/ outpatient services | New office visits |
| 99202 | Evaluation and Management | Office/ outpatient services | New office visits |
| 99203 | Evaluation and Management | Office/ outpatient services | New office visits |

---
## Table: `dim_emg_episode`
- **Row count:** 6

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE dim_emg_episode (
  episode_id TEXT PRIMARY KEY,
  episode_name TEXT,
  clinical_intent TEXT,
  codes_required JSON,   -- JSON array of CPT codes
  codes_optional JSON,   -- JSON array of optional CPT codes
  selection_rules TEXT   -- human-readable rules for picking among variants
)
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | episode_id | TEXT |  |  | ✓ |
| 1 | episode_name | TEXT |  |  |  |
| 2 | clinical_intent | TEXT |  |  |  |
| 3 | codes_required | JSON |  |  |  |
| 4 | codes_optional | JSON |  |  |  |
| 5 | selection_rules | TEXT |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `sqlite_autoindex_dim_emg_episode_1` | ✓ | `episode_id` | pk |  |

### Sample rows (up to 3)

| episode_id | episode_name | clinical_intent | codes_required | codes_optional | selection_rules |
| --- | --- | --- | --- | --- | --- |
| EMG_EP1 | Single-limb neuropathy/entrapment eval (UE or LE) | Baseline EMG/NCS for suspected CTS, ulnar neuropathy at elbow, peroneal neuropathy, etc. | ["99204","99214","95885","95886","95907","95908","95909"] | ["95860","95861","95910","95911","95912","95913","95936","95934"] | Pick ONE E/M (new=99204 or est=99214). Use 95885/95886 (EMG with NCS). Choose ONE NCS count code (95907–95913) based on # of studies. Add 95860/95861 if performing standalone EMG mapping without the combined codes. Add F-wave (95936) or H-reflex (95934) as clinically indicated. |
| EMG_EP2 | Bilateral upper extremity neuropathy eval | Comprehensive EMG/NCS for bilateral hand/forearm neuropathies (e.g., CTS both sides). | ["99204","99214","95886","95910","95911"] | ["95863","95912","95913","95936"] | Pick ONE E/M. 95886 (complete EMG per limb with NCS). Choose ONE NCS count code reflecting total studies across both limbs (often 95910–95911; escalate if many nerves). Add 95863 (needle EMG 3 extremities) only if paraspinals/extra segments push count; otherwise optional. |
| EMG_EP3 | Lumbar radiculopathy eval (unilateral or bilateral) | EMG/NCS with paraspinals for L4–S1 radic; includes tibial/peroneal/sural where appropriate. | ["99204","99214","95886","95909","95910"] | ["95864","95861","95911","95912","95934","95936"] | Pick ONE E/M. Use 95886 (with NCS). Select ONE NCS count code for the # of nerves tested (commonly 95909–95910). Add 95864 if four-extremity EMG is actually performed (rare); 95934/95936 if H-reflex or F-wave testing is clinically indicated. |

---
## Table: `dim_imaging_episode`
- **Row count:** 7

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE dim_imaging_episode (
  imaging_id TEXT PRIMARY KEY,
  imaging_name TEXT,
  body_region TEXT,
  modality TEXT,          -- 'X-ray' | 'MRI' | 'Ultrasound'
  codes JSON,             -- JSON array of CPT codes
  selection_rules TEXT
)
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | imaging_id | TEXT |  |  | ✓ |
| 1 | imaging_name | TEXT |  |  |  |
| 2 | body_region | TEXT |  |  |  |
| 3 | modality | TEXT |  |  |  |
| 4 | codes | JSON |  |  |  |
| 5 | selection_rules | TEXT |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `sqlite_autoindex_dim_imaging_episode_1` | ✓ | `imaging_id` | pk |  |

### Sample rows (up to 3)

| imaging_id | imaging_name | body_region | modality | codes | selection_rules |
| --- | --- | --- | --- | --- | --- |
| IMG_KNEE_MRI_WO | MRI Knee without contrast | Knee | MRI | ["73721"] | Use 73721 (WO). If contrast indicated, 73722 (W) or 73723 (W&W/O). |
| IMG_SHOULDER_MRI_WO | MRI Shoulder without contrast | Shoulder | MRI | ["73221"] | Use 73221 (WO). If contrast, 73222 (W) or 73223 (W&W/O). |
| IMG_LSPINE_MRI_WO | MRI Lumbar Spine without contrast | Lumbar Spine | MRI | ["72148"] | Use 72148 (WO). If contrast, 72149 (W) or 72158 (W&W/O). |

---
## Table: `dim_msk_episode`
- **Row count:** 9

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE dim_msk_episode (
  episode_id TEXT PRIMARY KEY,
  episode_name TEXT,
  body_region TEXT,
  typical_setting TEXT,     -- e.g., 'ASC' | 'Hospital OP'
  clinical_intent TEXT,
  codes_required JSON,      -- JSON array of CPT codes that usually appear
  codes_optional JSON,      -- JSON array of add-ons/alternates
  rehab_codes JSON,         -- post-op PT/OT commonly paired
  selection_rules TEXT
)
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | episode_id | TEXT |  |  | ✓ |
| 1 | episode_name | TEXT |  |  |  |
| 2 | body_region | TEXT |  |  |  |
| 3 | typical_setting | TEXT |  |  |  |
| 4 | clinical_intent | TEXT |  |  |  |
| 5 | codes_required | JSON |  |  |  |
| 6 | codes_optional | JSON |  |  |  |
| 7 | rehab_codes | JSON |  |  |  |
| 8 | selection_rules | TEXT |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `sqlite_autoindex_dim_msk_episode_1` | ✓ | `episode_id` | pk |  |

### Sample rows (up to 3)

| episode_id | episode_name | body_region | typical_setting | clinical_intent | codes_required | codes_optional | rehab_codes | selection_rules |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MSK_EP_KNEE_MENISC | Knee arthroscopy – partial meniscectomy | Knee | ASC | Arthroscopic debridement/excision of torn meniscus (medial or lateral). | ["29881","29880"] | ["29882","29883","29877"] | ["97161","97162","97110","97112","97140","97530"] | Use 29881 for single-compartment meniscectomy; 29880 if both compartments treated. 29882/29883 = meniscal repair (if performed); 29877 for chondroplasty. Choose ONE primary arthroscopy code. |
| MSK_EP_KNEE_ACL | ACL reconstruction | Knee | ASC | Reconstruction of anterior cruciate ligament with graft; may include meniscal work. | ["29888"] | ["29870","29881","29882","20924","20926"] | ["97161","97162","97110","97112","97140","97116","97530"] | 29888 is primary. Add 29881/29882 if meniscal tear addressed. 29870 for diagnostic scope. 20924/20926 for autograft/allograft harvesting as appropriate. |
| MSK_EP_SHOULDER_RCR | Shoulder arthroscopy – rotator cuff repair | Shoulder | ASC | Arthroscopic repair of rotator cuff; may include subacromial decompression or biceps tenodesis. | ["29827"] | ["29826","29828","29824","29822","29823"] | ["97161","97162","97110","97112","97140","97530"] | Use 29827 as primary. 29826 (SAD) and 29828 (biceps tenodesis) when performed. 29824 distal clavicle excision if needed. 29822–29823 debridement levels. Choose ONE primary. |

---
## Table: `dim_npi`
- **Row count:** 2,722,458

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE "dim_npi" (
	"npi"	TEXT,
	"credential"	TEXT,
	"primary_taxonomy_code"	TEXT,
	"enumeration_type"	TEXT,
	"primary_taxonomy_license"	TEXT,
	"first_name"	TEXT,
	"organization_name"	TEXT,
	"last_name"	TEXT,
	"primary_taxonomy_desc"	TEXT,
	"last_updated"	TEXT,
	"replacement_npi"	TEXT,
	"primary_taxonomy_state"	TEXT,
	"nppes_fetched"	INTEGER,
	"nppes_fetch_date"	TEXT,
	"enumeration_date"	TEXT,
	"status"	TEXT,
	"sole_proprietor"	TEXT,
	PRIMARY KEY("npi")
)
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | npi | TEXT |  |  | ✓ |
| 1 | credential | TEXT |  |  |  |
| 2 | primary_taxonomy_code | TEXT |  |  |  |
| 3 | enumeration_type | TEXT |  |  |  |
| 4 | primary_taxonomy_license | TEXT |  |  |  |
| 5 | first_name | TEXT |  |  |  |
| 6 | organization_name | TEXT |  |  |  |
| 7 | last_name | TEXT |  |  |  |
| 8 | primary_taxonomy_desc | TEXT |  |  |  |
| 9 | last_updated | TEXT |  |  |  |
| 10 | replacement_npi | TEXT |  |  |  |
| 11 | primary_taxonomy_state | TEXT |  |  |  |
| 12 | nppes_fetched | INTEGER |  |  |  |
| 13 | nppes_fetch_date | TEXT |  |  |  |
| 14 | enumeration_date | TEXT |  |  |  |
| 15 | status | TEXT |  |  |  |
| 16 | sole_proprietor | TEXT |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `idx_npi_enum` |  | `npi`, `enumeration_type` | c |  |
| `idx_dim_npi_taxonomy` |  | `primary_taxonomy_code` | c |  |
| `sqlite_autoindex_dim_npi_1` | ✓ | `npi` | pk |  |

### Sample rows (up to 3)

| npi | credential | primary_taxonomy_code | enumeration_type | primary_taxonomy_license | first_name | organization_name | last_name | primary_taxonomy_desc | last_updated | replacement_npi | primary_taxonomy_state | nppes_fetched | nppes_fetch_date | enumeration_date | status | sole_proprietor |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1588772040 | NaN | NaN |  | NaN | NaN | NaN | NaN |  |  | NaN | NaN | 1 |  |  | I |  |
| 1841334638 | NaN | 208600000X | NPI-1 | 91142 | HOLLY | NaN | LEROUX |  | 2007-07-08T00:00:00 | NaN | CO | 1 | 2007-07-08T00:00:00 | 2007-02-16T00:00:00 | A |  |
| 1265619720 | MD | 207K00000X | NPI-1 | 2009-01272 | EDINA | NaN | SWARTZ |  | 2020-06-12T00:00:00 | NaN | NC | 1 | 2020-06-12T00:00:00 | 2008-01-28T00:00:00 | A |  |

---
## Table: `dim_npi_address`
- **Row count:** 57,349

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE dim_npi_address (
      npi TEXT NOT NULL,
      address_hash TEXT NOT NULL,
      address_purpose TEXT,
      address_type TEXT,
      address_1 TEXT,
      address_2 TEXT,
      city TEXT,
      state TEXT,
      postal_code TEXT,
      country_code TEXT,
      telephone_number TEXT,
      fax_number TEXT,
      last_updated TEXT,
      zip_norm TEXT,
      latitude REAL,
      longitude REAL,
      county_name TEXT,
      county_fips TEXT,
      stat_area_name TEXT,
      stat_area_code TEXT,
      matched_address TEXT,
      PRIMARY KEY (npi, address_hash),
      FOREIGN KEY (npi) REFERENCES dim_npi(npi) ON UPDATE CASCADE ON DELETE CASCADE
    )
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | npi | TEXT | ✓ |  | ✓ |
| 1 | address_hash | TEXT | ✓ |  | ✓ |
| 2 | address_purpose | TEXT |  |  |  |
| 3 | address_type | TEXT |  |  |  |
| 4 | address_1 | TEXT |  |  |  |
| 5 | address_2 | TEXT |  |  |  |
| 6 | city | TEXT |  |  |  |
| 7 | state | TEXT |  |  |  |
| 8 | postal_code | TEXT |  |  |  |
| 9 | country_code | TEXT |  |  |  |
| 10 | telephone_number | TEXT |  |  |  |
| 11 | fax_number | TEXT |  |  |  |
| 12 | last_updated | TEXT |  |  |  |
| 13 | zip_norm | TEXT |  |  |  |
| 14 | latitude | REAL |  |  |  |
| 15 | longitude | REAL |  |  |  |
| 16 | county_name | TEXT |  |  |  |
| 17 | county_fips | TEXT |  |  |  |
| 18 | stat_area_name | TEXT |  |  |  |
| 19 | stat_area_code | TEXT |  |  |  |
| 20 | matched_address | TEXT |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `idx_addr_npi` |  | `npi`, `address_purpose` | c |  |
| `idx_addr_state` |  | `state` | c |  |
| `idx_addr_zip` |  | `zip_norm` | c |  |
| `sqlite_autoindex_dim_npi_address_1` | ✓ | `npi`, `address_hash` | pk |  |

### Foreign Keys

| From | To | References | On Update | On Delete | Match |
|------|----|------------|-----------|-----------|-------|
| `npi` | `npi` | `dim_npi` | CASCADE | CASCADE | NONE |

### Sample rows (up to 3)

| npi | address_hash | address_purpose | address_type | address_1 | address_2 | city | state | postal_code | country_code | telephone_number | fax_number | last_updated | zip_norm | latitude | longitude | county_name | county_fips | stat_area_name | stat_area_code | matched_address |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1073560959 | 1956e1549f6677824ad7e31f0be097be | LOCATION | DOM | 2462 HIGHWAY 34 EAST | SUITE A | NEWNAN | GA | 30265 | US | 7706835437 | 7706833758 | 2013-04-04T00:00:00 | 30265 | 33.405687615727 | -84.701868334475 | Coweta County | 13077 | Atlanta--Athens-Clarke County--Sandy Springs, GA-AL CSA | 122 | 2462 STATE RTE 34, NEWNAN, GA, 30265 |
| 1598846297 | ea4647598d8be391d8c030bafe2fc01f | MAILING | DOM | 5780 PEACHTREE DUNWOODY RD STE 300 |  | ATLANTA | GA | 303421513 | US | 4043038035 | 4043031325 | 2021-06-08T00:00:00 | 30342-1513 | 33.912109337597 | -84.352966448896 | Fulton County | 13121 | Atlanta--Athens-Clarke County--Sandy Springs, GA-AL CSA | 122 | 5780 PEACHTREE DUNWOODY RD, ATLANTA, GA, 30342 |
| 1295781268 | 8d214d8333ad2667bbe5b050a0e9a267 | LOCATION | DOM | 260 6TH AVE NW |  | JASPER | AL | 355047419 | US | 2053846919 | 2052216415 | 2025-10-03T18:08:18+00:00 | 35504-7419 | 33.850227498647 | -87.281995370282 | Walker County | 01127 | Birmingham-Cullman-Talladega, AL CSA | 142 | 260 6TH AVE NW, JASPER, AL, 35504 |

---
## Table: `dim_pos`
- **Row count:** 99

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE "dim_pos" (
"Place of Service Code" INTEGER,
  "Place of Service Name" TEXT,
  "Place of Service Description" TEXT
)
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | Place of Service Code | INTEGER |  |  |  |
| 1 | Place of Service Name | TEXT |  |  |  |
| 2 | Place of Service Description | TEXT |  |  |  |

### Sample rows (up to 3)

| Place of Service Code | Place of Service Name | Place of Service Description |
| --- | --- | --- |
| 1 | Pharmacy | A facility or location where drugs and other medically related items and services are sold, dispensed, or otherwise provided directly to patients. (Effective October 1, 2003) (Revised, effective October 1, 2005) |
| 2 | Telehealth Provided Other than in Patient’s Home | The location where health services and health related services are provided or received, through telecommunication technology. Patient is not located in their home when receiving health services or health related services through telecommunication technology. (Effective January 1, 2017) (Description change effective January 1, 2022, and applicable for Medicare April 1, 2022.) |
| 3 | School | A facility whose primary purpose is education. (Effective January 1, 2003) |

---
## Table: `dim_taxonomy`
- **Row count:** 883

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE "dim_taxonomy" (
"Code" TEXT,
  "Grouping" TEXT,
  "Classification" TEXT,
  "Specialization" TEXT,
  "Definition" TEXT,
  "Notes" TEXT,
  "Display Name" TEXT,
  "Section" TEXT
)
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | Code | TEXT |  |  |  |
| 1 | Grouping | TEXT |  |  |  |
| 2 | Classification | TEXT |  |  |  |
| 3 | Specialization | TEXT |  |  |  |
| 4 | Definition | TEXT |  |  |  |
| 5 | Notes | TEXT |  |  |  |
| 6 | Display Name | TEXT |  |  |  |
| 7 | Section | TEXT |  |  |  |

### Sample rows (up to 3)

| Code | Grouping | Classification | Specialization | Definition | Notes | Display Name | Section |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 193200000X | Group | Multi-Specialty |  | A business group of one or more individual practitioners, who practice with different areas of specialization. | [7/1/2003: new] | Multi-Specialty Group | Individual |
| 193400000X | Group | Single Specialty |  | A business group of one or more individual practitioners, all of who practice with the same area of specialization. | [7/1/2003: new] | Single Specialty Group | Individual |
| 207K00000X | Allopathic & Osteopathic Physicians | Allergy & Immunology |  | An allergist-immunologist is trained in evaluation, physical and laboratory diagnosis, and management of disorders involving the immune system. Selected examples of such conditions include asthma, anaphylaxis, rhinitis, eczema, and adverse reactions to drugs, foods, and insect stings as well as immune deficiency diseases (both acquired and congenital), defects in host defense, and problems related to autoimmune disease, organ transplantation, or malignancies of the immune system. | Source: American Board of Medical Specialties, 2007, www.abms.org  [7/1/2007: added definition, added source]  Additional Resources: American Board of Allergy and Immunology, 2007.  http://www.abai.org/   No subspecialty certificates in allergy and immunology are offered by the American Board of Allergy and Immunology (ABAI). The ABAI, however, does offer formal special pathways for physicians seeking dual certification in allergy/immunology and pediatric pulmonology; allergy/immunology and pediatric rheumatology; and allergy/immunology and adult rheumatology. | Allergy & Immunology Physician | Individual |

---
## Table: `dim_tin`
- **Row count:** 2,822

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE dim_tin (
      tin_value TEXT PRIMARY KEY,
      organization_name TEXT
    , last_updated TEXT)
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | tin_value | TEXT |  |  | ✓ |
| 1 | organization_name | TEXT |  |  |  |
| 2 | last_updated | TEXT |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `idx_dim_tin_name` |  | `organization_name` | c |  |
| `sqlite_autoindex_dim_tin_1` | ✓ | `tin_value` | pk |  |

### Sample rows (up to 3)

| tin_value | organization_name | last_updated |
| --- | --- | --- |
| 582655914 | MIDTOWN ENDOSCOPY CENTER, LLC | 2025-10-03T17:03:11+00:00 |
| 582400910 | AESTHETIC SPECIALTY CENTRE, PC | 2025-10-03T17:03:11+00:00 |
| 200044915 | ANESTHESIA RESOURCES OF AUGUSTA, LLC | 2025-10-03T17:03:11+00:00 |

---
## Table: `dim_tin_location`
- **Row count:** 2,347

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE dim_tin_location (
      tin_value TEXT NOT NULL,
      address_hash TEXT NOT NULL,
      address_1 TEXT,
      address_2 TEXT,
      city TEXT,
      state TEXT,
      zip_norm TEXT,
      latitude REAL,
      longitude REAL,
      support_npi_count INTEGER,
      primary_flag INTEGER,           -- 1 for primary, else 0
      primary_basis TEXT,             -- 'npi2' if chosen from org NPIs, else 'mode'
      npi_list_json TEXT,             -- JSON array of NPIs supporting this address
      last_updated TEXT,
      PRIMARY KEY (tin_value, address_hash)
    )
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | tin_value | TEXT | ✓ |  | ✓ |
| 1 | address_hash | TEXT | ✓ |  | ✓ |
| 2 | address_1 | TEXT |  |  |  |
| 3 | address_2 | TEXT |  |  |  |
| 4 | city | TEXT |  |  |  |
| 5 | state | TEXT |  |  |  |
| 6 | zip_norm | TEXT |  |  |  |
| 7 | latitude | REAL |  |  |  |
| 8 | longitude | REAL |  |  |  |
| 9 | support_npi_count | INTEGER |  |  |  |
| 10 | primary_flag | INTEGER |  |  |  |
| 11 | primary_basis | TEXT |  |  |  |
| 12 | npi_list_json | TEXT |  |  |  |
| 13 | last_updated | TEXT |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `idx_tin_loc_zip` |  | `zip_norm` | c |  |
| `idx_tin_loc_state` |  | `state` | c |  |
| `idx_tin_loc_tin` |  | `tin_value` | c |  |
| `sqlite_autoindex_dim_tin_location_1` | ✓ | `tin_value`, `address_hash` | pk |  |

### Sample rows (up to 3)

| tin_value | address_hash | address_1 | address_2 | city | state | zip_norm | latitude | longitude | support_npi_count | primary_flag | primary_basis | npi_list_json | last_updated |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 010499588 | 47346505de7d53290a3198db13d8e17b | 275 MARGINAL WAY |  | PORTLAND | ME | 04101-2542 | 43.667055430824 | -70.258579664764 | 1 | 1 | NPI-2 | ["1538165642"] | 2025-10-03 20:41:42 |
| 010514660 | 19c4fce4a816e9c3521db9dc33e2c426 | 177 COLLEGE AVE |  | WATERVILLE | ME | 04901-6219 | 44.570744848972 | -69.618830478875 | 1 | 1 | NPI-2 | ["1639647324"] | 2025-10-03 20:41:42 |
| 010543599 | 8501851b20d908eb8a0200fe83c9812e | 274 MAIN ST |  | FORT FAIRFIELD | ME | 04742-1121 | 46.770770490618 | -67.828157820233 | 1 | 1 | NPI-2 | ["1174671150"] | 2025-10-03 20:41:42 |

---
## Table: `dim_tin_npi`
- **Row count:** 15,747

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE dim_tin_npi (
                npi TEXT NOT NULL,
                tin_value TEXT NOT NULL,
                PRIMARY KEY (npi, tin_value)
            )
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | npi | TEXT | ✓ |  | ✓ |
| 1 | tin_value | TEXT | ✓ |  | ✓ |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `idx_tin_npi` |  | `tin_value`, `npi` | c |  |
| `sqlite_autoindex_dim_tin_npi_1` | ✓ | `npi`, `tin_value` | pk |  |

### Sample rows (up to 3)

| npi | tin_value |
| --- | --- |
| 1134133002 | 205077249 |
| 1356384135 | 272116605 |
| 1083775779 | 871078045 |

---
## Table: `s3_tiles`
- **Row count:** 8,688

<details><summary>CREATE SQL</summary>

```sql
CREATE TABLE s3_tiles (
        run_id TEXT,
        payer_slug TEXT,
        billing_class TEXT,
        negotiation_arrangement TEXT,
        negotiated_type TEXT,
        tin_value TEXT,
        proc_set TEXT,
        proc_class TEXT,
        proc_group TEXT,
        s3_prefix TEXT,          -- e.g., s3://bucket/tiles/payer=.../v=RUN/
        parts_count INTEGER,
        row_count INTEGER,
        billing_codes_json TEXT,       -- JSON array
        taxonomy_codes_json TEXT,      -- JSON array
        created_at_utc TEXT,
        UNIQUE(run_id, s3_prefix)      -- prevents duplicates within a run
    )
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | run_id | TEXT |  |  |  |
| 1 | payer_slug | TEXT |  |  |  |
| 2 | billing_class | TEXT |  |  |  |
| 3 | negotiation_arrangement | TEXT |  |  |  |
| 4 | negotiated_type | TEXT |  |  |  |
| 5 | tin_value | TEXT |  |  |  |
| 6 | proc_set | TEXT |  |  |  |
| 7 | proc_class | TEXT |  |  |  |
| 8 | proc_group | TEXT |  |  |  |
| 9 | s3_prefix | TEXT |  |  |  |
| 10 | parts_count | INTEGER |  |  |  |
| 11 | row_count | INTEGER |  |  |  |
| 12 | billing_codes_json | TEXT |  |  |  |
| 13 | taxonomy_codes_json | TEXT |  |  |  |
| 14 | created_at_utc | TEXT |  |  |  |

### Indexes

| Name | Unique | Columns | Origin | Partial |
|------|:------:|---------|--------|:-------:|
| `sqlite_autoindex_s3_tiles_1` | ✓ | `run_id`, `s3_prefix` | u |  |

### Sample rows (up to 3)

| run_id | payer_slug | billing_class | negotiation_arrangement | negotiated_type | tin_value | proc_set | proc_class | proc_group | s3_prefix | parts_count | row_count | billing_codes_json | taxonomy_codes_json | created_at_utc |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2025-10-04T034047Z | United Healthcare Georgia | professional | ffs | negotiated | 205106086 | Procedures | Musculoskeletal | Other | s3://bph-tic/tiles/payer=United_Healthcare_Georgia/class=professional/arr=ffs/type=negotiated/tin=205106086/proc_set=Procedures/proc_class=Musculoskeletal/proc_group=Other/v=2025-10-04T034047Z | 1 | 42534 | ["20103", "20150", "20200", "20205", "20206", "20220", "20225", "20240", "20245", "20250", "20251", "20500", "20520", "20525", "20552", "20553", "20555", "20650", "20662", "20663", "20690", "20692", "20693", "20696", "20697", "20822", "20900", "20902", "20924", "20972", "20973", "20982", "20983", "21010", "21011", "21012", "21013", "21014", "21015", "21016", "21025", "21026", "21029", "21030", "21031", "21032", "21034", "21040", "21044", "21046", "21047", "21048", "21049", "21050", "21060", "21070", "21073", "21076", "21077", "21079", "21080", "21081", "21082", "21083", "21084", "21086", "21087", "21088", "21100", "21120", "21121", "21122", "21123", "21125", "21127", "21137", "21138", "21139", "21141", "21142", "21143", "21150", "21172", "21175", "21181", "21193", "21194", "21195", "21196", "21198", "21199", "21206", "21208", "21209", "21210", "21215", "21230", "21235", "21240", "21242", "21243", "21244", "21245", "21246", "21248", "21249", "21255", "21256", "21260", "21261", "21263", "21267", "21270", "21275", "21280", "21282", "21295", "21296", "21315", "21320", "21325", "21330", "21335", "21336", "21337", "21338", "21339", "21340", "21345", "21346", "21347", "21355", "21356", "21360", "21365", "21366", "21385", "21386", "21387", "21390", "21395", "21401", "21406", "21407", "21408", "21421", "21422", "21440", "21445", "21451", "21452", "21453", "21454", "21461", "21462", "21465", "21470", "21485", "21490", "21497", "21501", "21502", "21550", "21552", "21554", "21555", "21556", "21557", "21558", "21600", "21610", "21685", "21700", "21720", "21742", "21743", "21811", "21812", "21813", "21920", "21925", "21930", "21931", "21932", "21933", "21935", "21936", "22100", "22101", "22102", "22315", "22505", "22510", "22511", "22513", "22514", "22856", "22867", "22869", "23000", "23020", "23030", "23031", "23035", "23040", "23044", "23065", "23066", "23071", "23073", "23075", "23076", "23077", "23078", "23100", "23101", "23105", "23106", "23107", "23120", "23125", "23130", "23140", "23145", "23146", "23150", "23155", "23156", "23170", "23172", "23174", "23180", "23182", "23184", "23190", "23195", "23330", "23333", "23334", "23395", "23397", "23400", "23405", "23406", "23410", "23412", "23415", "23420", "23430", "23440", "23450", "23455", "23460", "23462", "23465", "23466", "23470", "23472", "23473", "23480", "23485", "23490", "23491", "23505", "23515", "23520", "23530", "23532", "23550", "23552", "23575", "23585", "23605", "23615", "23616", "23625", "23630", "23655", "23660", "23665", "23670", "23675", "23680", "23700", "23800", "23802", "23930", "23931", "23935", "24000", "24006", "24065", "24066", "24071", "24073", "24075", "24076", "24077", "24079", "24100", "24101", "24102", "24105", "24110", "24115", "24116", "24120", "24125", "24126", "24130", "24134", "24136", "24138", "24140", "24145", "24147", "24149", "24150", "24152", "24155", "24200", "24201", "24300", "24301", "24305", "24310", "24320", "24330", "24331", "24332", "24340", "24341", "24342", "24343", "24344", "24345", "24346", "24357", "24358", "24359", "24360", "24361", "24362", "24363", "24365", "24366", "24370", "24371", "24400", "24410", "24420", "24430", "24435", "24470", "24495", "24498", "24505", "24515", "24516", "24535", "24538", "24545", "24546", "24565", "24566", "24575", "24577", "24579", "24582", "24586", "24587", "24605", "24615", "24620", "24635", "24655", "24665", "24666", "24675", "24685", "24800", "24802", "24925", "24935", "25000", "25001", "25020", "25023", "25024", "25025", "25028", "25031", "25035", "25040", "25065", "25066", "25071", "25073", "25075", "25076", "25077", "25078", "25085", "25100", "25101", "25105", "25107", "25109", "25110", "25111", "25112", "25115", "25116", "25118", "25119", "25120", "25125", "25126", "25130", "25135", "25136", "25145", "25150", "25151", "25170", "25210", "25215", "25230", "25240", "25248", "25259", "25260", "25263", "25265", "25270", "25272", "25274", "25275", "25280", "25290", "25295", "25300", "25301", "25310", "25312", "25315", "25316", "25320", "25332", "25335", "25337", "25350", "25355", "25360", "25365", "25370", "25375", "25390", "25391", "25392", "25393", "25394", "25400", "25405", "25415", "25420", "25425", "25426", "25430", "25431", "25440", "25441", "25442", "25443", "25444", "25445", "25446", "25447", "25449", "25450", "25455", "25490", "25491", "25492", "25505", "25515", "25520", "25525", "25526", "25545", "25565", "25574", "25575", "25605", "25606", "25607", "25608", "25609", "25624", "25628", "25635", "25645", "25651", "25652", "25670", "25671", "25676", "25685", "25690", "25695", "25800", "25805", "25810", "25820", "25825", "25830", "25907", "25909", "25922", "25931", "26011", "26020", "26025", "26030", "26034", "26035", "26037", "26040", "26045", "26055", "26060", "26070", "26075", "26080", "26100", "26105", "26110", "26111", "26113", "26115", "26116", "26117", "26118", "26121", "26123", "26130", "26135", "26140", "26145", "26160", "26170", "26180", "26185", "26200", "26205", "26210", "26215", "26230", "26235", "26236", "26250", "26260", "26262", "26340", "26350", "26352", "26356", "26357", "26358", "26370", "26372", "26373", "26390", "26392", "26410", "26412", "26415", "26416", "26418", "26420", "26426", "26428", "26432", "26433", "26434", "26437", "26440", "26442", "26445", "26449", "26450", "26455", "26460", "26471", "26474", "26476", "26477", "26478", "26479", "26480", "26483", "26485", "26489", "26490", "26492", "26494", "26496", "26497", "26498", "26499", "26500", "26502", "26508", "26510", "26516", "26517", "26518", "26520", "26525", "26530", "26531", "26535", "26536", "26540", "26541", "26542", "26545", "26546", "26548", "26550", "26555", "26560", "26561", "26562", "26565", "26567", "26568", "26580", "26587", "26590", "26591", "26593", "26596", "26607", "26608", "26615", "26645", "26650", "26665", "26675", "26676", "26685", "26686", "26705", "26706", "26715", "26727", "26735", "26742", "26746", "26756", "26765", "26776", "26785", "26820", "26841", "26842", "26843", "26844", "26850", "26852", "26860", "26862", "26910", "26951", "26952", "26990", "26991", "27000", "27001", "27003", "27006", "27027", "27033", "27035", "27040", "27041", "27043", "27045", "27047", "27048", "27049", "27050", "27052", "27057", "27059", "27060", "27062", "27065", "27066", "27067", "27080", "27086", "27087", "27097", "27098", "27100", "27105", "27110", "27111", "27179", "27202", "27235", "27238", "27252", "27257", "27266", "27267", "27275", "27279", "27301", "27305", "27306", "27307", "27310", "27323", "27324", "27325", "27326", "27327", "27328", "27329", "27330", "27331", "27332", "27333", "27334", "27335", "27337", "27339", "27340", "27345", "27347", "27350", "27355", "27356", "27357", "27360", "27364", "27372", "27380", "27381", "27385", "27386", "27390", "27391", "27392", "27393", "27394", "27395", "27396", "27397", "27400", "27403", "27405", "27407", "27409", "27412", "27415", "27416", "27418", "27420", "27422", "27424", "27425", "27427", "27428", "27429", "27430", "27435", "27437", "27438", "27475", "27477", "27479", "27485", "27496", "27497", "27498", "27499", "27502", "27503", "27509", "27510", "27517", "27524", "27532", "27552", "27566", "27570", "27594", "27600", "27601", "27602", "27603", "27604", "27605", "27606", "27607", "27610", "27612", "27613", "27614", "27615", "27616", "27618", "27619", "27620", "27625", "27626", "27630", "27632", "27634", "27635", "27637", "27638", "27640", "27641", "27647", "27650", "27652", "27654", "27656", "27658", "27659", "27664", "27665", "27675", "27676", "27680", "27681", "27685", "27686", "27687", "27690", "27691", "27695", "27696", "27698", "27700", "27702", "27705", "27707", "27709", "27720", "27722", "27726", "27730", "27732", "27734", "27740", "27742", "27745", "27752", "27756", "27758", "27759", "27762", "27766", "27768", "27769", "27781", "27784", "27792", "27810", "27814", "27818", "27822", "27823", "27825", "27826", "27827", "27828", "27829", "27831", "27832", "27842", "27846", "27848", "27860", "27870", "27871", "27884", "27889", "27892", "27893", "27894", "28001", "28002", "28003", "28005", "28008", "28010", "28011", "28020", "28022", "28024", "28035", "28039", "28041", "28043", "28045", "28046", "28047", "28050", "28052", "28054", "28055", "28060", "28062", "28070", "28072", "28080", "28086", "28088", "28090", "28092", "28100", "28102", "28103", "28104", "28106", "28107", "28108", "28110", "28111", "28112", "28113", "28114", "28116", "28118", "28119", "28120", "28122", "28124", "28126", "28130", "28140", "28150", "28153", "28160", "28171", "28173", "28175", "28192", "28193", "28200", "28202", "28208", "28210", "28220", "28222", "28225", "28226", "28230", "28232", "28234", "28238", "28240", "28250", "28260", "28261", "28262", "28264", "28270", "28272", "28280", "28285", "28286", "28288", "28289", "28291", "28292", "28295", "28296", "28297", "28298", "28299", "28300", "28302", "28304", "28305", "28306", "28307", "28308", "28309", "28310", "28312", "28313", "28315", "28320", "28322", "28340", "28341", "28344", "28345", "28360", "28406", "28415", "28420", "28435", "28436", "28445", "28446", "28455", "28456", "28465", "28476", "28485", "28496", "28505", "28525", "28531", "28545", "28546", "28555", "28575", "28576", "28585", "28606", "28615", "28635", "28636", "28645", "28666", "28675", "28705", "28715", "28725", "28730", "28735", "28737", "28740", "28750", "28755", "28760", "28805", "28810", "28820", "28825", "28890", "29800", "29804", "29805", "29806", "29807", "29819", "29820", "29821", "29822", "29823", "29824", "29825", "29827", "29828", "29830", "29834", "29835", "29836", "29837", "29838", "29840", "29843", "29844", "29845", "29846", "29847", "29848", "29850", "29851", "29855", "29856", "29860", "29861", "29862", "29863", "29866", "29867", "29868", "29870", "29871", "29873", "29874", "29875", "29876", "29877", "29879", "29880", "29881", "29882", "29883", "29884", "29885", "29886", "29887", "29888", "29889", "29891", "29892", "29893", "29894", "29895", "29897", "29898", "29899", "29900", "29901", "29902", "29904", "29905", "29906", "29907", "29914", "29915", "29916", "62269", "62287", "62292", "62350", "62351", "62360", "62361", "62362", "62380", "63001", "63003", "63005", "63011", "63012", "63015", "63016", "63017", "63020", "63030", "63040", "63042", "63045", "63046", "63047", "63055", "63056", "63064", "63075", "63265", "63266", "63267", "63268", "63600", "63610", "63650", "63655", "63662", "63663", "63664", "63685", "63688", "63741", "63744", "64553", "64555", "64561", "64568", "64569", "64575", "64580", "64581", "64585", "64590", "64595", "64605", "64610", "64633", "64635", "64702", "64704", "64708", "64712", "64713", "64714", "64716", "64718", "64719", "64721", "64722", "64726", "64732", "64734", "64736", "64738", "64740", "64742", "64744", "64746", "64763", "64766", "64771", "64772", "64774", "64776", "64782", "64784", "64786", "64788", "64790", "64792", "64795", "64802", "64804", "64820", "64821", "64822", "64823", "64831", "64834", "64835", "64836", "64840", "64856", "64857", "64858", "64861", "64862", "64864", "64865", "64885", "64886", "64890", "64891", "64892", "64893", "64895", "64896", "64897", "64898", "64905", "64907", "64910", "64911", "77003"] | ["174400000X", "207R00000X", "207RH0002X", "207RP1001X", "2085R0001X", "208M00000X"] | 2025-10-04T03:43:03.258287+00:00 |
| 2025-10-04T034047Z | United Healthcare Georgia | professional | ffs | negotiated | 580705892 | Procedures | Musculoskeletal | Other | s3://bph-tic/tiles/payer=United_Healthcare_Georgia/class=professional/arr=ffs/type=negotiated/tin=580705892/proc_set=Procedures/proc_class=Musculoskeletal/proc_group=Other/v=2025-10-04T034047Z | 1 | 27522 | ["20103", "20150", "20200", "20205", "20206", "20220", "20225", "20240", "20245", "20250", "20251", "20500", "20520", "20525", "20552", "20553", "20555", "20650", "20662", "20663", "20690", "20692", "20693", "20696", "20697", "20822", "20900", "20902", "20924", "20972", "20973", "20982", "20983", "21010", "21011", "21012", "21013", "21014", "21015", "21016", "21025", "21026", "21029", "21030", "21031", "21032", "21034", "21040", "21044", "21046", "21047", "21048", "21049", "21050", "21060", "21070", "21073", "21076", "21077", "21079", "21080", "21081", "21082", "21083", "21084", "21086", "21087", "21088", "21100", "21120", "21121", "21122", "21123", "21125", "21127", "21137", "21138", "21139", "21141", "21142", "21143", "21150", "21172", "21175", "21181", "21193", "21194", "21195", "21196", "21198", "21199", "21206", "21208", "21209", "21210", "21215", "21230", "21235", "21240", "21242", "21243", "21244", "21245", "21246", "21248", "21249", "21255", "21256", "21260", "21261", "21263", "21267", "21270", "21275", "21280", "21282", "21295", "21296", "21315", "21320", "21325", "21330", "21335", "21336", "21337", "21338", "21339", "21340", "21345", "21346", "21347", "21355", "21356", "21360", "21365", "21366", "21385", "21386", "21387", "21390", "21395", "21401", "21406", "21407", "21408", "21421", "21422", "21440", "21445", "21451", "21452", "21453", "21454", "21461", "21462", "21465", "21470", "21485", "21490", "21497", "21501", "21502", "21550", "21552", "21554", "21555", "21556", "21557", "21558", "21600", "21610", "21685", "21700", "21720", "21742", "21743", "21811", "21812", "21813", "21920", "21925", "21930", "21931", "21932", "21933", "21935", "21936", "22100", "22101", "22102", "22315", "22505", "22510", "22511", "22513", "22514", "22856", "22867", "22869", "23000", "23020", "23030", "23031", "23035", "23040", "23044", "23065", "23066", "23071", "23073", "23075", "23076", "23077", "23078", "23100", "23101", "23105", "23106", "23107", "23120", "23125", "23130", "23140", "23145", "23146", "23150", "23155", "23156", "23170", "23172", "23174", "23180", "23182", "23184", "23190", "23195", "23330", "23333", "23334", "23395", "23397", "23400", "23405", "23406", "23410", "23412", "23415", "23420", "23430", "23440", "23450", "23455", "23460", "23462", "23465", "23466", "23470", "23472", "23473", "23480", "23485", "23490", "23491", "23505", "23515", "23520", "23530", "23532", "23550", "23552", "23575", "23585", "23605", "23615", "23616", "23625", "23630", "23655", "23660", "23665", "23670", "23675", "23680", "23700", "23800", "23802", "23930", "23931", "23935", "24000", "24006", "24065", "24066", "24071", "24073", "24075", "24076", "24077", "24079", "24100", "24101", "24102", "24105", "24110", "24115", "24116", "24120", "24125", "24126", "24130", "24134", "24136", "24138", "24140", "24145", "24147", "24149", "24150", "24152", "24155", "24200", "24201", "24300", "24301", "24305", "24310", "24320", "24330", "24331", "24332", "24340", "24341", "24342", "24343", "24344", "24345", "24346", "24357", "24358", "24359", "24360", "24361", "24362", "24363", "24365", "24366", "24370", "24371", "24400", "24410", "24420", "24430", "24435", "24470", "24495", "24498", "24505", "24515", "24516", "24535", "24538", "24545", "24546", "24565", "24566", "24575", "24577", "24579", "24582", "24586", "24587", "24605", "24615", "24620", "24635", "24655", "24665", "24666", "24675", "24685", "24800", "24802", "24925", "24935", "25000", "25001", "25020", "25023", "25024", "25025", "25028", "25031", "25035", "25040", "25065", "25066", "25071", "25073", "25075", "25076", "25077", "25078", "25085", "25100", "25101", "25105", "25107", "25109", "25110", "25111", "25112", "25115", "25116", "25118", "25119", "25120", "25125", "25126", "25130", "25135", "25136", "25145", "25150", "25151", "25170", "25210", "25215", "25230", "25240", "25248", "25259", "25260", "25263", "25265", "25270", "25272", "25274", "25275", "25280", "25290", "25295", "25300", "25301", "25310", "25312", "25315", "25316", "25320", "25332", "25335", "25337", "25350", "25355", "25360", "25365", "25370", "25375", "25390", "25391", "25392", "25393", "25394", "25400", "25405", "25415", "25420", "25425", "25426", "25430", "25431", "25440", "25441", "25442", "25443", "25444", "25445", "25446", "25447", "25449", "25450", "25455", "25490", "25491", "25492", "25505", "25515", "25520", "25525", "25526", "25545", "25565", "25574", "25575", "25605", "25606", "25607", "25608", "25609", "25624", "25628", "25635", "25645", "25651", "25652", "25670", "25671", "25676", "25685", "25690", "25695", "25800", "25805", "25810", "25820", "25825", "25830", "25907", "25909", "25922", "25931", "26011", "26020", "26025", "26030", "26034", "26035", "26037", "26040", "26045", "26055", "26060", "26070", "26075", "26080", "26100", "26105", "26110", "26111", "26113", "26115", "26116", "26117", "26118", "26121", "26123", "26130", "26135", "26140", "26145", "26160", "26170", "26180", "26185", "26200", "26205", "26210", "26215", "26230", "26235", "26236", "26250", "26260", "26262", "26340", "26350", "26352", "26356", "26357", "26358", "26370", "26372", "26373", "26390", "26392", "26410", "26412", "26415", "26416", "26418", "26420", "26426", "26428", "26432", "26433", "26434", "26437", "26440", "26442", "26445", "26449", "26450", "26455", "26460", "26471", "26474", "26476", "26477", "26478", "26479", "26480", "26483", "26485", "26489", "26490", "26492", "26494", "26496", "26497", "26498", "26499", "26500", "26502", "26508", "26510", "26516", "26517", "26518", "26520", "26525", "26530", "26531", "26535", "26536", "26540", "26541", "26542", "26545", "26546", "26548", "26550", "26555", "26560", "26561", "26562", "26565", "26567", "26568", "26580", "26587", "26590", "26591", "26593", "26596", "26607", "26608", "26615", "26645", "26650", "26665", "26675", "26676", "26685", "26686", "26705", "26706", "26715", "26727", "26735", "26742", "26746", "26756", "26765", "26776", "26785", "26820", "26841", "26842", "26843", "26844", "26850", "26852", "26860", "26862", "26910", "26951", "26952", "26990", "26991", "27000", "27001", "27003", "27006", "27027", "27033", "27035", "27040", "27041", "27043", "27045", "27047", "27048", "27049", "27050", "27052", "27057", "27059", "27060", "27062", "27065", "27066", "27067", "27080", "27086", "27087", "27097", "27098", "27100", "27105", "27110", "27111", "27179", "27202", "27235", "27238", "27252", "27257", "27266", "27267", "27275", "27279", "27301", "27305", "27306", "27307", "27310", "27323", "27324", "27325", "27326", "27327", "27328", "27329", "27330", "27331", "27332", "27333", "27334", "27335", "27337", "27339", "27340", "27345", "27347", "27350", "27355", "27356", "27357", "27360", "27364", "27372", "27380", "27381", "27385", "27386", "27390", "27391", "27392", "27393", "27394", "27395", "27396", "27397", "27400", "27403", "27405", "27407", "27409", "27412", "27415", "27416", "27418", "27420", "27422", "27424", "27425", "27427", "27428", "27429", "27430", "27435", "27437", "27438", "27475", "27477", "27479", "27485", "27496", "27497", "27498", "27499", "27502", "27503", "27509", "27510", "27517", "27524", "27532", "27552", "27566", "27570", "27594", "27600", "27601", "27602", "27603", "27604", "27605", "27606", "27607", "27610", "27612", "27613", "27614", "27615", "27616", "27618", "27619", "27620", "27625", "27626", "27630", "27632", "27634", "27635", "27637", "27638", "27640", "27641", "27647", "27650", "27652", "27654", "27656", "27658", "27659", "27664", "27665", "27675", "27676", "27680", "27681", "27685", "27686", "27687", "27690", "27691", "27695", "27696", "27698", "27700", "27702", "27705", "27707", "27709", "27720", "27722", "27726", "27730", "27732", "27734", "27740", "27742", "27745", "27752", "27756", "27758", "27759", "27762", "27766", "27768", "27769", "27781", "27784", "27792", "27810", "27814", "27818", "27822", "27823", "27825", "27826", "27827", "27828", "27829", "27831", "27832", "27842", "27846", "27848", "27860", "27870", "27871", "27884", "27889", "27892", "27893", "27894", "28001", "28002", "28003", "28005", "28008", "28010", "28011", "28020", "28022", "28024", "28035", "28039", "28041", "28043", "28045", "28046", "28047", "28050", "28052", "28054", "28055", "28060", "28062", "28070", "28072", "28080", "28086", "28088", "28090", "28092", "28100", "28102", "28103", "28104", "28106", "28107", "28108", "28110", "28111", "28112", "28113", "28114", "28116", "28118", "28119", "28120", "28122", "28124", "28126", "28130", "28140", "28150", "28153", "28160", "28171", "28173", "28175", "28192", "28193", "28200", "28202", "28208", "28210", "28220", "28222", "28225", "28226", "28230", "28232", "28234", "28238", "28240", "28250", "28260", "28261", "28262", "28264", "28270", "28272", "28280", "28285", "28286", "28288", "28289", "28291", "28292", "28295", "28296", "28297", "28298", "28299", "28300", "28302", "28304", "28305", "28306", "28307", "28308", "28309", "28310", "28312", "28313", "28315", "28320", "28322", "28340", "28341", "28344", "28345", "28360", "28406", "28415", "28420", "28435", "28436", "28445", "28446", "28455", "28456", "28465", "28476", "28485", "28496", "28505", "28525", "28531", "28545", "28546", "28555", "28575", "28576", "28585", "28606", "28615", "28635", "28636", "28645", "28666", "28675", "28705", "28715", "28725", "28730", "28735", "28737", "28740", "28750", "28755", "28760", "28805", "28810", "28820", "28825", "28890", "29800", "29804", "29805", "29806", "29807", "29819", "29820", "29821", "29822", "29823", "29824", "29825", "29827", "29828", "29830", "29834", "29835", "29836", "29837", "29838", "29840", "29843", "29844", "29845", "29846", "29847", "29848", "29850", "29851", "29855", "29856", "29860", "29861", "29862", "29863", "29866", "29867", "29868", "29870", "29871", "29873", "29874", "29875", "29876", "29877", "29879", "29880", "29881", "29882", "29883", "29884", "29885", "29886", "29887", "29888", "29889", "29891", "29892", "29893", "29894", "29895", "29897", "29898", "29899", "29900", "29901", "29902", "29904", "29905", "29906", "29907", "29914", "29915", "29916", "62269", "62287", "62292", "62350", "62351", "62360", "62361", "62362", "62380", "63001", "63003", "63005", "63011", "63012", "63015", "63016", "63017", "63020", "63030", "63040", "63042", "63045", "63046", "63047", "63055", "63056", "63064", "63075", "63265", "63266", "63267", "63268", "63600", "63610", "63650", "63655", "63662", "63663", "63664", "63685", "63688", "63741", "63744", "64553", "64555", "64561", "64568", "64569", "64575", "64580", "64581", "64585", "64590", "64595", "64605", "64610", "64633", "64635", "64702", "64704", "64708", "64712", "64713", "64714", "64716", "64718", "64719", "64721", "64722", "64726", "64732", "64734", "64736", "64738", "64740", "64742", "64744", "64746", "64763", "64766", "64771", "64772", "64774", "64776", "64782", "64784", "64786", "64788", "64790", "64792", "64795", "64802", "64804", "64820", "64821", "64822", "64823", "64831", "64834", "64835", "64836", "64840", "64856", "64857", "64858", "64861", "64862", "64864", "64865", "64885", "64886", "64890", "64891", "64892", "64893", "64895", "64896", "64897", "64898", "64905", "64907", "64910", "64911", "77003"] | ["103TC2200X", "1223E0200X", "174400000X", "207XX0005X", "2084P0800X", "2085R0202X", "2085R0204X", "213EP1101X", "231H00000X", "363AM0700X", "363LF0000X"] | 2025-10-04T03:43:07.257595+00:00 |
| 2025-10-04T034047Z | United Healthcare Georgia | professional | ffs | negotiated | 205106086 | Procedures | Other organ systems | Other | s3://bph-tic/tiles/payer=United_Healthcare_Georgia/class=professional/arr=ffs/type=negotiated/tin=205106086/proc_set=Procedures/proc_class=Other_organ_systems/proc_group=Other/v=2025-10-04T034047Z | 1 | 26420 | ["19020", "19081", "19083", "19085", "19100", "19101", "19105", "19110", "19112", "19120", "19125", "19296", "19298", "19300", "19301", "19302", "19303", "19307", "19316", "19318", "19325", "19340", "19342", "19350", "19355", "19357", "19370", "19371", "19380", "19396", "22900", "22901", "22902", "22903", "22904", "22905", "30100", "30110", "30115", "30117", "30118", "30120", "30124", "30125", "30130", "30140", "30150", "30160", "30210", "30220", "30310", "30320", "30400", "30410", "30420", "30430", "30435", "30450", "30460", "30462", "30465", "30520", "30540", "30545", "30580", "30600", "30620", "30630", "30801", "30802", "30915", "30920", "30930", "31002", "31020", "31030", "31032", "31040", "31050", "31051", "31070", "31075", "31080", "31081", "31084", "31085", "31086", "31087", "31090", "31200", "31201", "31205", "31235", "31237", "31238", "31239", "31240", "31241", "31253", "31254", "31255", "31256", "31257", "31259", "31267", "31276", "31287", "31288", "31292", "31293", "31294", "31295", "31296", "31297", "31298", "31300", "31400", "31420", "31510", "31512", "31525", "31526", "31527", "31528", "31529", "31530", "31531", "31535", "31536", "31540", "31541", "31545", "31546", "31551", "31552", "31553", "31554", "31560", "31561", "31570", "31571", "31572", "31573", "31574", "31576", "31578", "31580", "31584", "31587", "31590", "31591", "31592", "31600", "31601", "31603", "31610", "31611", "31612", "31613", "31614", "31622", "31623", "31624", "31625", "31626", "31628", "31629", "31630", "31631", "31634", "31635", "31636", "31638", "31640", "31641", "31643", "31645", "31647", "31648", "31652", "31653", "31660", "31661", "31730", "31750", "31755", "31785", "31820", "31825", "31830", "32400", "32550", "32551", "32556", "32557", "32601", "32604", "32606", "32607", "32608", "32609", "32994", "32998", "38220", "38221", "38222", "38240", "38300", "38305", "38308", "38500", "38505", "38510", "38520", "38525", "38530", "38531", "38542", "38550", "38555", "38570", "38571", "38572", "38700", "38720", "38740", "38745", "38760", "39401", "39402", "40810", "40812", "40814", "40816", "40819", "40820", "40840", "40842", "40843", "40844", "40845", "50020", "50080", "50081", "50200", "50382", "50385", "50387", "50396", "50432", "50433", "50434", "50435", "50436", "50437", "50541", "50542", "50543", "50544", "50551", "50553", "50555", "50557", "50561", "50562", "50570", "50572", "50574", "50575", "50576", "50580", "50590", "50592", "50593", "50688", "50693", "50694", "50695", "50727", "50945", "50947", "50948", "50951", "50953", "50955", "50957", "50961", "50970", "50972", "50974", "50976", "50980", "51020", "51030", "51040", "51045", "51050", "51060", "51065", "51080", "51102", "51500", "51520", "51535", "51710", "51715", "51727", "51728", "51729", "51845", "51860", "51880", "51990", "51992", "52317", "52318", "52320", "52325", "52327", "52330", "52332", "52334", "52341", "52342", "52343", "52344", "52345", "52346", "52351", "52352", "52353", "52354", "52355", "52356", "52400", "52402", "52450", "52500", "52601", "52630", "52640", "52647", "52648", "52649", "52700", "53000", "53010", "53020", "53025", "53040", "53060", "53080", "53085", "53200", "53210", "53215", "53220", "53230", "53235", "53240", "53250", "53260", "53265", "53270", "53275", "53400", "53405", "53410", "53420", "53425", "53430", "53431", "53440", "53442", "53444", "53445", "53447", "53449", "53450", "53460", "53500", "53502", "53505", "53510", "53515", "53520", "53605", "53620", "53665", "53850", "53852", "53854", "53855", "53860", "54000", "54001", "54015", "54100", "54105", "54110", "54111", "54112", "54115", "54120", "54150", "54160", "54161", "54162", "54163", "54164", "54205", "54300", "54304", "54308", "54312", "54316", "54318", "54322", "54324", "54326", "54328", "54332", "54336", "54340", "54344", "54348", "54352", "54360", "54380", "54385", "54400", "54401", "54405", "54408", "54410", "54411", "54416", "54417", "54420", "54435", "54437", "54440", "54500", "54505", "54512", "54520", "54522", "54530", "54535", "54550", "54560", "54600", "54620", "54640", "54650", "54660", "54670", "54680", "54690", "54692", "54700", "54800", "54830", "54840", "54860", "54861", "54865", "54900", "54901", "55040", "55041", "55060", "55100", "55110", "55120", "55150", "55175", "55180", "55200", "55250", "55400", "55500", "55520", "55530", "55535", "55540", "55550", "55600", "55680", "55700", "55705", "55706", "55720", "55725", "55860", "55866", "55873", "55874", "55875", "55920", "55970", "55980", "56440", "56441", "56442", "56620", "56625", "56700", "56740", "56800", "56805", "56810", "57000", "57010", "57020", "57022", "57023", "57061", "57065", "57105", "57106", "57107", "57109", "57120", "57130", "57135", "57155", "57200", "57210", "57220", "57230", "57240", "57250", "57260", "57265", "57268", "57282", "57283", "57284", "57285", "57288", "57289", "57291", "57292", "57295", "57300", "57310", "57320", "57330", "57335", "57400", "57410", "57415", "57423", "57425", "57426", "57460", "57461", "57510", "57513", "57520", "57522", "57530", "57550", "57555", "57556", "57558", "57700", "57720", "57800", "58120", "58145", "58260", "58262", "58263", "58270", "58290", "58291", "58292", "58294", "58345", "58346", "58350", "58353", "58356", "58541", "58542", "58543", "58544", "58545", "58546", "58550", "58552", "58553", "58554", "58555", "58558", "58559", "58560", "58561", "58562", "58563", "58565", "58570", "58571", "58572", "58573", "58600", "58615", "58660", "58661", "58662", "58670", "58671", "58672", "58673", "58674", "58770", "58800", "58805", "58820", "58900", "58920", "58925", "59100", "59150", "59151", "59160", "59300", "59320", "59409", "59412", "59414", "59612", "59812", "59820", "59821", "59840", "59841", "59870", "60000", "60200", "60210", "60212", "60220", "60225", "60240", "60252", "60260", "60271", "60280", "60281", "60500", "60502", "60520", "61215", "61330", "61623", "61626", "61720", "61770", "61790", "61791", "61880", "61885", "61886", "61888", "62000", "62194", "62225", "62230", "64912", "69005", "69105", "69110", "69120", "69140", "69145", "69150", "69205", "69300", "69310", "69320", "69421", "69436", "69440", "69450", "69501", "69502", "69505", "69511", "69530", "69540", "69550", "69552", "69601", "69602", "69603", "69604", "69610", "69620", "69631", "69632", "69633", "69635", "69636", "69637", "69641", "69642", "69643", "69644", "69645", "69646", "69650", "69660", "69661", "69662", "69666", "69667", "69670", "69676", "69700", "69711", "69714", "69717", "69720", "69725", "69740", "69745", "69801", "69805", "69806", "69905", "69910", "69915", "69930", "69955", "69960", "69970", "75894", "75989", "76945", "76946", "76948", "76965", "77002", "77012", "77013", "77021", "77022"] | ["174400000X", "207R00000X", "207RH0002X", "207RP1001X", "2085R0001X", "208M00000X"] | 2025-10-04T03:43:11.124248+00:00 |

---
## View: `dim_tin_primary_location`

<details><summary>CREATE SQL</summary>

```sql
CREATE VIEW dim_tin_primary_location AS
    SELECT *
    FROM dim_tin_location
    WHERE primary_flag = 1
```
</details>

### Columns

| # | Name | Type | Not Null | Default | PK |
|---:|------|------|:--------:|---------|:--:|
| 0 | tin_value | TEXT |  |  |  |
| 1 | address_hash | TEXT |  |  |  |
| 2 | address_1 | TEXT |  |  |  |
| 3 | address_2 | TEXT |  |  |  |
| 4 | city | TEXT |  |  |  |
| 5 | state | TEXT |  |  |  |
| 6 | zip_norm | TEXT |  |  |  |
| 7 | latitude | REAL |  |  |  |
| 8 | longitude | REAL |  |  |  |
| 9 | support_npi_count | INTEGER |  |  |  |
| 10 | primary_flag | INTEGER |  |  |  |
| 11 | primary_basis | TEXT |  |  |  |
| 12 | npi_list_json | TEXT |  |  |  |
| 13 | last_updated | TEXT |  |  |  |

### Sample rows (up to 3)

| tin_value | address_hash | address_1 | address_2 | city | state | zip_norm | latitude | longitude | support_npi_count | primary_flag | primary_basis | npi_list_json | last_updated |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 010499588 | 47346505de7d53290a3198db13d8e17b | 275 MARGINAL WAY |  | PORTLAND | ME | 04101-2542 | 43.667055430824 | -70.258579664764 | 1 | 1 | NPI-2 | ["1538165642"] | 2025-10-03 20:41:42 |
| 010514660 | 19c4fce4a816e9c3521db9dc33e2c426 | 177 COLLEGE AVE |  | WATERVILLE | ME | 04901-6219 | 44.570744848972 | -69.618830478875 | 1 | 1 | NPI-2 | ["1639647324"] | 2025-10-03 20:41:42 |
| 010543599 | 8501851b20d908eb8a0200fe83c9812e | 274 MAIN ST |  | FORT FAIRFIELD | ME | 04742-1121 | 46.770770490618 | -67.828157820233 | 1 | 1 | NPI-2 | ["1174671150"] | 2025-10-03 20:41:42 |
