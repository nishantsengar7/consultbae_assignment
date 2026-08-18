# Data Quality & Entity Resolution Report (Task 4)

**Project:** ConsultBae Talent Network Data Pipeline  
**Database:** `data/consultbae.db` (SQLite)  
**Source Datasets Analyzed:**
1. `source1_naukri_applicants.csv` (Naukri job applicants: Full Name, Email, Phone, City, Experience, Current CTC, Applied Date, Skills)
2. `source2_gig_workers.csv` (Gig worker platform: email_id, worker_name, rate, location, status, skill_tags)
3. `source3_cbnexus_contacts.csv` (Internal CRM contacts: Name, Phone Number, City, Verified, Projects Completed)

---

## Executive Summary & Database Metrics

To build a unified candidate database, `task1_merge/merge_pipeline.py` implemented a **Normalise-First, Disjoint-Set-Union (Union-Find) Entity Resolution Engine**. Across the three messy input files:

- **Total Input Rows:** 103 records loaded into `source_records`
- **Total Unified Profiles Resolved:** 61 unique candidates from Task 1 (plus walk-ins added dynamically in Task 3)
- **Multi-Source Match Distribution:**
  - **3 Sources Merged (`source_count = 3`):** 15 persons (triangulated across Naukri, Gig platform, and CRM)
  - **2 Sources Merged (`source_count = 2`):** 10 persons (matched across 2 platforms)
  - **1 Source Only (`source_count = 1`):** 36 persons (unique to a single system)
- **Deterministic Merge Events Logged:** 42 hard merge operations recorded in `match_log`
- **Ambiguous Pairs Flagged for Human Review:** 5 pairs in `needs_review`
- **Suspect CTC Scaling Rows:** 19 candidates with LPA decimal values flagged in `ctc_unit_suspect`
- **City Location Conflicts:** 5 candidates with cross-system city discrepancies flagged in `city_conflict`

Below is the comprehensive catalog of every data quality issue identified, the exact evidence from the database, and the technical rationale for each handling decision.

---

## Catalog of Data Quality Issues & Remediation

### 1. Phone Number Format Inconsistency

- **Issue:** Phone numbers were formatted with varying national/international prefixes, leading zeros, dashes, and spacing.
  - *Concrete Examples:*
    - `+919000000254` (Tanvi Gupta, `source1` L2) — leading plus and country code
    - `919000000231` (Priya Saxena, `source1` L28) — 12-digit string starting with 91
    - `09000000287` (Priya Singh, `source1` L7) — 11-digit string with leading trunk zero `0`
    - `9000000237` (Manish Reddy, `source1` L5) — bare 10-digit number
    - `+91-9000000131` (Arjun Mehta, `source3` L5) — hyphenated country code
- **Where:** `source1_naukri_applicants.csv` and `source3_cbnexus_contacts.csv` (affected nearly 100% of phone fields in both files).
- **What I Did & Why:**
  - Implemented `normalise_phone()`:
    1. Stripped all non-digit characters (`\D`).
    2. If 12 digits starting with `91`, stripped `91`.
    3. If 11 digits starting with `0`, stripped `0`.
    4. Validated that the remainder is exactly 10 digits (`len == 10`).
    5. Returned `None` for any invalid number rather than guessing.
  - *Rationale:* Phone numbers serve as a primary deterministic joining key between `source1` and `source3`. Without canonical 10-digit normalization, deterministic joins fail completely, leading to duplicate entities.

---

### 2. City Name Variants & Inconsistent Casing

- **Issue:** The same metropolitan area was represented using historical names, modern names, and inconsistent casing.
  - *Concrete Examples:*
    - `Bangalore` (`source1` L3), `bangalore` (`source2` L11), and `Bengaluru` (`source2` L15, `source3` L2)
    - `Gurgaon` (`source2` L9), `gurgaon` (`source1` L16), `Gurugram` (`source1` L5), and `gurugram ` with trailing whitespace (`source2` L13)
    - Mixed casing: `PUNE` (`source2` L3), `pune` (`source2` L14), `NOIDA` (`source1` L4), `Noida ` (`source1` L11), `new delhi` (`source1` L12), `New Delhi` (`source1` L18)
- **Where:** Across all three files (`source1`, `source2`, `source3`).
- **What I Did & Why:**
  - Implemented `CITY_ALIASES` mapping table inside `normalise_city()`:
    - `"bangalore"` / `"bengaluru"` $\rightarrow$ `"Bengaluru"`
    - `"gurgaon"` / `"gurugram"` $\rightarrow$ `"Gurugram"`
    - Stripped leading/trailing whitespace and normalized casing to Title Case.
  - **Deliberate Geographic Preservation:** Kept `"Delhi"`, `"New Delhi"`, and `"Delhi NCR"` as distinct canonical values because they can represent distinct administrative jurisdictions.
  - **City Conflict Tracking:** When a merged candidate had different canonical cities across sources (e.g., `Person #17` Meera Bhatia with `Delhi NCR | New Delhi | Delhi` or `Person #16` Arjun Mishra with `Delhi | New Delhi`), the pipeline concatenated them with pipe separators and set `city_conflict = 1` (flagged 5 persons).
  - *Rationale:* Preserves candidate relocation history rather than arbitrarily discarding one source's location data.

---

### 3. Email Casing & Whitespace Inconsistencies

- **Issue:** Email addresses contained uppercase characters and inconsistent casing across platforms.
  - *Concrete Examples:*
    - `ISHA.CHOPRA95@MAILTEST.EXAMPLE.ORG` (`source2` L7)
    - `DEEPAK.NAIR44@EXAMPLE.COM` (`source2` L15)
    - `VARUN.SAXENA21@EXAMPLE.IN` (`source2` L13)
    - `Isha.Chopra95@Mailtest.Example.Org` (`source2` L20)
- **Where:** `source2_gig_workers.csv` (10 rows had uppercase/mixed-case emails) and `source1_naukri_applicants.csv`.
- **What I Did & Why:**
  - Implemented `normalise_email()` which applies `.strip().lower()`. Returns `None` for empty strings.
  - *Rationale:* Per RFC 5321 and industry practice, email domain and mailbox matching is treated case-insensitively. Lowercasing enabled direct string equality indexing in the Union-Find algorithm.

---

### 4. In-File Duplicate Candidates (Intra-Source Duplicates)

- **Issue:** The same applicant was submitted multiple times within the same source file under slightly modified names or alternate email addresses.
  - *Concrete Examples:*
    - **Nikhil Chopra (`source1` L27 vs L37):**
      - `source1` L27 (`Record #26`): `Full Name: "Nikhil Chopra"`, `Email: "alt.nikhil.chopra70@example.com"`, `Phone: "09000000103"`, `City: "NOIDA"`, `CTC: "7.8"`, `Applied Date: "07/03/2026"`
      - `source1` L37 (`Record #36`): `Full Name: "Nikhil Chopra"`, `Email: "nikhil.chopra70@example.com"`, `Phone: "09000000103"`, `City: "NOIDA"`, `CTC: "7.8"`, `Applied Date: "07/03/2026"`
      - *Analysis:* Same phone `9000000103`, identical experience, CTC, skills, and applied date, but one used an alternate email prefix (`alt.`).
    - **Rohit Verma / R. Verma (`source1` L25 vs L31):**
      - `source1` L25 (`Record #24`): `Full Name: "R. Verma"`, `Email: "rohit.verma13@mailtest.example.org"`, `Phone: "9000000294"`, `City: "Bangalore"`, `CTC: "6.1"`
      - `source1` L31 (`Record #30`): `Full Name: "Rohit Verma"`, `Email: "rohit.verma13@mailtest.example.org"`, `Phone: "9000000294"`, `City: "Bangalore"`, `CTC: "6.1"`
      - *Analysis:* Exact duplicate where the name was entered as an initial (`R. Verma`) on first submission and full name (`Rohit Verma`) on second.
- **Where:** `source1_naukri_applicants.csv`.
- **What I Did & Why:**
  - Tracked seen `(email, phone)` combinations during file ingestion.
  - Flagged repeat entries with `intra_dup_flag = 1` in `source_records` (`Record #30` flagged).
  - Passed both records to Union-Find: the shared phone/email automatically unified `Record #26` & `Record #36` into `Person #26`, and `Record #24` & `Record #30` into `Person #24`.
  - Stored raw JSON for both records in `source_records` for 100% auditability while preventing duplicate profiles in the master `persons` table.

---

### 5. Column-Shifted / Corrupted Row

- **Issue:** A raw CSV row had its values shifted across columns due to unquoted/improper delimiter handling in the source export.
  - *Concrete Example:*
    - `source2_gig_workers.csv` Line 20:
      ```csv
      "react, javascript, mysql",ISHA.CHOPRA95@MAILTEST.EXAMPLE.ORG,Isha Chopra,1406/hr,Pune,active
      ```
      - `email_id` received `"react, javascript, mysql"` (Skills string)
      - `worker_name` received `"ISHA.CHOPRA95@MAILTEST.EXAMPLE.ORG"` (Email string)
      - `rate` received `"Isha Chopra"` (Name string)
      - `location` received `"1406/hr"` (Rate string)
      - `status` received `"Pune"` (City string)
      - `skill_tags` received `"active"` (Status string)
- **Where:** `source2_gig_workers.csv`, Line 20.
- **What I Did & Why:**
  - Detected column shift by asserting that `email_id` must contain `'@'`.
  - Tagged the record with `skip_reason = 'column_shifted'` (`Record #60` in `source_records`).
  - Excluded the corrupted row from entity resolution to avoid polluting the graph with bogus names/emails.
  - Saved the raw row verbatim in `source_records` so no source data was silently dropped.
  - Note: Isha Chopra was already cleanly ingested from Line 7 (`ISHA.CHOPRA95@MAILTEST.EXAMPLE.ORG`, `Isha Chopra`, `1406/hr`, `Pune`, `active`), so no valid candidate data was lost.

---

### 6. Blank Rows & Trailing Empty Lines

- **Issue:** Source CSV files contained empty lines and comma-only blank rows.
  - *Concrete Examples:*
    - `source2_gig_workers.csv` Line 12: `,,,,,`
    - `source1_naukri_applicants.csv` Line 45 (trailing blank line)
    - `source3_cbnexus_contacts.csv` Line 34 (trailing blank line)
- **Where:** Present in all 3 source CSVs.
- **What I Did & Why:**
  - Implemented `is_blank_row()` which verifies if all CSV values in a row are whitespace or empty.
  - Cleanly skipped blank rows during ingestion while logging the skipped line number.
  - *Rationale:* Prevents the database from creating anonymous `NULL` entity records.

---

### 7. Repeated Header Row Embedded as Data

- **Issue:** A CSV export concatenated multiple batches and embedded a secondary header row inside the data body.
  - *Concrete Example:*
    - `source3_cbnexus_contacts.csv` Line 16:
      ```csv
      name,phone number,city,verified,projects completed
      ```
- **Where:** `source3_cbnexus_contacts.csv`, Line 16.
- **What I Did & Why:**
  - Implemented `is_repeated_header(row, expected_keys)` which checks if the row values form a superset of the expected schema column headers.
  - Skipped the row during ingestion.
  - *Rationale:* Prevents creating a candidate named `"Name"` with phone `"Phone Number"` and city `"City"`.

---

### 8. Inconsistent Units Within a Single Column (CTC & Hourly/Monthly Rates)

#### A. CTC Unit Ambiguity in `source1` (`Current CTC`)
- **Issue:** The `Current CTC` column mixed absolute annual INR values with decimal LPA (Lakhs Per Annum) values.
  - *Concrete Examples:*
    - Absolute INR: `1195422` (₹11.95L), `864237` (₹8.64L), `1181149` (₹11.81L)
    - Decimal LPA: `4.2`, `8.3`, `5.1`, `6.1`, `5.8`, `11.2`, `7.6`, `2.4`, `10.0`, `11.9`, `7.8`, `6.6`, `2.7`, `11.4`, `9.3`, `5.9`, `10.3`
- **Exact DB Count:** **19 candidates** in `persons` table have `ctc_unit_suspect = 1`.
  - *Sample flagged candidates:* `Person #5` Amit Agarwal (`ctc_raw = 4.2`), `Person #7` Shreya Gupta (`ctc_raw = 8.3`), `Person #17` Meera Bhatia (`ctc_raw = 11.2`), `Person #24` R. Verma (`ctc_raw = 6.1`).
- **What I Did & Why:**
  - Implemented `flag_ctc_suspect()`: flagged any numerical CTC value `< 200` as suspect LPA scale (`ctc_unit_suspect = 1`).
  - **Did NOT perform automatic multiplication ($\times 100,000$):** Left `ctc_raw` exactly as provided in the raw data.
  - *Rationale:* Automated scaling can introduce silent mathematical corruption if a non-standard salary format (e.g. thousands, monthly stipend, or foreign currency) is present. Flagging allows downstream compensation modules to apply verified conversion rules safely.

#### B. Rate Unit Ambiguity in `source2` (`rate`)
- **Issue:** The `rate` column mixed hourly rates and monthly stipends without a dedicated unit column.
  - *Concrete Examples:*
    - Hourly: `1415/hr`, `1231/hr`, `403/hr`, `440/hr`, `1406/hr`, `330/hr`, `843/hr`, `1331/hr`
    - Monthly: `15k/month`, `72k/month`, `28k/month`, `56k/month`, `79k/month`, `42k/month`, `73k/month`, `55k/month`, `22k/month`, `21k/month`, `59k/month`, `38k/month`, `71k/month`
- **Where:** `source2_gig_workers.csv` (100% of rows contain embedded unit strings).
- **What I Did & Why:**
  - Detected and preserved the explicit rate string intact with units.
  - Did NOT convert monthly to hourly (e.g. dividing by 160 hours) because gig worker hours per month vary widely (part-time vs full-time).

---

### 9. Inconsistent Boolean & Status Encodings

#### A. `Verified` Column in `source3`
- **Issue:** The `Verified` column contained multiple representations of truthy and falsy values.
  - *Concrete Values Found:* `{'yes', 'Yes', 'Y', 'No', 'N'}`
- **Where:** `source3_cbnexus_contacts.csv`.
- **What I Did & Why:**
  - Normalized values during reading:
    - `'y'`, `'yes'` $\rightarrow$ `True` (SQLite integer `1`)
    - `'n'`, `'no'` $\rightarrow$ `False` (SQLite integer `0`)
    - Blank / unexpected $\rightarrow$ `None` (`NULL`)

#### B. `status` Column in `source2`
- **Issue:** Status was entered in mixed casing (`active`, `Active`, `ACTIVE`, `Inactive`, `paused`) plus the corrupted row value (`Pune`).
- **Where:** `source2_gig_workers.csv`.
- **What I Did & Why:**
  - Stripped and case-normalized values during processing.

---

### 10. Inconsistent Date Formats

- **Issue:** The `Applied Date` column in `source1` contained 4+ incompatible date formats.
  - *Concrete Examples:*
    - ISO format: `2026-08-08`, `2026-07-13`, `2026-06-24`
    - Hyphenated DD-MM-YYYY: `24-07-2026`, `28-07-2026`, `22-08-2026`, `15-06-2026`, `21-07-2026`
    - Text month: `7 Jul 2026`, `8 Jul 2026`, `15 Jul 2026`
    - Slashed dates: `07/13/2026`, `07/03/2026`, `08/13/2026`
- **Where:** `source1_naukri_applicants.csv` (all 42 date values).
- **What I Did & Why:**
  - Implemented `normalise_date()` with a waterfall parsing list of `DATE_FORMATS`:
    `["%d-%m-%Y", "%Y-%m-%d", "%d %b %Y", "%m/%d/%Y", "%d/%m/%Y", "%B %d, %Y"]`.
  - Normalized all dates to standard **ISO 8601** (`YYYY-MM-DD`).
  - Handled ambiguity between US (`MM/DD`) and UK (`DD/MM`) by prioritizing the format established by unambiguous entries (e.g. `07/13/2026` indicates `MM/DD/YYYY`).

---

## Issues `merge_pipeline.py` Did NOT Explicitly Handle (Known Limitations & Gaps)

While the pipeline handled all critical identity and structural defects, the following nuances were deliberately not auto-resolved and should be noted for future pipeline iterations:

1. **Skill Taxonomy Standardization:**
   - Skills were lowercased and deduplicated across sources (e.g., `merged_skills` in `persons`), but not mapped to a canonical ontology (e.g., `react` vs `react.js`, `mysql` vs `sql`, `web scraping` vs `selenium`).
2. **Missing Transitive Cross-File Bridges (Source 2 $\leftrightarrow$ Source 3):**
   - Because `source2` has *no phone numbers* and `source3` has *no email addresses*, candidates appearing in both `source2` and `source3` without an appearance in `source1` cannot be deterministically matched. The pipeline safely flagged them into `needs_review` rather than auto-merging.
3. **Experience vs Project Metrics:**
   - `source1` tracks `Experience (Years)` while `source3` tracks `Projects Completed`. These represent different dimensions of seniority and were preserved in raw JSON rather than converted into an arbitrary synthetic score.

---

## Case Study: The "Arjun Mehta" Disambiguation

The handling of **Arjun Mehta** provides a textbook example of why probabilistic or naive name-based entity resolution is dangerous in recruitment databases.

### The Evidence in the Database

Querying `source_records` and `persons` reveals **4 distinct records** across the 3 files under the name "Arjun Mehta" in "Noida":

```
[Record #19] (source1 L20) -> Name: "Arjun Mehta", Email: "arjun.mehta9@example.in", Phone: "9000000131", City: "Noida", CTC: "1181149"
[Record #77] (source3 L5)  -> Name: "Arjun Mehta", Phone: "+91-9000000131", City: "Noida", Projects: "9", Verified: "No"
[Record #58] (source2 L18) -> Name: "Arjun Mehta", Email: "arjun.mehta77@mailtest.example.org", Phone: None, City: "Noida", Rate: "42k/month"
[Record #99] (source3 L28) -> Name: "Arjun Mehta", Phone: "9000000272", Email: None, City: "Noida", Projects: "14", Verified: "Yes"
```

### How the Pipeline Resolved Them

1. **Hard Merge 1 (`Person #19`):**
   - `Record #19` (`source1`) and `Record #77` (`source3`) share the verified normalized phone `9000000131`.
   - The Union-Find engine merged them into `Person #19` with `sources = 'source1,source3'`.
2. **Singleton 1 (`Person #41`):**
   - `Record #58` (`source2`) has email `arjun.mehta77@mailtest.example.org` but NO phone number.
   - It cannot link to `Person #19` because `arjun.mehta77` $\neq$ `arjun.mehta9`.
3. **Singleton 2 (`Person #57`):**
   - `Record #99` (`source3`) has phone `9000000272` but NO email address.
   - It cannot link to `Person #19` because `9000000272` $\neq$ `9000000131`.
4. **Fuzzy Flag in `needs_review` (`Review #1`):**
   - Comparing `Record #58` (Person #41) and `Record #99` (Person #57): both share Name (`"Arjun Mehta"`) and City (`"Noida"`), resulting in a **Fuzzy Score of 100.0**.
   - However, `Record #58` has no phone, and `Record #99` has no email.

```sql
SELECT review_id, name_a, city_a, email_a, phone_a, name_b, city_b, email_b, phone_b, fuzzy_score 
FROM needs_review WHERE review_id = 1;
```
*Result:* `Review #1: Arjun Mehta (Noida | email=arjun.mehta77@mailtest.example.org | phone=None) <-> Arjun Mehta (Noida | email=None | phone=9000000272) | Score=100.0`

### Why It Was NOT Auto-Merged
"Arjun Mehta" is a common name in the Delhi NCR / Noida technology corridor. If the pipeline merged based solely on `Name + City`:
- It would falsely assume that `arjun.mehta77@mailtest.example.org` owns phone number `9000000272`.
- It would conflate two potentially distinct gig workers/contractors into a single profile.
- Downstream systems (e.g. background verification, automated SMS/email outreach, Task 3 voice submissions) would send sensitive communications to the wrong contact channel.

### What Evidence Would Resolve It
To safely merge or permanently separate `Person #41` and `Person #57`:
1. **Secondary Contact Verification:** Requesting the gig worker (`arjun.mehta77`) to verify their mobile number in the platform UI. If the phone is `9000000272`, they merge with `Person #57`.
2. **Skill / Project Overlap Analysis:** Comparing skill tags from `source2` (`fastapi, pandas, web scraping, zapier, docker, mysql`) against project portfolio records in `source3`.
3. **Task 3 Audio Submission Bridge:** If Arjun Mehta submits a voice intro on the Task 3 web app with email `arjun.mehta77@mailtest.example.org` and phone `9000000272`, the newly ingested phone creates the deterministic bridge.

---

## Conclusion & Architecture Summary

The data cleaning and entity resolution design strictly followed the principle of **"Deterministic matching, probabilistic flagging, and zero data loss"**:
- **Zero Loss:** Every raw CSV line is preserved in `source_records.raw_json`.
- **High Precision:** Merges occurred only over deterministic canonical identifiers (`canonical_phone`, `canonical_email`).
- **Human-in-the-Loop:** All ambiguous cases (`needs_review`) and unit anomalies (`ctc_unit_suspect`, `city_conflict`) are explicitly flagged for human oversight.
