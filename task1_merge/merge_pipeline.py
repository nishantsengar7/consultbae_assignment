"""
merge_pipeline.py
=================
Task 1: Merge three messy CSV files from different recruitment systems into
a single clean SQLite database using Union-Find entity resolution.

Design philosophy:
  - Normalise first, match second: all identifier normalisation happens
    before any comparison so the matching logic stays simple.
  - Union-Find (disjoint set union) lets us handle transitive links
    (A~B and B~C => A,B,C are one person) without pairwise joins.
  - Fuzzy name+city is only a flag, never an auto-merge: it goes to
    needs_review so a human decides.
  - Ambiguous data (CTC unit mismatch, rate unit mismatch) is flagged
    with a boolean; the raw value is left intact.
  - Every raw source row is stored as JSON in source_records so every
    merge decision is fully traceable.

Dependencies:
    pip install rapidfuzz
"""

import csv
import json
import re
import sqlite3
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from rapidfuzz import fuzz

BASE_DIR = Path(__file__).parent
S1_PATH  = BASE_DIR / "source_csvs" / "source1_naukri_applicants.csv" if (BASE_DIR / "source_csvs" / "source1_naukri_applicants.csv").exists() else BASE_DIR / "source1_naukri_applicants.csv"
S2_PATH  = BASE_DIR / "source_csvs" / "source2_gig_workers.csv" if (BASE_DIR / "source_csvs" / "source2_gig_workers.csv").exists() else BASE_DIR / "source2_gig_workers.csv"
S3_PATH  = BASE_DIR / "source_csvs" / "source3_cbnexus_contacts.csv" if (BASE_DIR / "source_csvs" / "source3_cbnexus_contacts.csv").exists() else BASE_DIR / "source3_cbnexus_contacts.csv"
DB_PATH  = BASE_DIR / "consultbae_merged.db"

FUZZY_THRESHOLD = 85

def normalise_email(raw: str):
    """
    Lowercase and strip whitespace.
    Returns None for blank/null.
    Case differences in emails are not meaningful per RFC 5321 (local-part
    is technically case-sensitive, but in practice all major providers
    treat them case-insensitively; lowercasing is the standard approach).
    """
    if not raw or not raw.strip():
        return None
    return raw.strip().lower()

def normalise_phone(raw: str):
    """
    Strip everything except digits, then normalise to a 10-digit number.

    India mobile numbers are 10 digits starting with [6-9].
    Common formats seen in the data:
      +919000000254  -> strip '+' -> 919000000254 -> strip leading '91' -> 9000000254
       919000000231  -> strip leading '91' -> 9000000231
      09000000287    -> strip leading '0'  -> 9000000287
       9000000237    -> already 10 digits
      +91-9000000131 -> strip non-digits -> 919000000131 -> strip '91' -> 9000000131

    We do NOT accept numbers that do not resolve to exactly 10 digits after
    normalisation; those are logged as None and left un-matched.
    """
    if not raw or not raw.strip():
        return None
    digits = re.sub(r'\D', '', raw.strip())
    if len(digits) == 12 and digits.startswith('91'):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith('0'):
        digits = digits[1:]
    if len(digits) == 10:
        return digits
    return None

CITY_ALIASES = {
    "gurgaon":    "Gurugram",
    "gurugram":   "Gurugram",
    "bangalore":  "Bengaluru",
    "bengaluru":  "Bengaluru",
    "pune":       "Pune",
    "noida":      "Noida",
    "delhi":      "Delhi",
    "new delhi":  "New Delhi",
    "delhi ncr":  "Delhi NCR",
}

def normalise_city(raw: str) -> str:
    """
    Strip, lowercase-lookup in alias table, return canonical name.
    Unknown cities are title-cased and returned as-is (do not silently drop).
    """
    if not raw or not raw.strip():
        return ""
    key = raw.strip().lower()
    return CITY_ALIASES.get(key, raw.strip().title())

def normalise_name(raw: str) -> str:
    """
    Strip, title-case, collapse multiple spaces, remove soft-hyphens and
    zero-width chars.  Used only for fuzzy display / grouping, not for
    hard matching.
    """
    if not raw:
        return ""
    cleaned = unicodedata.normalize("NFKC", raw.strip())
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.title()

def flag_ctc_suspect(raw_ctc: str) -> bool:
    """
    Returns True if the CTC value looks like it is in Lakhs (small decimal)
    rather than absolute rupees.

    Heuristic: values < 200 are almost certainly in LPA (Lakh Per Annum)
    while values > 200 are almost certainly absolute rupees.

    We deliberately do NOT convert -- we just raise a flag.
    """
    if not raw_ctc or not raw_ctc.strip():
        return False
    try:
        val = float(raw_ctc.strip())
        return val < 200
    except ValueError:
        return False

def flag_rate_suspect(raw_rate: str) -> bool:
    """
    Returns True if the rate field mixes units (e.g., 'k/month' vs '/hr').
    We flag *all* rate values so downstream knows the unit is embedded in
    the string, not implicit.
    """
    if not raw_rate or not raw_rate.strip():
        return False
    return bool(re.search(r'(k/month|/hr)', raw_rate.strip(), re.IGNORECASE))

DATE_FORMATS = [
    "%d-%m-%Y",
    "%Y-%m-%d",
    "%d %b %Y",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%B %d, %Y",
]

def normalise_date(raw: str):
    """
    Try each known format; return ISO YYYY-MM-DD on first match.
    Ambiguity between MM/DD and DD/MM is unavoidable for dates where
    both day and month <= 12; we try MM/DD first (US format as used in
    the data: '07/13/2026').
    """
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw

def is_blank_row(row: dict) -> bool:
    """
    True if every CSV value in the row is empty/whitespace.
    Skips internal pipeline keys (prefixed with '_') and non-string values
    (e.g. the integer _source_line we inject).
    """
    for k, v in row.items():
        if k.startswith('_'):
            continue
        if not isinstance(v, str):
            continue
        if v.strip():
            return False
    return True

def is_repeated_header(row: dict, expected_keys: set) -> bool:
    """
    True if the row's VALUES look like column headers (i.e. the file has
    a header row embedded as a data row -- seen in source3 line 16).
    We check whether the values are a superset of expected column names.
    """
    vals = {v.strip().lower() for v in row.values() if isinstance(v, str) and v}
    keys = {k.strip().lower() for k in expected_keys}
    return keys.issubset(vals)

def read_source1(path: Path) -> list:
    """
    Read source1_naukri_applicants.csv.
    Cleans:
      - blank trailing rows
      - within-source deduplication by (email, phone) pair
      - flags CTC unit ambiguity
      - normalises date
    Returns list of dicts with '_norm_email', '_norm_phone' added.
    """
    records = []
    seen_email_phone = set()

    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            row['_source_line'] = i
            row['_source'] = 'source1'

            if is_blank_row(row):
                print(f"  [S1 L{i}] SKIP: blank row")
                continue

            row['_norm_email'] = normalise_email(row.get('Email', ''))
            row['_norm_phone'] = normalise_phone(row.get('Phone', ''))
            row['_norm_city']  = normalise_city(row.get('City', ''))
            row['_norm_name']  = normalise_name(row.get('Full Name', ''))
            row['_norm_date']  = normalise_date(row.get('Applied Date', ''))

            row['_ctc_unit_suspect'] = flag_ctc_suspect(row.get('Current CTC', ''))

            key = (row['_norm_email'], row['_norm_phone'])
            if key in seen_email_phone and key != (None, None):
                print(f"  [S1 L{i}] WARN: within-source duplicate "
                      f"email={row['_norm_email']} phone={row['_norm_phone']} "
                      f"name={row.get('Full Name','')!r}")
                row['_intra_source_duplicate'] = True
            else:
                row['_intra_source_duplicate'] = False
                seen_email_phone.add(key)

            records.append(row)

    return records

def read_source2(path: Path) -> list:
    """
    Read source2_gig_workers.csv.
    Cleans:
      - blank rows
      - column-shifted row (line 20: skills value leaked into email column)
      - flags rate unit ambiguity
    """
    records = []
    expected_keys = {'email_id', 'worker_name', 'rate', 'location', 'status', 'skill_tags'}
    seen_emails = set()

    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            row['_source_line'] = i
            row['_source'] = 'source2'

            if is_blank_row(row):
                print(f"  [S2 L{i}] SKIP: blank row")
                continue

            raw_email_field = row.get('email_id', '')
            if raw_email_field and '@' not in raw_email_field:
                print(f"  [S2 L{i}] SKIP: column-shifted row (email_id={raw_email_field!r})")
                row['_skip_reason'] = 'column_shifted'
                row['_norm_email'] = None
                row['_norm_phone'] = None
                row['_norm_city']  = normalise_city(row.get('location', ''))
                row['_norm_name']  = normalise_name(row.get('worker_name', ''))
                row['_rate_unit_suspect'] = flag_rate_suspect(row.get('rate', ''))
                row['_intra_source_duplicate'] = False
                records.append(row)
                continue

            row['_norm_email'] = normalise_email(raw_email_field)
            row['_norm_phone'] = None
            row['_norm_city']  = normalise_city(row.get('location', ''))
            row['_norm_name']  = normalise_name(row.get('worker_name', ''))
            row['_rate_unit_suspect'] = flag_rate_suspect(row.get('rate', ''))
            row['_skip_reason'] = None

            if row['_norm_email'] and row['_norm_email'] in seen_emails:
                print(f"  [S2 L{i}] WARN: within-source duplicate email "
                      f"{row['_norm_email']!r} name={row.get('worker_name','')!r}")
                row['_intra_source_duplicate'] = True
            else:
                row['_intra_source_duplicate'] = False
                if row['_norm_email']:
                    seen_emails.add(row['_norm_email'])

            records.append(row)

    return records

def read_source3(path: Path) -> list:
    """
    Read source3_cbnexus_contacts.csv.
    Cleans:
      - blank rows
      - repeated header row embedded as data (line 16)
      - normalises Verified column to True/False/None
    """
    HEADER_KEYS = {'name', 'phone number', 'city', 'verified', 'projects completed'}
    records = []
    seen_phones = set()

    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            row['_source_line'] = i
            row['_source'] = 'source3'

            if is_blank_row(row):
                print(f"  [S3 L{i}] SKIP: blank row")
                continue

            if is_repeated_header(row, HEADER_KEYS):
                print(f"  [S3 L{i}] SKIP: repeated header row embedded as data")
                continue

            row['_norm_email'] = None
            row['_norm_phone'] = normalise_phone(row.get('Phone Number', ''))
            row['_norm_city']  = normalise_city(row.get('City', ''))
            row['_norm_name']  = normalise_name(row.get('Name', ''))

            v = (row.get('Verified') or '').strip().lower()
            if v in ('y', 'yes'):
                row['_verified_bool'] = True
            elif v in ('n', 'no'):
                row['_verified_bool'] = False
            else:
                row['_verified_bool'] = None

            row['_skip_reason'] = None

            if row['_norm_phone'] and row['_norm_phone'] in seen_phones:
                print(f"  [S3 L{i}] WARN: within-source duplicate phone "
                      f"{row['_norm_phone']!r} name={row.get('Name','')!r}")
                row['_intra_source_duplicate'] = True
            else:
                row['_intra_source_duplicate'] = False
                if row['_norm_phone']:
                    seen_phones.add(row['_norm_phone'])

            records.append(row)

    return records

class UnionFind:
    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank   = [0] * n

    def find(self, x: int) -> int:
        """Path-compressed find."""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: int, y: int) -> bool:
        """
        Union by rank.  Returns True if x and y were in different sets
        (i.e. a merge actually happened).
        """
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return False
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1
        return True

def build_canonical(group: list) -> dict:
    """
    Given a group of records that Union-Find has decided are the same person,
    produce a single canonical dict.

    Resolution strategy:
      - email:   take the first non-None value found (prefer source1/source2
                 order since source3 has none).
      - phone:   take the first non-None value found (prefer source1/source3).
      - name:    prefer source1 Full Name; fall back to source2/3.
      - city:    prefer source1; if multiple distinct canonicals, record all
                 pipe-separated and set city_conflict=True.
      - sources: comma-separated list of which sources contributed.
    """
    def first_nonblank(records, *keys):
        for r in records:
            for k in keys:
                v = r.get(k)
                if v and str(v).strip():
                    return str(v).strip()
        return None

    source_order = {'source1': 0, 'source2': 1, 'source3': 2}
    group_sorted = sorted(group, key=lambda r: source_order.get(r['_source'], 9))

    email  = first_nonblank(group_sorted, '_norm_email')
    phone  = first_nonblank(group_sorted, '_norm_phone')
    name   = first_nonblank(group_sorted, '_norm_name')

    cities = []
    for r in group_sorted:
        c = r.get('_norm_city', '')
        if c and c not in cities:
            cities.append(c)

    city_conflict = len(cities) > 1
    city_canonical = ' | '.join(cities)

    sources_present = sorted({r['_source'] for r in group})

    ctc_raw     = first_nonblank(group_sorted, 'Current CTC')
    ctc_suspect = any(r.get('_ctc_unit_suspect', False) for r in group_sorted)

    all_skills = []
    for r in group_sorted:
        for sk_key in ('Skills', 'skill_tags'):
            raw_sk = r.get(sk_key, '')
            if raw_sk:
                for s in raw_sk.split(','):
                    s = s.strip().lower()
                    if s and s not in all_skills:
                        all_skills.append(s)

    return {
        'canonical_email':    email,
        'canonical_phone':    phone,
        'canonical_name':     name,
        'canonical_city':     city_canonical,
        'city_conflict':      city_conflict,
        'sources':            ','.join(sources_present),
        'source_count':       len(sources_present),
        'merged_skills':      ', '.join(all_skills),
        'ctc_raw':            ctc_raw,
        'ctc_unit_suspect':   ctc_suspect,
    }

SCHEMA = """
CREATE TABLE IF NOT EXISTS persons (
    person_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name    TEXT,
    canonical_email   TEXT,
    canonical_phone   TEXT,
    canonical_city    TEXT,
    city_conflict     INTEGER DEFAULT 0,
    sources           TEXT,
    source_count      INTEGER,
    merged_skills     TEXT,
    ctc_raw           TEXT,
    ctc_unit_suspect  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS source_records (
    record_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id         INTEGER REFERENCES persons(person_id),
    source_name       TEXT,
    source_line       INTEGER,
    raw_json          TEXT,
    norm_email        TEXT,
    norm_phone        TEXT,
    norm_city         TEXT,
    norm_name         TEXT,
    intra_dup_flag    INTEGER DEFAULT 0,
    skip_reason       TEXT
);

CREATE TABLE IF NOT EXISTS match_log (
    log_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id         INTEGER REFERENCES persons(person_id),
    match_type        TEXT,
    matched_field     TEXT,
    source_a          TEXT,
    source_line_a     INTEGER,
    source_b          TEXT,
    source_line_b     INTEGER,
    notes             TEXT
);

CREATE TABLE IF NOT EXISTS needs_review (
    review_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id_a       INTEGER REFERENCES source_records(record_id),
    record_id_b       INTEGER REFERENCES source_records(record_id),
    fuzzy_score       REAL,
    match_basis       TEXT,
    name_a            TEXT,
    name_b            TEXT,
    city_a            TEXT,
    city_b            TEXT,
    email_a           TEXT,
    phone_a           TEXT,
    email_b           TEXT,
    phone_b           TEXT,
    conflict_notes    TEXT
);
"""

def init_db(db_path: Path) -> sqlite3.Connection:
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn

def run_pipeline():
    print("=" * 70)
    print("CONSULTBAE -- Entity Resolution Pipeline")
    print("=" * 70)

    print("\n[1] Reading and cleaning source files ...")
    s1 = read_source1(S1_PATH)
    s2 = read_source2(S2_PATH)
    s3 = read_source3(S3_PATH)

    all_records = s1 + s2 + s3
    n = len(all_records)
    print(f"\n  Loaded: {len(s1)} s1 rows | {len(s2)} s2 rows | {len(s3)} s3 rows "
          f"= {n} total (after removing blank/header rows)")

    email_to_idx = defaultdict(list)
    phone_to_idx = defaultdict(list)

    for idx, rec in enumerate(all_records):
        e = rec.get('_norm_email')
        p = rec.get('_norm_phone')
        if e:
            email_to_idx[e].append(idx)
        if p:
            phone_to_idx[p].append(idx)

    print("\n[2] Running Union-Find entity resolution ...")
    uf = UnionFind(n)
    merge_events = []

    def do_union(idx_a, idx_b, match_type, matched_value):
        """Union two record indices and record the event."""
        merged = uf.union(idx_a, idx_b)
        if merged:
            ra, rb = all_records[idx_a], all_records[idx_b]
            merge_events.append({
                'match_type':    match_type,
                'matched_field': matched_value,
                'source_a':      ra['_source'],
                'source_line_a': ra['_source_line'],
                'source_b':      rb['_source'],
                'source_line_b': rb['_source_line'],
                'notes':         f"{ra.get('_norm_name','?')} <-> {rb.get('_norm_name','?')}",
            })

    for email, indices in email_to_idx.items():
        for i in range(len(indices) - 1):
            do_union(indices[i], indices[i + 1], 'email', email)

    for phone, indices in phone_to_idx.items():
        for i in range(len(indices) - 1):
            do_union(indices[i], indices[i + 1], 'phone', phone)

    components = defaultdict(list)
    for idx in range(n):
        root = uf.find(idx)
        components[root].append(idx)

    print(f"  Merge events: {len(merge_events)}")
    print(f"  Unique person components found: {len(components)}")

    print("\n[3] Running fuzzy name+city fallback (singletons only) ...")

    singleton_indices = [
        idx_list[0]
        for idx_list in components.values()
        if len(idx_list) == 1
    ]

    singleton_indices = [
        i for i in singleton_indices
        if all_records[i].get('_skip_reason') is None
    ]

    fuzzy_candidates = []

    checked_pairs = set()
    for i, idx_a in enumerate(singleton_indices):
        for idx_b in singleton_indices[i + 1:]:
            pair_key = frozenset([idx_a, idx_b])
            if pair_key in checked_pairs:
                continue
            checked_pairs.add(pair_key)

            ra = all_records[idx_a]
            rb = all_records[idx_b]

            name_a = ra.get('_norm_name', '')
            name_b = rb.get('_norm_name', '')
            city_a = ra.get('_norm_city', '')
            city_b = rb.get('_norm_city', '')

            if not name_a or not name_b:
                continue

            name_score = fuzz.token_sort_ratio(name_a, name_b)
            city_score = fuzz.token_sort_ratio(city_a, city_b)

            if name_score >= FUZZY_THRESHOLD and city_score >= FUZZY_THRESHOLD:
                ea, pa = ra.get('_norm_email'), ra.get('_norm_phone')
                eb, pb = rb.get('_norm_email'), rb.get('_norm_phone')
                conflict_notes = ""
                if ea and eb and ea != eb:
                    conflict_notes += f"different emails ({ea} vs {eb}); "
                if pa and pb and pa != pb:
                    conflict_notes += f"different phones ({pa} vs {pb}); "

                print(f"  FUZZY CANDIDATE: {name_a!r}+{city_a!r} <-> "
                      f"{name_b!r}+{city_b!r}  "
                      f"name_score={name_score} city_score={city_score}"
                      + (f"  CONFLICT: {conflict_notes}" if conflict_notes else ""))

                fuzzy_candidates.append({
                    'idx_a':          idx_a,
                    'idx_b':          idx_b,
                    'fuzzy_score':    (name_score + city_score) / 2,
                    'name_a':         name_a,
                    'name_b':         name_b,
                    'city_a':         city_a,
                    'city_b':         city_b,
                    'email_a':        ea,
                    'phone_a':        pa,
                    'email_b':        eb,
                    'phone_b':        pb,
                    'conflict_notes': conflict_notes.strip('; '),
                })

    print(f"  Fuzzy candidates flagged for review: {len(fuzzy_candidates)}")

    print("\n[4] Writing to SQLite database ...")
    conn = init_db(DB_PATH)
    cur  = conn.cursor()

    root_to_person_id = {}

    idx_to_record_id = {}

    for root, idx_list in components.items():
        group = [all_records[i] for i in idx_list]
        canon = build_canonical(group)
        cur.execute("""
            INSERT INTO persons
              (canonical_name, canonical_email, canonical_phone,
               canonical_city, city_conflict, sources, source_count,
               merged_skills, ctc_raw, ctc_unit_suspect)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            canon['canonical_name'],
            canon['canonical_email'],
            canon['canonical_phone'],
            canon['canonical_city'],
            int(canon['city_conflict']),
            canon['sources'],
            canon['source_count'],
            canon['merged_skills'],
            canon['ctc_raw'],
            int(canon['ctc_unit_suspect']),
        ))
        person_id = cur.lastrowid
        root_to_person_id[root] = person_id

    for idx, rec in enumerate(all_records):
        root      = uf.find(idx)
        person_id = root_to_person_id.get(root)

        raw_copy = {k: v for k, v in rec.items() if not k.startswith('_')}

        cur.execute("""
            INSERT INTO source_records
              (person_id, source_name, source_line, raw_json,
               norm_email, norm_phone, norm_city, norm_name,
               intra_dup_flag, skip_reason)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            person_id,
            rec['_source'],
            rec['_source_line'],
            json.dumps(raw_copy, ensure_ascii=False),
            rec.get('_norm_email'),
            rec.get('_norm_phone'),
            rec.get('_norm_city'),
            rec.get('_norm_name'),
            int(rec.get('_intra_source_duplicate', False)),
            rec.get('_skip_reason'),
        ))
        idx_to_record_id[idx] = cur.lastrowid

    source_line_to_idx = {}
    for idx, rec in enumerate(all_records):
        key = (rec['_source'], rec['_source_line'])
        source_line_to_idx[key] = idx

    for event in merge_events:
        key_a = (event['source_a'], event['source_line_a'])
        idx_a = source_line_to_idx.get(key_a)
        p_id = None
        if idx_a is not None:
            root = uf.find(idx_a)
            p_id = root_to_person_id.get(root)

        cur.execute("""
            INSERT INTO match_log
              (person_id, match_type, matched_field,
               source_a, source_line_a, source_b, source_line_b, notes)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            p_id,
            event['match_type'],
            event['matched_field'],
            event['source_a'],
            event['source_line_a'],
            event['source_b'],
            event['source_line_b'],
            event['notes'],
        ))

    for fc in fuzzy_candidates:
        record_id_a = idx_to_record_id.get(fc['idx_a'])
        record_id_b = idx_to_record_id.get(fc['idx_b'])
        cur.execute("""
            INSERT INTO needs_review
              (record_id_a, record_id_b, fuzzy_score, match_basis,
               name_a, name_b, city_a, city_b,
               email_a, phone_a, email_b, phone_b, conflict_notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            record_id_a,
            record_id_b,
            fc['fuzzy_score'],
            'name+city',
            fc['name_a'],
            fc['name_b'],
            fc['city_a'],
            fc['city_b'],
            fc['email_a'],
            fc['phone_a'],
            fc['email_b'],
            fc['phone_b'],
            fc['conflict_notes'],
        ))

    conn.commit()
    conn.close()
    print(f"  Database written to: {DB_PATH}")

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    total_input = len(s1) + len(s2) + len(s3)
    unique_persons = len(components)

    source_count_dist = defaultdict(int)
    for root, idx_list in components.items():
        group = [all_records[i] for i in idx_list]
        distinct_sources = len({r['_source'] for r in group})
        source_count_dist[distinct_sources] += 1

    print(f"  Total input rows (after blank/header removal):  {total_input}")
    print(f"  Unique persons resolved:                         {unique_persons}")
    print(f"  Persons appearing in 1 source only:             {source_count_dist[1]}")
    print(f"  Persons appearing in exactly 2 sources:         {source_count_dist[2]}")
    print(f"  Persons appearing in all 3 sources:             {source_count_dist[3]}")
    print(f"  Pairs flagged for manual review (fuzzy):        {len(fuzzy_candidates)}")
    print(f"  Hard merge events logged:                        {len(merge_events)}")
    print(f"\n  Output: {DB_PATH}")
    print("=" * 70)

if __name__ == "__main__":
    run_pipeline()
