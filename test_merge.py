"""
test_merge.py
==============
Comprehensive Test Suite for Task 1: Entity Resolution & Merge Pipeline
Includes:
  1. Unit Tests for Normalization (email, phone, city, name, date, CTC/rate flags)
  2. Data Ingestion & Edge-Case Handling Tests (blank rows, repeated headers, shifted columns)
  3. Union-Find Disjoint Set & Transitive Resolution Tests
  4. End-to-End Pipeline Execution & SQLite Database Integrity Tests
"""

import sys
import unittest
import sqlite3
from pathlib import Path

# Add task1_merge to Python path
BASE_DIR = Path(__file__).parent
TASK1_DIR = BASE_DIR / "task1_merge"
if str(TASK1_DIR) not in sys.path:
    sys.path.insert(0, str(TASK1_DIR))

from merge_pipeline import (
    normalise_email,
    normalise_phone,
    normalise_city,
    normalise_name,
    normalise_date,
    flag_ctc_suspect,
    flag_rate_suspect,
    is_blank_row,
    is_repeated_header,
    read_source1,
    read_source2,
    read_source3,
    UnionFind,
    build_canonical,
    run_pipeline,
    S1_PATH,
    S2_PATH,
    S3_PATH,
    DB_PATH,
)


class TestNormalisation(unittest.TestCase):
    """Unit tests for identifier normalisation and formatting functions."""

    def test_normalise_email(self):
        self.assertEqual(normalise_email("John.Doe@Example.COM"), "john.doe@example.com")
        self.assertEqual(normalise_email("  user@domain.in  "), "user@domain.in")
        self.assertIsNone(normalise_email(""))
        self.assertIsNone(normalise_email("   "))
        self.assertIsNone(normalise_email(None))

    def test_normalise_phone(self):
        # +91 prefix
        self.assertEqual(normalise_phone("+919000000254"), "9000000254")
        # 91 prefix (12 digits)
        self.assertEqual(normalise_phone("919000000231"), "9000000231")
        # Leading 0 (11 digits)
        self.assertEqual(normalise_phone("09000000287"), "9000000287")
        # Bare 10 digits
        self.assertEqual(normalise_phone("9000000237"), "9000000237")
        # Hyphenated / spaces
        self.assertEqual(normalise_phone("+91-9000000131"), "9000000131")
        self.assertEqual(normalise_phone("900 000 0254"), "9000000254")
        # Invalid numbers (too short / too long)
        self.assertIsNone(normalise_phone("12345"))
        self.assertIsNone(normalise_phone("911234567890123"))
        self.assertIsNone(normalise_phone(""))
        self.assertIsNone(normalise_phone(None))

    def test_normalise_city(self):
        # Alias mappings
        self.assertEqual(normalise_city("bangalore"), "Bengaluru")
        self.assertEqual(normalise_city("bengaluru"), "Bengaluru")
        self.assertEqual(normalise_city("gurgaon"), "Gurugram")
        self.assertEqual(normalise_city("gurugram"), "Gurugram")
        self.assertEqual(normalise_city("pune"), "Pune")
        self.assertEqual(normalise_city("PUNE"), "Pune")
        self.assertEqual(normalise_city("noida"), "Noida")
        # Delhi variations preserved distinct
        self.assertEqual(normalise_city("delhi"), "Delhi")
        self.assertEqual(normalise_city("new delhi"), "New Delhi")
        self.assertEqual(normalise_city("delhi ncr"), "Delhi NCR")
        # Unknown cities title-cased
        self.assertEqual(normalise_city("hyderabad"), "Hyderabad")
        self.assertEqual(normalise_city("   mumbai   "), "Mumbai")
        self.assertEqual(normalise_city(""), "")
        self.assertEqual(normalise_city(None), "")

    def test_normalise_name(self):
        self.assertEqual(normalise_name("  john   doe  "), "John Doe")
        self.assertEqual(normalise_name("PRIYA SAXENA"), "Priya Saxena")
        self.assertEqual(normalise_name(""), "")
        self.assertEqual(normalise_name(None), "")

    def test_normalise_date(self):
        # DD-MM-YYYY
        self.assertEqual(normalise_date("15-08-2026"), "2026-08-15")
        # YYYY-MM-DD
        self.assertEqual(normalise_date("2026-07-20"), "2026-07-20")
        # MM/DD/YYYY (e.g., Naukri format)
        self.assertEqual(normalise_date("07/13/2026"), "2026-07-13")
        # DD Mon YYYY
        self.assertEqual(normalise_date("20 Jul 2026"), "2026-07-20")
        self.assertIsNone(normalise_date(""))
        self.assertIsNone(normalise_date(None))

    def test_flag_ctc_suspect(self):
        # Values < 200 indicate LPA decimals
        self.assertTrue(flag_ctc_suspect("7.8"))
        self.assertTrue(flag_ctc_suspect("12.5"))
        self.assertTrue(flag_ctc_suspect("24"))
        # Values > 200 indicate full rupee figures
        self.assertFalse(flag_ctc_suspect("780000"))
        self.assertFalse(flag_ctc_suspect("1250000"))
        self.assertFalse(flag_ctc_suspect(""))
        self.assertFalse(flag_ctc_suspect(None))

    def test_flag_rate_suspect(self):
        self.assertTrue(flag_rate_suspect("50k/month"))
        self.assertTrue(flag_rate_suspect("500/hr"))
        self.assertTrue(flag_rate_suspect("1200 /hr"))
        self.assertFalse(flag_rate_suspect("50000"))
        self.assertFalse(flag_rate_suspect(""))
        self.assertFalse(flag_rate_suspect(None))


class TestCleaningAndEdgeCases(unittest.TestCase):
    """Unit tests for row cleaning, blank detection, and repeated headers."""

    def test_is_blank_row(self):
        self.assertTrue(is_blank_row({"col1": "", "col2": "   ", "col3": ""}))
        self.assertFalse(is_blank_row({"col1": "", "col2": "value", "col3": ""}))
        # Pipeline metadata keys starting with '_' are ignored
        self.assertTrue(is_blank_row({"_source": "source1", "_source_line": 2, "col1": ""}))

    def test_is_repeated_header(self):
        header_keys = {"name", "phone number", "city", "verified", "projects completed"}
        repeated_row = {
            "Name": "Name",
            "Phone Number": "Phone Number",
            "City": "City",
            "Verified": "Verified",
            "Projects Completed": "Projects Completed",
        }
        data_row = {
            "Name": "Rahul Sharma",
            "Phone Number": "9000000201",
            "City": "Noida",
            "Verified": "Yes",
            "Projects Completed": "5",
        }
        self.assertTrue(is_repeated_header(repeated_row, header_keys))
        self.assertFalse(is_repeated_header(data_row, header_keys))


class TestUnionFindAndEntityResolution(unittest.TestCase):
    """Unit tests for Disjoint-Set Union-Find and canonical builder."""

    def test_union_find_operations(self):
        uf = UnionFind(5)
        self.assertEqual(uf.find(0), 0)
        self.assertEqual(uf.find(1), 1)

        # Merge 0 and 1
        self.assertTrue(uf.union(0, 1))
        self.assertEqual(uf.find(0), uf.find(1))
        # Second union returns False (already merged)
        self.assertFalse(uf.union(0, 1))

        # Transitive merge: 1 and 2 => 0, 1, 2 are in the same set
        self.assertTrue(uf.union(1, 2))
        self.assertEqual(uf.find(0), uf.find(2))

        # Separate set 3 and 4
        self.assertTrue(uf.union(3, 4))
        self.assertNotEqual(uf.find(0), uf.find(3))

        # Merge the two components
        self.assertTrue(uf.union(2, 3))
        self.assertEqual(uf.find(0), uf.find(4))

    def test_build_canonical(self):
        group = [
            {
                "_source": "source1",
                "_norm_name": "Aarav Sharma",
                "_norm_email": "aarav.sharma@example.com",
                "_norm_phone": "9000000201",
                "_norm_city": "Noida",
                "Current CTC": "12.5",
                "_ctc_unit_suspect": True,
                "Skills": "python, fast-api, postgresql",
            },
            {
                "_source": "source2",
                "_norm_name": "Aarav S.",
                "_norm_email": "aarav.sharma@example.com",
                "_norm_phone": None,
                "_norm_city": "Delhi NCR",
                "rate": "60k/month",
                "_rate_unit_suspect": True,
                "skill_tags": "python, docker",
            },
        ]
        canonical = build_canonical(group)
        self.assertEqual(canonical["canonical_name"], "Aarav Sharma")
        self.assertEqual(canonical["canonical_email"], "aarav.sharma@example.com")
        self.assertEqual(canonical["canonical_phone"], "9000000201")
        # City conflict because Noida != Delhi NCR
        self.assertTrue(canonical["city_conflict"])
        self.assertIn("Noida", canonical["canonical_city"])
        self.assertIn("Delhi NCR", canonical["canonical_city"])
        self.assertEqual(canonical["source_count"], 2)
        self.assertEqual(canonical["sources"], "source1,source2")
        self.assertTrue(canonical["ctc_unit_suspect"])
        # Skills deduped and combined
        for sk in ["python", "fast-api", "postgresql", "docker"]:
            self.assertIn(sk, canonical["merged_skills"])


class TestPipelineIntegration(unittest.TestCase):
    """End-to-end integration tests verifying source reading, pipeline execution, and database state."""

    @classmethod
    def setUpClass(cls):
        # Run pipeline once before testing database
        run_pipeline()
        cls.conn = sqlite3.connect(DB_PATH)
        cls.conn.row_factory = sqlite3.Row

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_source_files_exist(self):
        self.assertTrue(S1_PATH.exists(), f"Source 1 CSV missing at {S1_PATH}")
        self.assertTrue(S2_PATH.exists(), f"Source 2 CSV missing at {S2_PATH}")
        self.assertTrue(S3_PATH.exists(), f"Source 3 CSV missing at {S3_PATH}")

    def test_source_row_counts(self):
        s1 = read_source1(S1_PATH)
        s2 = read_source2(S2_PATH)
        s3 = read_source3(S3_PATH)
        self.assertEqual(len(s1), 42, "Source 1 should yield 42 records")
        self.assertEqual(len(s2), 31, "Source 2 should yield 31 records (1 blank skipped)")
        self.assertEqual(len(s3), 30, "Source 3 should yield 30 records (1 header skipped)")
        self.assertEqual(len(s1) + len(s2) + len(s3), 103, "Total input records should be 103")

    def test_database_tables_exist(self):
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row[0] for row in cur.fetchall()}
        required_tables = {"persons", "source_records", "match_log", "needs_review"}
        self.assertTrue(required_tables.issubset(tables), f"Missing tables: {required_tables - tables}")

    def test_database_record_counts(self):
        cur = self.conn.cursor()

        cur.execute("SELECT COUNT(*) FROM source_records")
        self.assertEqual(cur.fetchone()[0], 103, "source_records must have 103 rows")

        cur.execute("SELECT COUNT(*) FROM persons")
        self.assertEqual(cur.fetchone()[0], 61, "persons table must resolve to 61 unique entities")

        cur.execute("SELECT COUNT(*) FROM match_log")
        self.assertEqual(cur.fetchone()[0], 42, "match_log must record 42 merge events")

        cur.execute("SELECT COUNT(*) FROM needs_review")
        self.assertEqual(cur.fetchone()[0], 5, "needs_review must have 5 fuzzy candidates")

    def test_source_count_distribution(self):
        cur = self.conn.cursor()
        cur.execute("SELECT source_count, COUNT(*) FROM persons GROUP BY source_count")
        dist = dict(cur.fetchall())
        self.assertEqual(dist.get(3, 0), 15, "15 persons should appear in all 3 sources")
        self.assertEqual(dist.get(2, 0), 10, "10 persons should appear in exactly 2 sources")
        self.assertEqual(dist.get(1, 0), 36, "36 persons should appear in 1 source only")

    def test_city_conflicts_and_suspect_flags(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM persons WHERE city_conflict = 1")
        self.assertEqual(cur.fetchone()[0], 5, "Exactly 5 persons must have city_conflict=1")

        cur.execute("SELECT COUNT(*) FROM persons WHERE ctc_unit_suspect = 1")
        self.assertEqual(cur.fetchone()[0], 19, "Exactly 19 persons must have ctc_unit_suspect=1")

    def test_data_integrity_constraints(self):
        cur = self.conn.cursor()

        # All canonical emails must be lowercase
        cur.execute("SELECT canonical_email FROM persons WHERE canonical_email IS NOT NULL")
        for (email,) in cur.fetchall():
            self.assertEqual(email, email.lower(), f"Email is not lowercased: {email}")

        # All canonical phones must be exactly 10 digits
        cur.execute("SELECT canonical_phone FROM persons WHERE canonical_phone IS NOT NULL")
        for (phone,) in cur.fetchall():
            self.assertTrue(phone.isdigit() and len(phone) == 10, f"Invalid canonical phone: {phone}")

        # Every source record must be linked to a valid person_id
        cur.execute("SELECT COUNT(*) FROM source_records WHERE person_id IS NULL")
        self.assertEqual(cur.fetchone()[0], 0, "No source record should have an unassigned person_id")


if __name__ == "__main__":
    unittest.main(verbosity=2)
