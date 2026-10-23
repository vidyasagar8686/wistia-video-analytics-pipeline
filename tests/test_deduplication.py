"""Exercise the production deduplication functions on real Spark DataFrames."""
from datetime import date, datetime
import os
from pathlib import Path
import sys
import unittest

from transform.pipeline_support import DataContractError, deduplicate_records, validate_business_keys


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "set RUN_SPARK_TESTS=1 for real Spark tests")
class DeduplicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import pyspark
        os.environ["SPARK_HOME"] = str(Path(pyspark.__file__).parent)
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        os.environ["PYSPARK_PYTHON"] = sys.executable
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("wistia-dedup-tests")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.showConsoleProgress", "false")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.conf.set("spark.sql.session.timeZone", "UTC")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def events(self, rows):
        return self.spark.createDataFrame(rows, "event_key string, visitor_key string, media_id string, watched double")

    def test_exact_replays_collapse_to_one_event(self):
        frame = self.events([("e1", "v1", "m1", 0.5)] * 3)
        self.assertEqual(deduplicate_records(frame, ["event_key"], "events").count(), 1)

    def test_distinct_keys_with_identical_metrics_remain_distinct(self):
        frame = self.events([("e1", "v1", "m1", 0.5), ("e2", "v1", "m1", 0.5)])
        self.assertEqual(deduplicate_records(frame, ["event_key"], "events").count(), 2)

    def test_conflicting_event_metric_has_no_arbitrary_hash_winner(self):
        frame = self.events([("e1", "v1", "m1", 0.5), ("e1", "v1", "m1", 0.9)])
        with self.assertRaisesRegex(DataContractError, "conflicting copies.*without a source revision"):
            deduplicate_records(frame, ["event_key"], "events")

    def test_event_key_cannot_change_video_or_visitor(self):
        for changed in [("e1", "v2", "m1", 0.5), ("e1", "v1", "m2", 0.5)]:
            with self.subTest(changed=changed), self.assertRaises(DataContractError):
                deduplicate_records(self.events([("e1", "v1", "m1", 0.5), changed]), ["event_key"], "events")

    def test_null_blank_and_padded_identifiers_are_rejected(self):
        for key in [None, "", " ", "\t", "\n", " e1", "e1 ", "e1\n"]:
            with self.subTest(key=key), self.assertRaises(DataContractError):
                validate_business_keys(self.events([(key, "v1", "m1", 0.5)]), ["event_key"], "events")

    def test_all_event_identity_columns_are_checked(self):
        for record in [("e1", None, "m1", 0.5), ("e1", "v1", "", 0.5)]:
            with self.subTest(record=record), self.assertRaises(DataContractError):
                validate_business_keys(self.events([record]), ["event_key", "visitor_key", "media_id"], "events")

    def test_missing_key_column_is_a_contract_failure(self):
        frame = self.spark.createDataFrame([(0.5,)], "watched double")
        with self.assertRaisesRegex(DataContractError, "missing business-key columns"):
            deduplicate_records(frame, ["event_key"], "events")

    def test_numeric_identifier_is_not_silently_coerced(self):
        frame = self.spark.createDataFrame([(123,)], "visitor_key long")
        with self.assertRaisesRegex(DataContractError, "must be a string"):
            validate_business_keys(frame, ["visitor_key"], "visitors")

    def test_invalid_key_is_not_hidden_by_a_valid_duplicate(self):
        frame = self.events([(None, "v1", "m1", 0.5), ("e1", "v1", "m1", 0.5)])
        with self.assertRaises(DataContractError):
            deduplicate_records(frame, ["event_key"], "events")

    def profiles(self, rows):
        return self.spark.createDataFrame(rows, "visitor_key string, last_active_at timestamp, browser string")

    def test_profile_selects_newest_activity_snapshot(self):
        frame = self.profiles([("v1", datetime(2026, 9, 1), "old"),
                               ("v1", datetime(2026, 10, 1), "new")])
        result = deduplicate_records(frame, ["visitor_key"], "visitors", "last_active_at").collect()
        self.assertEqual([r.browser for r in result], ["new"])

    def test_equal_latest_activity_with_different_payloads_fails(self):
        frame = self.profiles([("v1", datetime(2026, 10, 1), "one"),
                               ("v1", datetime(2026, 10, 1), "other")])
        with self.assertRaisesRegex(DataContractError, "conflicting copies.*last_active_at"):
            deduplicate_records(frame, ["visitor_key"], "visitors", "last_active_at")

    def test_equal_latest_identical_profiles_collapse(self):
        frame = self.profiles([("v1", datetime(2026, 10, 1), "same")] * 2)
        self.assertEqual(deduplicate_records(frame, ["visitor_key"], "visitors", "last_active_at").count(), 1)

    def test_older_conflicts_do_not_override_a_newer_profile(self):
        frame = self.profiles([("v1", datetime(2026, 9, 1), "old1"),
                               ("v1", datetime(2026, 9, 1), "old2"),
                               ("v1", datetime(2026, 10, 1), "new")])
        result = deduplicate_records(frame, ["visitor_key"], "visitors", "last_active_at").first()
        self.assertEqual(result.browser, "new")

    def test_null_ordering_value_is_not_silently_discarded(self):
        frame = self.profiles([("v1", None, "unknown"), ("v1", datetime(2026, 10, 1), "valid")])
        with self.assertRaisesRegex(DataContractError, "null or invalid ordering value"):
            deduplicate_records(frame, ["visitor_key"], "visitors", "last_active_at")

    def test_missing_ordering_column_fails(self):
        frame = self.spark.createDataFrame([("v1",)], "visitor_key string")
        with self.assertRaisesRegex(DataContractError, "missing ordering column"):
            deduplicate_records(frame, ["visitor_key"], "visitors", "last_active_at")

    def test_unparsed_ordering_string_is_not_compared_lexically(self):
        frame = self.spark.createDataFrame([("v1", "2026-10-01")], "visitor_key string, last_active_at string")
        with self.assertRaisesRegex(DataContractError, "parsed date or timestamp"):
            deduplicate_records(frame, ["visitor_key"], "visitors", "last_active_at")

    def test_media_orders_by_source_updated_timestamp(self):
        frame = self.spark.createDataFrame([
            ("m1", datetime(2026, 9, 1), "old"), ("m1", datetime(2026, 10, 1), "new")],
            "media_id string, updated_at timestamp, title string")
        self.assertEqual(deduplicate_records(frame, ["media_id"], "media", "updated_at").first().title, "new")

    def daily(self, rows):
        return self.spark.createDataFrame(rows,
            "media_id string, date date, load_count long, _snapshot_at timestamp")

    def test_daily_recollection_selects_later_observed_snapshot(self):
        frame = self.daily([("m1", date(2026, 9, 30), 10, datetime(2026, 10, 1, 8)),
                            ("m1", date(2026, 9, 30), 12, datetime(2026, 10, 1, 9))])
        result = deduplicate_records(frame, ["media_id", "date"], "daily_stats", "_snapshot_at", ["media_id"])
        self.assertEqual(result.first().load_count, 12)

    def test_same_snapshot_conflicting_daily_values_fail(self):
        frame = self.daily([("m1", date(2026, 9, 30), 10, datetime(2026, 10, 1)),
                            ("m1", date(2026, 9, 30), 12, datetime(2026, 10, 1))])
        with self.assertRaises(DataContractError):
            deduplicate_records(frame, ["media_id", "date"], "daily_stats", "_snapshot_at", ["media_id"])

    def test_missing_daily_date_is_not_grouped_into_a_null_key(self):
        frame = self.daily([("m1", None, 10, datetime(2026, 10, 1))])
        with self.assertRaises(DataContractError):
            deduplicate_records(frame, ["media_id", "date"], "daily_stats", "_snapshot_at", ["media_id"])

    def test_errors_do_not_include_offending_identity_values(self):
        secret = " private-visitor-id "
        frame = self.spark.createDataFrame([(secret,)], "visitor_key string")
        with self.assertRaises(DataContractError) as error:
            validate_business_keys(frame, ["visitor_key"], "visitors")
        self.assertNotIn("private-visitor-id", str(error.exception))


if __name__ == "__main__":
    unittest.main()
