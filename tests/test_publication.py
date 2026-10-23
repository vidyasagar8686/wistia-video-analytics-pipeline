import json
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from transform.pipeline_support import mask_ip, publish_catalog, publish_local, read_pointer, suppression_keys
from botocore.exceptions import ClientError


class PrivacyTests(unittest.TestCase):
    def test_ipv4_network(self):
        self.assertEqual(mask_ip("97.100.12.233"), "97.100.12.0/24")

    def test_ipv6_network(self):
        self.assertEqual(mask_ip("2001:db8:abcd:1234::1"), "2001:db8:abcd::/48")

    def test_invalid_ip_never_passes_through(self):
        for value in [None, "", "not-an-ip"]:
            self.assertIsNone(mask_ip(value))

    def test_suppression_registry_rejects_missing_or_invalid_keys(self):
        for payload in [{}, {"visitor_keys": "all"}, {"visitor_keys": [None]}]:
            with self.assertRaises(ValueError):
                suppression_keys(payload)
        self.assertEqual(suppression_keys({"visitor_keys": ["a", "a"]}), {"a"})


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.tables = {name: {"columns": [{"Name": "id", "Type": "string"}],
                              "location": "s3://bucket/gold_versions/run/" + name + "/"}
                       for name in ["dim_media", "dim_visitor", "fact_media_daily", "fact_media_engagement"]}

    def test_catalog_failure_never_switches_release(self):
        glue, s3 = Mock(), Mock()
        glue.create_table.side_effect = RuntimeError("catalog unavailable")
        with self.assertRaises(RuntimeError):
            publish_catalog(glue, s3, "bucket", "", "run", self.tables, '"old"')
        s3.put_object.assert_not_called()

    def test_all_tables_registered_before_conditional_pointer(self):
        glue, s3 = Mock(), Mock()
        def check_order(**kwargs):
            self.assertEqual(glue.create_table.call_count, 4)
            self.assertEqual(kwargs["IfMatch"], '"old"')
        s3.put_object.side_effect = check_order
        publish_catalog(glue, s3, "bucket", "", "run", self.tables, '"old"')

    def test_first_publication_requires_no_existing_pointer(self):
        glue, s3 = Mock(), Mock()
        publish_catalog(glue, s3, "bucket", "", "run", self.tables, None)
        self.assertEqual(s3.put_object.call_args.kwargs["IfNoneMatch"], "*")

    def test_concurrent_pointer_change_is_failure(self):
        glue, s3 = Mock(), Mock()
        s3.put_object.side_effect = ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        with self.assertRaises(ClientError):
            publish_catalog(glue, s3, "bucket", "", "run", self.tables, '"stale"')

    def test_pointer_access_denied_is_not_treated_as_first_release(self):
        s3 = Mock()
        s3.get_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
        with self.assertRaises(ClientError):
            read_pointer(s3, "bucket", "releases/current.json")

    def test_older_execution_cannot_replace_already_published_newer_run(self):
        s3 = Mock()
        s3.get_object.return_value = {"ETag": '"new"', "Body": io.BytesIO(
            json.dumps({"started_at": "2026-10-02T00:00:00Z"}).encode())}
        with self.assertRaises(RuntimeError):
            read_pointer(s3, "bucket", "releases/current.json", "2026-10-01T00:00:00Z")

    def test_local_release_pointer(self):
        with tempfile.TemporaryDirectory() as directory:
            publish_local(directory, "run", self.tables)
            data = json.loads((Path(directory) / "releases/current.json").read_text())
            self.assertEqual(data["run_id"], "run")
            self.assertEqual(len(data["tables"]), 4)
