import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from ingestion import wistia_client as client


def event(stamp, key="e"):
    return {"received_at": stamp, "event_key": key}


class PaginationTests(unittest.TestCase):
    def test_cap_is_failure_not_completion(self):
        with self.assertRaises(client.IncompleteExtraction):
            client.collect_pages(lambda _: [event("2026-10-01T00:00:00Z")],
                                 "received_at", None, 2, "events")

    def test_equal_watermark_timestamp_is_replayed_across_pages(self):
        stamp = "2026-10-01T00:00:00Z"
        pages = {1: [event(stamp, "a")], 2: [event(stamp, "b")],
                 3: [event("2026-09-30T00:00:00Z")]}
        rows = client.collect_pages(pages.get, "received_at", stamp, 3, "events")
        self.assertEqual([r["event_key"] for r in rows], ["a", "b"])

    def test_empty_page_proves_completion(self):
        self.assertEqual(client.collect_pages(lambda _: [], "received_at", None, 1, "events"), [])

    def test_midstream_request_failure_discards_incomplete_batch(self):
        pages = {1: [event("2026-10-01T00:00:00Z")], 2: None}
        self.assertIsNone(client.collect_pages(pages.get, "received_at", None, 2, "events"))

    def test_unordered_source_fails(self):
        with self.assertRaises(client.IncompleteExtraction):
            client.collect_pages(lambda _: [event("2026-09-01T00:00:00Z"),
                                             event("2026-10-01T00:00:00Z")],
                                 "received_at", None, 2, "events")

    def test_timezone_normalization(self):
        self.assertEqual(client.parse_timestamp("2026-10-01T01:00:00+01:00"),
                         client.parse_timestamp("2026-10-01T00:00:00Z"))


class WatermarkTests(unittest.TestCase):
    @patch.object(client.boto3, "resource")
    def test_conditional_initial_write(self, resource):
        table = resource.return_value.Table.return_value
        client.set_watermark("events#video", "2026-10-01", None)
        self.assertEqual(table.put_item.call_args.kwargs["ConditionExpression"],
                         "attribute_not_exists(endpoint)")

    @patch.object(client.boto3, "resource")
    def test_stale_writer_conflict_is_propagated(self, resource):
        table = resource.return_value.Table.return_value
        table.put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        with self.assertRaises(ClientError):
            client.set_watermark("events#video", "2026-10-02", "2026-10-01")
        self.assertEqual(table.put_item.call_args.kwargs["ExpressionAttributeValues"],
                         {":expected": "2026-10-01"})

    @patch.object(client.boto3, "resource")
    def test_reads_are_strongly_consistent(self, resource):
        table = resource.return_value.Table.return_value
        table.get_item.return_value = {}
        self.assertIsNone(client.get_watermark("events#video"))
        table.get_item.assert_called_once_with(Key={"endpoint": "events#video"}, ConsistentRead=True)

    def test_watermark_cannot_regress(self):
        with self.assertRaises(ValueError):
            client.set_watermark("x", "2026-09-01", "2026-10-01")

    @patch.object(client, "set_watermark")
    @patch.object(client, "save_to_bronze")
    @patch.object(client, "get_all_events", side_effect=client.IncompleteExtraction("cap reached"))
    @patch.object(client, "get_media_metadata", return_value={"name": "video", "updated": "2026-10-01", "created": "2026-10-01"})
    @patch.object(client, "get_media_stat", return_value={"play_count": 1, "load_count": 2})
    @patch.object(client, "get_token", return_value="test-token")
    def test_cap_aborts_run_without_advancing_watermark(self, token, stats, metadata, events, save, watermark):
        yesterday = (client.datetime.now(client.timezone.utc).date() - client.timedelta(days=1)).isoformat()
        with patch.object(client, "get_watermark", return_value=yesterday):
            with self.assertRaises(client.IncompleteExtraction):
                client.run()
        watermark.assert_not_called()

    @patch.object(client, "set_watermark")
    @patch.object(client, "save_to_bronze", side_effect=OSError("storage unavailable"))
    @patch.object(client, "get_media_stat", return_value={"play_count": 1, "load_count": 2})
    @patch.object(client, "get_token", return_value="test-token")
    def test_bronze_write_failure_never_advances_progress(self, token, stats, save, watermark):
        with self.assertRaises(OSError):
            client.run()
        watermark.assert_not_called()


if __name__ == "__main__":
    unittest.main()
