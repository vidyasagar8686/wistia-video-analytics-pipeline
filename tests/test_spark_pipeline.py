"""Run real Spark subprocesses against synthetic JSON, without AWS credentials."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "set RUN_SPARK_TESTS=1 for real Spark tests")
class SparkPipelineTests(unittest.TestCase):
    def write(self, root, endpoint, name, rows):
        folder = root / "bronze" / endpoint / "load_date=2026-10-01"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / name).write_text(json.dumps(rows), encoding="utf-8")

    def fixture(self, root):
        videos = ["8hunphufxp", "9k4tbcdfg0"]
        for i, video in enumerate(videos):
            self.write(root, "media", video + ".json", {
                "hashed_id": video, "name": "Video", "created": "2026-09-01T00:00:00Z",
                "updated": "2026-09-01T00:00:00Z", "share_link": {"url": "https://example.com"},
                "duration": 60.0})
            daily_row = {"date": "2026-09-30", "load_count": 10, "play_count": 1, "hours_watched": 0.01}
            daily_payload = {
                "ingestion": {"media_id": video, "completed": True},
                "data": [daily_row]}
            self.write(root, "daily_stats", f"renamed_{i}.json" if i == 0 else video + "_123456.json",
                       daily_payload if i == 0 else [daily_row])
        profile = {"browser": "Firefox", "platform": "Linux", "mobile": False}
        event = {"event_key": "event1", "visitor_key": "visitor1", "media_id": videos[0],
                 "received_at": "2026-09-30T00:00:00Z", "percent_viewed": 0.5,
                 "ip": "2001:db8:abcd:1234::1", "country": "US", "region": "IL", "city": "Chicago",
                 "user_agent_details": profile}
        erased = {**event, "event_key": "erased-event", "visitor_key": "erased-visitor"}
        self.write(root, "events", "events.json", [event, event, erased])
        privacy = root / "privacy"
        privacy.mkdir()
        (privacy / "suppressed_visitors.json").write_text(json.dumps({"visitor_keys": ["erased-visitor"]}))
        self.write(root, "visitors", "visitors.json", [{
            "visitor_key": "visitor1", "created_at": "2026-09-01T00:00:00Z",
            "last_active_at": "2026-09-30T00:00:00Z", "load_count": 1, "play_count": 1,
            "user_agent_details": profile, "visitor_identity": {"email": "private@example.com"}}])

    def run_job(self, script, root, run_id, success=True):
        import pyspark
        env = {**os.environ, "SPARK_LOCAL_IP": "127.0.0.1",
               "SPARK_HOME": str(Path(pyspark.__file__).parent),
               "PYSPARK_SUBMIT_ARGS": "--master local[2] --conf spark.sql.shuffle.partitions=4 pyspark-shell"}
        result = subprocess.run([sys.executable, str(ROOT / "transform" / script),
                                 "--base_path", str(root), "--run_id", run_id],
                                cwd=ROOT, env=env,
                                capture_output=True, text=True, timeout=180)
        if success:
            self.assertEqual(result.returncode, 0, result.stdout[-4000:] + result.stderr[-6000:])
        else:
            self.assertNotEqual(result.returncode, 0)
        return result

    def test_metadata_survives_rename_and_invalid_candidate_keeps_old_release(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            silver = self.run_job("silver_clean.py", root, "valid")
            self.assertRegex(silver.stdout, r"silver events\s+saved:\s+1")
            self.run_job("gold_build.py", root, "valid")
            pointer = root / "releases/current.json"
            first = pointer.read_text()
            self.assertEqual(json.loads(first)["run_id"], "valid")
            # A Gold-only validation failure must not publish or overwrite a release.
            metadata_file = root / "bronze/media/load_date=2026-10-01/8hunphufxp.json"
            metadata = json.loads(metadata_file.read_text())
            metadata["duration"] = -1.0
            metadata_file.write_text(json.dumps(metadata))
            self.run_job("silver_clean.py", root, "invalid_duration")
            failed_gold = self.run_job("gold_build.py", root, "invalid_duration", success=False)
            self.assertIn("Missing or invalid video duration", failed_gold.stderr)
            self.assertEqual(pointer.read_text(), first)
            self.assertFalse((root / "gold_versions/invalid_duration").exists())
            # An attribution conflict is rejected; an existing release stays intact.
            bad_file = root / "bronze/daily_stats/load_date=2026-10-01/renamed_0.json"
            envelope = json.loads(bad_file.read_text())
            envelope["data"][0]["media_id"] = "9k4tbcdfg0"
            bad_file.write_text(json.dumps(envelope))
            failed = self.run_job("silver_clean.py", root, "invalid", success=False)
            self.assertIn("conflicts with ingestion metadata", failed.stderr)
            self.assertEqual(pointer.read_text(), first)


if __name__ == "__main__":
    unittest.main()
