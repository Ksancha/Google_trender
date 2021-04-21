from google_trender.google_trender import USTrendRunner
import pandas as pd
from moto import mock_s3
import boto3
from unittest import TestCase

@mock_s3
class TestUSTrendRunner(TestCase):

    def set_Up(self):
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="my-test-bucket")

    def test_trend_runner_init(self):
        runner = USTrendRunner(bucket="test_bucket", now_ts=pd.Timestamp("10-05-2020 01:00:00"), search_terms="blah")

        self.assertEqual(runner.bucket, "test_bucket")
        self.assertEqual(runner.now, pd.Timestamp("10-05-2020 01:00:00"))
        self.assertListEqual(runner.search_terms, ["blah"])

    def test_make_time_prefix(self):
        runner = USTrendRunner(bucket="my-test-bucket", search_terms=["boo"], now_ts=pd.Timestamp("10-05-2020 01:00:00"))
        self.assertEqual("2020/10/5/1", runner._make_time_prefix())