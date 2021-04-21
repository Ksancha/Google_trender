from pytrends.request import TrendReq
import boto3
import pandas as pd
import io


class USTrendRunner:
    """Class for getting US Google trends"""

    def __init__(self, search_terms, bucket=None, now_ts=None):
        self.search_terms = search_terms
        if not isinstance(search_terms, list):
            self.search_terms = [self.search_terms]
        self.pytrends = TrendReq(hl='US')
        self.data = {}

        self.bucket = bucket
        self.now = now_ts or pd.Timestamp.now()

    def run(self):
        """Get data from google trends and upload to S3"""
        self.get_data()
        if self.bucket:
            self.send_data_to_s3()

    def get_data(self):
        """Populate data dict with different data"""
        self.data["subregion"] = self._get_interest_by_region()
        self.data["interest over time"] = self._get_last_hour_interest()

    def _get_interest_by_region(self):
        """Get interest by region DF"""
        self.pytrends.build_payload(kw_list=self.search_terms, timeframe=f'now 1-H', geo='US')
        return self.pytrends.interest_by_region(resolution="REGION")

    def _get_last_hour_interest(self):
        """Get interest timeseries DF"""
        # Hourly data returned by Google has only 59 entries.
        # If we run the pipeline hourly we will loose 1 point every time
        # Instead we'll ask for 4 hours and last 60 entries
        # If we build 4 hour payload in the first place, Subregion data will be different
        self.pytrends.build_payload(kw_list=self.search_terms, timeframe="now 4-H", geo='US')
        df_interest_over_time = self.pytrends.interest_over_time()
        return df_interest_over_time.drop(columns="isPartial").iloc[:-60, :]

    def send_data_to_s3(self):
        """Send data to S3"""
        self.send_subregion_data_to_s3()
        self.send_interest_data_to_s3()
        pass

    def send_subregion_data_to_s3(self):
        """Wrapper around sending subregion data to S3"""
        prefix = f"Data/Subregion/{self.now.year}/{self.now.month}/{self.now.day}/{self.now.hour}/"
        for column in self.data["subregion"].columns:
            dataframe_to_s3(df=self.data["subregion"][[column]].transpose(), bucket=self.bucket, key=prefix + column, index=False)

    def send_interest_data_to_s3(self):
        """Wrapper around sending interest over time data to S3"""
        prefix = f"Data/Interest Over Time/{self._make_time_prefix()}/"
        for column in self.data["interest over time"].columns:
            dataframe_to_s3(df=self.data["subregion"][[column]].transpose(), bucket=self.bucket, key=prefix + column, index=False)

    def _make_time_prefix(self):
        """Make TS partition prefix"""
        return f"{self.now.year}/{self.now.month}/{self.now.day}/{self.now.hour}"


def dataframe_to_s3(df, bucket, key, **args):
    """Upload a DF to S3"""
    client = boto3.client("s3")
    file = io.BytesIO(df.to_csv(**args).encode("utf8"))
    resp = client.put_object(Bucket=bucket, Key=key, Body=file)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
