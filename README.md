# Google_trender

Google Trender is a runner class that gets Subregion and History over time data from Google Trends


Clone the repo and and setup a python v env

```pip install requirements.txt```

See example.py on how to operate with the runner.


## Notes / Possible improvements

1) The code can be wrapped up in a flask application to allow HTTP(S) requests

2) Test S3 logic with moto library

3) Set up Github actions for automated testing

4) To upload to S3 AWS credentials must already be set up

5) Save data as parquet files rather than CSVs to improve query speed

6) Data on S3 is partitioned by year/month/day/hour/search_term


