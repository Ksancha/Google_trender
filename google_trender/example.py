from google_trender.google_trender import USTrendRunner


def get_sample_data():
    search_terms = ["Elon Musk", "Joe Biden", "Taylor Swift", "Black Panther"]
    runner = USTrendRunner(search_terms=search_terms)
    runner.run()

    return runner.data


def get_sample_data_and_upload_to_s3():
    search_terms = ["Elon Musk", "Joe Biden", "Taylor Swift", "Black Panther"]
    bucket = "my-bucket-on-s3"
    runner = USTrendRunner(search_terms=search_terms, bucket=bucket)
    runner.run()
