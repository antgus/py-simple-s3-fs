from src.fs import S3ObjectStore
from test.unit_tests import ObjectStoreUnitTests
import argparse

if __name__ == "__main__":
    raise AssertionError("TODO - This is still WIP")
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--bucket", type=str, required=True, help="the s3 bucket where testing will be performed")
    args = parser.parse_args()
    bucket_name = args.bucket
    # execute only if run as a script
    tests = ObjectStoreUnitTests()
    store = S3ObjectStore()
    tests.run_tests(store, "s3://"+bucket_name + "/")
