import os
from dotenv import load_dotenv
import unittest

from AWS import AWS

load_dotenv()


class MyTestCase(unittest.TestCase):
    # AWS
    def test_csv_from_s3_existing_file(self):
        aws = AWS(os.getenv('ACCESS_KEY_ID'), os.getenv('SECRET_ACCESS_KEY'))
        subdir = "csv-not-processed/"
        file_name = "test.csv"
        aws.csv_from_s3(os.getenv('BUCKET'), subdir, file_name)
        local_folder = 'data/'
        self.assertEqual(os.path.exists(local_folder + file_name), True)

    def test_csv_from_s3_not_existing_file(self):
        aws = AWS(os.getenv('ACCESS_KEY_ID'), os.getenv('SECRET_ACCESS_KEY'))
        subdir = "csv-not-processed/"
        file_name = "nonExists.csv"
        aws.csv_from_s3(os.getenv('BUCKET'), subdir, file_name)
        local_folder = 'data/'
        self.assertEqual(os.path.exists(local_folder + file_name), False)


if __name__ == '__main__':
    unittest.main()
