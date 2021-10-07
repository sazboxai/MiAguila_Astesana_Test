import os
from dotenv import load_dotenv
import unittest

from AWS import AWS
from ProcessCSV import valid_coordinate, read_index

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

    # ProcessCSV
    def test_valid_coordinate_correct_values(self):
        response = valid_coordinate("0.629834723775309", "51.7923246977375")
        self.assertEqual(response, True)

    def test_valid_coordinate_incorrect_values_1(self):
        response = valid_coordinate("0.629834723775309", 0)
        self.assertEqual(response, False)

    def test_valid_coordinate_incorrect_values_2(self):
        response = valid_coordinate("0.629834723775309", "word")
        self.assertEqual(response, False)

    def test_read_index(self):
        response = read_index('data/', 'test.csv')
        self.assertEqual((isinstance(response, int)), True)


if __name__ == '__main__':
    unittest.main()
