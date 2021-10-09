import os
from dotenv import load_dotenv
import unittest
from sqlalchemy import create_engine

from API import async_queries
from AWS import AWS
from DB import DB
from ProcessCSV import valid_coordinate, read_index, read_data

load_dotenv()


class MyTestCase(unittest.TestCase):

    # AWS
    def test_csv_from_s3_not_existing_file(self):
        aws = AWS(os.getenv('ACCESS_KEY_ID'), os.getenv('SECRET_ACCESS_KEY'))
        subdir = "csv-not-processed/"
        file_name = "nonExists.csv"
        aws.csv_from_s3(os.getenv('BUCKET'), subdir, file_name)
        local_folder = 'data/'
        self.assertEqual(os.path.exists(local_folder + file_name), False)

    # ProcessCSV
    def test_read_index_not_exist(self):
        self.assertRaises(Exception, lambda: read_index('data/', "nonExists.txt"))

    def test_read_data_not_exist(self):
        self.assertRaises(Exception, lambda: read_data('data/', "nonExists.csv"))

    def test_valid_coordinate_correct_values(self):
        response = valid_coordinate("0.629834723775309", "51.7923246977375")
        self.assertEqual(response, True)

    def test_valid_coordinate_incorrect_values_1(self):
        response = valid_coordinate("0.629834723775309", 0)
        self.assertEqual(response, False)

    def test_valid_coordinate_incorrect_values_2(self):
        response = valid_coordinate("0.629834723775309", "word")
        self.assertEqual(response, False)

    # API
    def test_async_queries_invalid_urls(self):
        urls = ['https://invalid']
        response = async_queries(urls)
        self.assertEqual(response[0], None)

    # DB
    def test_db_connection(self):
        try:
            db = DB(os.getenv('DB_USER'), os.getenv('DB_PASS'), os.getenv('DB_PORT'), os.getenv('DB_NAME'))
            db_string = f"postgresql://{db.db_user}:{db.db_pass}@localhost:{db.port}/{db.db_name}"
            connection = create_engine(db_string)
            self.assertEqual(True, True)
        except:
            self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
