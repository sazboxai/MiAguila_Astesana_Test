import os

from dotenv import load_dotenv

from AWS import AWS
from ProcessCSV import read_index, read_data

load_dotenv()


def wrapper(S3_subdir, file_name, local_subdir):
    # In case not exists, Download csv file form S3 to Local (/data)
    aws = AWS(os.getenv('ACCESS_KEY_ID'), os.getenv('SECRET_ACCESS_KEY'))
    aws.csv_from_s3(os.getenv('BUCKET'), S3_subdir, file_name, local_subdir)

    # Reading file and index; index represent the last row processed
    df_file = read_data(local_subdir, file_name)
    index = read_index(local_subdir, file_name)
    print(f"At moment {index}/{len(df_file)} rows has been processed ")

    

if __name__ == "__main__":
    wrapper("csv-not-processed/", "test.csv", 'data/')
else:
    print("File one executed when imported")
