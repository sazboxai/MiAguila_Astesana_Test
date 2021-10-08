import os

from dotenv import load_dotenv

from API import api_get_batchs
from AWS import AWS
from ProcessCSV import read_index, read_data, invalids_coordinates, modify_index

load_dotenv()


def wrapper(S3_subdir, file_name, local_subdir, batch_size, queries_per_sec):
    # In case not exists, Download csv file form S3 to Local (data/)
    aws = AWS(os.getenv('ACCESS_KEY_ID'), os.getenv('SECRET_ACCESS_KEY'))
    aws.csv_from_s3(os.getenv('BUCKET'), S3_subdir, file_name)

    # Reading file and index; index represent the last row processed
    df_file = read_data(local_subdir, file_name)
    index = read_index(local_subdir, file_name)
    print(f"At moment {index}/{len(df_file)} rows has been processed ")
    if batch_size > len(df_file) - index: batch_size = len(df_file) - index

    # Validating Coordinates and query Api by Batch
    while index < len(df_file):
        if batch_size > len(df_file) - index: batch_size = len(df_file) - index
        batch = df_file[index:index + batch_size]
        df_valid, df_invalids = invalids_coordinates(batch)
        df_valid_with_pc = api_get_batchs(df_valid, queries_per_sec)

        # insert valid_values into DB
        # inser invalid_values into CSV_file
        # update txt_index
        index += batch_size
        modify_index(local_subdir, file_name, index)

        # Metrics
        print("processed rows: " + str(index) + '/' + str(len(df_file)) + " ---Total[%]: " + str(
            index / len(df_file) * 100) + '%')
        print("Remaining rows XX" + "Remaining time to completely process file: 00:00")
        index += batch_size


if __name__ == "__main__":
    wrapper("csv-not-processed/", "test.csv", 'data/', 100, 35)
else:
    print("File one executed when imported")
