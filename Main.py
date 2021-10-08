import os
import time

from dotenv import load_dotenv

from API import api_get_batchs
from AWS import AWS
from DB import DB
from ProcessCSV import read_index, read_data, invalids_coordinates, modify_index, save_invalid_values, convert_seconds
import pandas as pd

load_dotenv()


def wrapper(s3_subdir, file_name, local_subdir, batch_size, queries_per_sec):
    # In case not exists, Download csv file form S3 to Local (data/)
    aws = AWS(os.getenv('ACCESS_KEY_ID'), os.getenv('SECRET_ACCESS_KEY'))
    aws.csv_from_s3(os.getenv('BUCKET'), s3_subdir, file_name)

    # Reading file and index; index represent the last row processed
    df_file = read_data(local_subdir, file_name)
    index = read_index(local_subdir, file_name)
    print(f"At moment {index}/{len(df_file)} rows has been processed ")
    if batch_size > len(df_file) - index: batch_size = len(df_file) - index

    # Validating Coordinates and query Api by Batch
    while index < len(df_file):
        print("########## Procesing new batch ##########")
        start_time = time.time()
        if batch_size > len(df_file) - index: batch_size = len(df_file) - index
        batch = df_file[index:index + batch_size]
        df_valid, df_invalids = invalids_coordinates(batch)
        df_valid_with_pc, df_invalids_aux = api_get_batchs(df_valid, queries_per_sec)
        df_invalids = pd.concat([df_invalids, df_invalids_aux])

        # insert valid_values into DB
        db = DB(os.getenv('DB_USER'), os.getenv('DB_PASS'), os.getenv('DB_PORT'), os.getenv('DB_NAME'))
        db.insert_db(df_valid_with_pc, "post_codes")
        db.update_transaction_log(file_name, index, index + batch_size, "transaction_log", len(df_valid_with_pc),
                                  len(df_invalids))
        # insert invalid_values into CSV_file
        save_invalid_values(df_invalids, local_subdir, file_name)
        # update txt_index
        index += batch_size
        modify_index(local_subdir, file_name, index)

        # Metrics
        print(f"Batch with rows {str(index - batch_size)} to {str(index)} processed in {time.time() - start_time} sec")
        print("Total processed rows: " + str(index) + '/' + str(len(df_file)) + " ---Total[%]: " + str(
            index / len(df_file) * 100) + '%')
        [hs, m, s] = convert_seconds((time.time() - start_time) * (len(df_file) - index) / batch_size)
        print(f"Remaining time to completely process file: {hs}hs:{m}min:{s}sec")


if __name__ == "__main__":

    # wrapper("csv-not-processed/", "postcodesgeo.csv", 'data/', 100, 50)
    wrapper("csv-not-processed/", "test.csv", 'data/', 100, 50)

else:
    print("Cannot start program")
