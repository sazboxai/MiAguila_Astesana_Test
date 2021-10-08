from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import datetime

load_dotenv()


class DB:
    def __init__(self, db_user, db_pass, port, db_name):
        self.db_user = db_user
        self.db_pass = db_pass
        self.port = port
        self.db_name = db_name

    def insert_db(self, df, table_name):
        db_string = f"postgresql://{self.db_user}:{self.db_pass}@localhost:{self.port}/{self.db_name}"
        engine = create_engine(db_string)
        df.to_sql(table_name, engine, if_exists='append', index=False)

    def update_transaction_log(self, file_name, idx_from, idx_to, table_name, n_val, n_inv):
        df_trans = pd.DataFrame([[file_name, idx_from, idx_to, n_val, n_inv, n_val + n_inv, datetime.datetime.now()]],
                                columns=['file_processed', 'idx_from', 'idx_to', 'valid', 'invalids', 'total',
                                         'insertion_date'])
        db_string = f"postgresql://{self.db_user}:{self.db_pass}@localhost:{self.port}/{self.db_name}"
        engine = create_engine(db_string)
        df_trans.to_sql(table_name, engine, if_exists='append', index=False)
