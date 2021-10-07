import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

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
        df.to_sql(table_name, engine, if_exists='append')
