import os
from sqlalchemy import create_engine


class DB:
    def __init__(self, db_user, db_pass, db_name):
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name

    def insert_db(self, df, table_name):
        db_string = f"postgresql://{os.environ[self.db_user]}:{os.environ[self.db_pass]}@localhost:5432/{os.environ[self.db_name]}"
        engine = create_engine(db_string)
        df.to_sql(table_name, engine, if_exists='append')
