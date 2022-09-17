import csv
import logging
# s3
# import boto3
# import joblib
import tempfile
import time
import traceback
from io import StringIO

import pandas as pd
import psycopg2
import psycopg2.extras
import sqlalchemy
from sqlalchemy.orm import Session

# email
# from boto3 import client

logger = logging.getLogger('production-estimation.cloud')

class PostgresRDS(object):
    """
    Class Connects to a PostgreSQL DB with password access
    Need to input the database that needs to be connected to
    Note Set the username, password and endpoint in the config file via env variables
    """

    def __init__(self, dbname, user, password, host, port=5432, verbose=0):
        self.engine = None
        self.Session = None
        self.db = dbname
        self.username = user
        self.pwd = password
        self.endpoint = host
        self.port = port
        self.vprint = print if verbose != 0 else lambda *a, **k: None

    def connect(self):
        """
        Connects to the database and gives us the engine
        :return: engine
        """
        engine_config = {
            'sqlalchemy.url': 'postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}'.format(
                user=self.username,
                pw=self.pwd,
                host=self.endpoint,
                port=self.port,
                db=self.db
            ),
            'sqlalchemy.pool_pre_ping': True,
            'sqlalchemy.pool_recycle': 3600
        }

        engine = sqlalchemy.engine_from_config(engine_config, prefix='sqlalchemy.')
        self.Session = Session(engine)

        return engine

    def __enter__(self):
        self.engine = self.connect()
        self.vprint("Connected to {} DataBase".format(self.db))
        return self.engine

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Session.close() #type: ignore
        self.engine.dispose() #type: ignore
        self.vprint("Connection Closed")


def pandas_to_postgres(df: pd.DataFrame, conn_dict: dict, table_name: str, unique_keys=None, on_update=False, verbose=0):
    """
    Add data from a pandas dataframe onto a postgres table.
    The Table should be present, and contain the columns that the dataframe has.

    :param df: A pandas DataFrame
    :param conn_dict: The connection str to connect to the postgres database.
                      Should have the keys like: dbname, user, password, host, port
    :param table_name: The name of the table in the database. Include the schema name if necessary.
                       Should be lower case.
    :param unique_keys: List of Unique Keys that will be violated if duplicated.
                        Default: None. Assumes no keys will be violated.
    :param on_update: Default: False, Skips the rows which violate the duplicate key condition.
                      If True, Will update the rows with the new values.
                      Will only come into play if unique keys are provided.
    :param verbose: Print out progress and additional info.
    """
    vprint = print if verbose != 0 else lambda *args, **kwargs: None
    t0 = time.time()

    # Setup the parameters for the query
    cols_str = ', '.join('"{}"'.format(k) for k in df.columns)  # get the column name str
    data = [tuple(x) for x in df.to_numpy()]

    # generating Queries for different Use cases
    if unique_keys is None:
        vprint(f"Adding data to the table without checking for key violations.")
        # the base condition
        query = """
        INSERT INTO {table} ({columns})
        VALUES %s
        """.format(table=table_name, columns=cols_str)
    else:
        unique_index_str = ', '.join('"{}"'.format(i) for i in unique_keys)
        if on_update:
            # Update on unique key violation
            vprint("Adding data to the table and if unique_indexes are violated, those rows will be updated.")
            update_cols = [c for c in df.columns if c not in unique_keys]
            update_cols_str = ', '.join(f'"{t}" = excluded."{t}"' for t in update_cols)
            # Upsert Case where columns are updated
            query = """
            INSERT INTO {table} ({columns})
            VALUES %s
            ON CONFLICT ({unique_index})
            DO UPDATE SET {update_str};
            """.format(table=table_name, columns=cols_str, unique_index=unique_index_str, update_str=update_cols_str)

        else:
            # On unique key conflict don't update
            vprint("Adding data to the table and if unique_indexes are violated, those rows will be dropped.")
            query = """
            INSERT INTO {table} ({columns})
            VALUES %s
            ON CONFLICT ({unique_index})
            DO NOTHING;
            """.format(table=table_name, columns=cols_str, unique_index=unique_index_str)

    # Adding the data to the database
    try:
        conn = psycopg2.connect(**conn_dict)
        try:
            with conn:
                with conn.cursor() as curs:
                    psycopg2.extras.execute_values(cur=curs,
                                                   sql=query,
                                                   argslist=data)
            print(f'Data was added successfully.')
        except Exception as e:
            print(f"Data Update Failed Due to: \n{e}")
        finally:
            conn.close()
    except psycopg2.OperationalError as e:
        print(f"Database Connections Issue: {e}")
    except Exception:
        traceback.print_exc()

    t1 = time.time()
    vprint("Time taken for operation {:.2f}s".format(t1 - t0))

