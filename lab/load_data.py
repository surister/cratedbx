import json
import os
import pprint

import polars
import pymongo
import psycopg2
import mysql.connector
import data


def load_row_to_mongo(row: dict,
                      uri: str,
                      table_name: str,
                      db: str = 'testdb',
                      how_many: int = 1) -> None:
    client = pymongo.MongoClient(uri)
    db = client[db]
    table = db[table_name]

    for _ in range(how_many):
        table.insert_one(row.copy())


def load_row_to_mysql(row: dict,
                      uri: str,
                      table_name: str,
                      db: str = 'testdb',
                      how_many: int = 1) -> None:
    columns = ",".join(row.keys())
    values_interpolation = ', '.join(['%s'] * len(row))
    query = f'INSERT INTO {table_name} ({columns}) VALUES ({values_interpolation})'

    values = list(row.values())
    for i in values:
        if isinstance(i, dict):
            index = values.index(i)
            values[index] = json.dumps(i)
        if isinstance(i, list):
            index = values.index(i)
            values[index] = json.dumps(i)

    conn = mysql.connector.connect(user='root', password='mysql', host='localhost',
                                   database='mysql')
    curr = conn.cursor()
    for _ in range(how_many):
        curr.execute(query, values)

    conn.commit()


def load_row_to_postgres(row: dict,
                         uri: str,
                         table_name: str,
                         db: str = 'testdb',
                         how_many: int = 1) -> None:
    columns = ",".join(row.keys())
    values_interpolation = ', '.join(['%s'] * len(row))

    conn = psycopg2.connect(uri)
    curr = conn.cursor()
    query = f'INSERT INTO {table_name} ({columns}) VALUES ({values_interpolation})'

    values = list(row.values())
    for i in values:
        if isinstance(i, dict):
            index = values.index(i)
            values[index] = json.dumps(i)

    curr.executemany(query, ((tuple(values),) * how_many))
    conn.commit()
    print('ok')


def load_df_to_mongo(df: polars.DataFrame,
                     uri: str,
                     table_name: str,
                     db: str = 'test_db') -> None:
    client = pymongo.MongoClient(uri)
    db = client[db]
    table = db[table_name]

    batch_size = 10_00
    buffer = []
    for row in df.iter_rows(named=True):
        buffer.append(row)

        if len(buffer) == batch_size:
            table.insert_many(buffer)
            buffer.clear()


df = polars.read_parquet('./data/taxi/')
MONGO_DB_URI = os.getenv('MONGO_ATLAS_URI')
POSTGRES_DB_URI = os.getenv('POSTGRES_URI')
MYSQL_DB_URI = os.getenv('MYSQL_URI', 'mysql://mysql:mysql@localhost:3306/mysql')

# load_row_to_mongo(data.array, 'mongodb://localhost', 'simple_array', how_many=10_0000)
# load_df_to_mongo(df, MONGO_DB_URI, table_name='taxi')
# load_row_to_postgres(data.array, POSTGRES_DB_URI, 'simple_array', how_many=1)
load_row_to_mysql(data.array, MYSQL_DB_URI, 'simple_array', how_many=10_000)
