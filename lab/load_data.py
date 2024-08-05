import os
import polars
import pymongo
import data

def load_row_to_mongo(row: dict, uri: str, table_name: str, db: str = 'test_db') -> None:
    client = pymongo.MongoClient(uri)
    db = client[db]
    table = db[table_name]
    table.insert_one(row)


def load_df_to_mongo(df: polars.DataFrame,
                     uri: str,
                     table_name: str,
                     db: str = 'test_db') -> None:
    client = pymongo.MongoClient(uri)
    db = client[db]
    table = db[table_name]

    batch_size = 10_000
    buffer = []
    for row in df.iter_rows(named=True):
        buffer.append(row)

        if len(buffer) == batch_size:
            table.insert_many(buffer)
            buffer.clear()


df = polars.read_parquet('./data/taxi/')
MONGO_DB_URI = os.getenv('MONGO_ATLAS_URI')

load_row_to_mongo(data.arrays, MONGO_DB_URI, 'simple_array')
# load_df_to_mongo(df, MONGO_DB_URI, table_name='taxi')
