import psycopg2
import pandas as pd
from sqlalchemy import create_engine



CONNECTION_DETAILS = "postgresql://public_readonly:nearprotocol@35.184.214.98/testnet_explorer"
HOST = "35.184.214.98"
DATABASE = "testnet_explorer"
USER = "public_readonly"
PASSWORD = "nearprotocol"

def create_pandas_table(sql_query, connection):
    table = pd.read_sql(sql_query, engine)
    return table


connection = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
engine = create_engine(CONNECTION_DETAILS)

cursor = connection.cursor()


query = '''
        select 
            date_trunc('minute', to_timestamp(block_timestamp/1000/1000/1000)) as time,
            signer_account_id as signer,
            receiver_account_id as receiver
        from 
            transactions t
        where
            receiver_account_id = 'guest-book.testnet'
        '''


records = create_pandas_table(query, connection)

print(records)

connection.close()