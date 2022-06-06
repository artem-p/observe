from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import time


CONNECTION_DETAILS = "postgresql://public_readonly:nearprotocol@35.184.214.98/testnet_explorer"
HOST = "35.184.214.98"
DATABASE = "testnet_explorer"
USER = "public_readonly"
PASSWORD = "nearprotocol"

def create_pandas_table(sql_query, connection):
    table = pd.read_sql(sql_query, engine)
    return table


engine = create_engine(CONNECTION_DETAILS)
connection = engine.connect()

now = time.time_ns()

hour_ago = now - 3600 * 1000 * 1000 * 1000

print(hour_ago)

query = f'''
        select 
            date_trunc('minute', to_timestamp(block_timestamp/1000/1000/1000)) as time,
            signer_account_id as signer,
            receiver_account_id as receiver
        from 
            transactions t
        where
            block_timestamp > {hour_ago}
        limit 100
        '''


records = create_pandas_table(query, connection)

print(records)

connection.invalidate()
engine.dispose()