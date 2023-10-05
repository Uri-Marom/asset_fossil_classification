import gspread
import pandas as pd
from sqlalchemy import create_engine

# connect to G-Sheets
# JSON in ~/.config/gspread/service_account.json
my_gc = gspread.service_account()
# connect to db
my_engine = create_engine("postgresql://enva:enva1234@enva.cz4dlrm0xto9.us-east-1.rds.amazonaws.com:5432/enva")


def gspread_to_df(gs_key, gc=my_gc, sheet_num=0):
    ws = gc.open_by_key(gs_key).get_worksheet(sheet_num)
    return pd.DataFrame(ws.get_all_records())


def df_to_db_table(df, table_name, engine=my_engine, if_exists='replace', chunksize=500):
    df.to_sql(name=table_name, con=engine, schema=None, if_exists=if_exists, index=False, chunksize=chunksize)


def gspread_to_db_table(gs_key, table_name, sheet_num=0, gc=my_gc, engine=my_engine, if_exists='replace', chunksize=500):
    df = gspread_to_df(gs_key, gc, sheet_num)
    df_to_db_table(df, table_name, engine, if_exists, chunksize)
    print('uploaded worksheet to db, table name: {}'.format(table_name))