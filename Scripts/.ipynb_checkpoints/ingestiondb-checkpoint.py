import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import logging
import time

engine=create_engine('sqlite:///inventory.db')
logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)
def ingest_db(df,table_name,engine):
    df.to_sql(table_name,con=engine,if_exists='replace',index=False)

def load_raw_data():
    start=time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df=pd.read_csv('data/'+file)
            logging.info(f"Ingesting {file}")
            ingest_db(df,file[:-4],engine)
    end=time.time()
    totaltime=(end-start)/60
    logging.info("------------Ingestion complete----------")
    logging.info(f"\n Totsl Time Taken: {totaltime} minutes")

if __name__=="__main__":
    load_raw_data()
