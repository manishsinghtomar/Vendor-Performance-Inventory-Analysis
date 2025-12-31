import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode="a" # a stands for append
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''This function will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine, if_exists= 'replace', index=False) # if_exists='append' in case of continous data in company, index=False because we dont want to store data

def load_raw_data():
    '''this function will load the CSVs as dataframe and ingest into db''' #doc string - tells what function does
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingesting {file} in db') #string formatting - 
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60 # after substract it returns answer in secs thats why we divided by 60
    logging.info('----------Ingestion Complete--------')
    logging.info(f'\nTotal Time Taken: {total_time} minutes')

if __name__=='__main__':   
    load_raw_data()