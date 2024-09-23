# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
import os
import logging
import pickle

# %%
logging.basicConfig(level=logging.INFO)

def load_grammy():
    try:
        load_dotenv()

        localhost = os.getenv('LOCALHOST')
        port = os.getenv('PORT')
        nameDB = os.getenv('DB_NAME')
        userDB = os.getenv('DB_USER')
        passDB = os.getenv('DB_PASS')
        
        engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')
        inspector = inspect(engine)
        
        connection = engine.connect()
        logging.info("Successfully connected to the database.")
        
        dataframe = 'grammy_awards'  
        df_grammy = pd.read_sql_table(dataframe, engine)
        
        logging.info("Successfully loaded the data.")
        logging.info(df_grammy.head(5))
        
        connection.close()
        
        return df_grammy
    
    
    except Exception as e:
        logging.error(f"Error loading the data: {str(e)}")
        
    
data = load_grammy()

# %%
def check_grammy(df):
    try:
        logging.info("Starting the ckeck process.")
        null_counts = df.isnull().sum()
        logging.info(f"The total of values null in the dataframe is: \n{null_counts} ")
        num_duplicates = df.duplicated().sum()
        logging.info(f"The total of duplicates in the dataframe is: {num_duplicates}")
        logging.info(f"Cheacking type of data: \n{df.info()}")
        logging.info("The dataframe is ready to merge.")
        grammy_ready = df
        
        with open('grammy_ready_df.pkl', 'wb') as f:
            pickle.dump(grammy_ready, f)
        logging.info("The dataframe has been saved in 'grammy_ready_df.pkl'.")
        
        return grammy_ready
        
    except Exception as e:
        logging.error(f"Error checking the data: {str(e)}")
        
check_grammy(data)


