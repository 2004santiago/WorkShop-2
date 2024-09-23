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

def load_spotify():
    try:
        df = pd.read_csv('../Clean-Data/spotify-clean.csv')
        logging.info(df.head(5))
        logging.info("Spotify data loaded")
        return df
        
    except Exception as e:
        logging.error(f"Error loading Spotify data: {e}")
        
        
data = load_spotify()

# %%
def check_spotify(df):
    try:
        logging.info("Starting the ckeck process.")
        null_counts = df.isnull().sum()
        logging.info(f"The total of values null in the dataframe is: \n{null_counts} ")
        num_duplicates = df.duplicated().sum()
        logging.info(f"The total of duplicates in the dataframe is: {num_duplicates}")
        logging.info(f"Cheacking type of data: \n{df.info()}")
        logging.info("The dataframe is ready to merge.")
        spotify_ready = df
        
        with open('spotify_ready_df.pkl', 'wb') as f:
            pickle.dump(spotify_ready, f)
        logging.info("The dataframe has been saved in 'spotify_ready_df.pkl'.")
        
        return spotify_ready
    
    except Exception as e:
        logging.error(f"Error checking Spotify data: {e}")
    
check_spotify(data)


