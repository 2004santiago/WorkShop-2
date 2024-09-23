# %%
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import logging
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

def load_all_dataframes():
    try:
        grammy_df = pd.read_pickle('./grammy_ready_df.pkl')
        spotify_df = pd.read_pickle('./spotify_ready_df.pkl')
        
        logging.info(f"Grammy dataframe: \n{grammy_df.head(5)}")
        logging.info(f"Spotify dataframe: \n{spotify_df.head(5)}")
    
        
        logging.info("Dataframes loaded successfully")
        
        return grammy_df, spotify_df
        
        
    except Exception as e:
        logging.error(f"Error loading dataframes: {e}")

grammy, spotify = load_all_dataframes()

# %%
def merge(grammy_df, spotify_df):
    try:
        logging.info("Merging dataframes...")
        
        merged_df = pd.merge(grammy_df, spotify_df, left_on='artist', right_on='artists', how='inner')

        merged_df['artist'] = merged_df['artist'].combine_first(merged_df['artists'])
        
        if 'winner' in merged_df.columns:
            valores_unicos = merged_df['winner'].unique()
            logging.info(f"Unique values in 'winner' column: {valores_unicos}")

            conteo_registros = merged_df['winner'].value_counts()
            for valor, conteo in conteo_registros.items():
                logging.info(f"Total records with value '{valor}': {conteo}")

            merged_df = merged_df.drop(columns=['winner'])
            logging.info("Column 'winner' removed from the dataset.")

        merged_df.drop(columns=['artists','liveness','time_signature', 'loudness','mode'], inplace=True)

        logging.info("Merge completed successfully.")
        return merged_df

    except Exception as e:
        logging.error(f"Error during dataframe merge: {e}")


df_merge = merge(grammy, spotify)

# %%
def save_csv(mergedf):
    try:
        # num_duplicates = mergedf.duplicated().sum()
        # logging.info(f"The total of duplicates in the dataframe is: {num_duplicates}")
        mergedf.to_csv('../Clean-Data/merge_df.csv', index=False)
        logging.info("Dataframe saved as CSV successfully.")
        
    except Exception as e:
        logging.error(f"Error saving dataframe as CSV: {e}")
        
save_csv(df_merge)

# %%
def save_DB(df):
    try:
        load_dotenv()

        localhost = os.getenv('LOCALHOST')
        port = os.getenv('PORT')
        nameDB = os.getenv('DB_NAME')
        userDB = os.getenv('DB_USER')
        passDB = os.getenv('DB_PASS')

        engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')
        inspector = inspect(engine)

        with engine.connect() as connection:
            logging.info("Successfully connected to the database.")
            
            try:
                df.to_sql('data_merge', engine, if_exists='replace', index=False)
                logging.info("Table 'data_merge' added.")

            except Exception as e:
                logging.error(f"Error adding data: {e}")

    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        
    finally:
        engine.dispose()
        logging.info("Database connection closed.")
        
save_DB(df_merge)

# %%

def authenticate():
    gauth = GoogleAuth()

    gauth.LoadClientConfigFile("../client_secret.json")
    
    gauth.LocalWebserverAuth()
    
    return gauth

def save_drive(df, file_name='df_merge.csv', folder_id=None):
    try:
        temp_file_path = f"./{file_name}"
        df.to_csv(temp_file_path, index=False)
        logging.info("DataFrame saved as CSV successfully.")

        gauth = authenticate()
        drive = GoogleDrive(gauth)

        file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}] if folder_id else []})
        file.SetContentFile(temp_file_path)
        file.Upload()

        print(f"Archivo '{file_name}' subido correctamente a Google Drive.")

        os.remove(temp_file_path)
        
    except Exception as e:
        print(f"Error al subir el archivo a Google Drive: {e}")

save_drive(df_merge)


