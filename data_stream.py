import pandas as pd
import time
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / 'data' / 'portugal_listings.csv'  

def data_stream(file_path=DATA_PATH, delay=1, chunk_size=10):
    
    df = pd.read_csv(file_path)
    
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        yield chunk
        time.sleep(delay)  