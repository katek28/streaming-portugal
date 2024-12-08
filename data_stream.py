import pandas as pd
import time

def stream_data(file_path, delay=1, chunk_size=10):
   
    df = pd.read_csv(file_path)
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        yield chunk
        time.sleep(delay)