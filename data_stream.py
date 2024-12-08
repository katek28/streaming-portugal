import pandas as pd
import time

def data_stream(df, batch_size=10, delay=1):

    for start in range(0, len(df), batch_size):
        end = start + batch_size
        yield df.iloc[start:end]
        time.sleep(delay)