import pandas as pd

def save_to_csv(df, file_path):
    df.to_csv(file_path, index=False)