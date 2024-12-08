import pandas as pd

def save_to_csv(dataframe, path):
    """
    Сохраняет DataFrame в CSV файл.
    """
    dataframe.to_csv(path, index=False)
    print(f"Data saved to {path}")