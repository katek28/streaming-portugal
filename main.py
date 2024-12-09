import pandas as pd
import time
import subprocess
import threading
from pathlib import Path
from data_stream import data_stream
from data_processing import filter_data, aggregate_data
from utils import save_to_csv

# Базовый путь
DATA_PATH = 'D:/streaming-portugal/data/portugal_listings.csv'
OUTPUT_PATH = 'D:/streaming-portugal/data/aggregated_data.csv'

# Загрузка данных
df = pd.read_csv(DATA_PATH, low_memory=False)

# Преобразование колонок в числовой формат
df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
df['LivingArea'] = pd.to_numeric(df['LivingArea'], errors='coerce')

# Удаление строки с пропущенными значениями
df = df.dropna(subset=['Price', 'LivingArea'])

# Обработка потока данных
def process_stream():
    aggregated_data = pd.DataFrame()

    for chunk in data_stream(df, chunk_size=10, delay=1): 
        filtered_chunk = filter_data(chunk)
        aggregated_chunk = aggregate_data(filtered_chunk)
        aggregated_data = pd.concat([aggregated_data, aggregated_chunk])

        # Вывод промежуточного результата
        print(aggregated_chunk)

        # Сохранение результата в CSV
        save_to_csv(aggregated_data, OUTPUT_PATH)

        time.sleep(30)  

# Функция для запуска визуализации Streamlit в отдельном процессе
def run_visualization():
    subprocess.run(["streamlit", "run", "visualization.py"])

# Запуск
if __name__ == "__main__":
    
    visual_thread = threading.Thread(target=run_visualization)
    visual_thread.daemon = True  
    visual_thread.start()

    # Запуск обработки данных
    process_stream()