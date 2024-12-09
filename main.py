from pathlib import Path
import pandas as pd
from data_stream import data_stream
from data_processing import filter_data, aggregate_data
from utils import save_to_csv
import time

# Базовый путь
#BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = 'D:/streaming-portugal/data/portugal_listings.csv'
OUTPUT_PATH = 'D:/streaming-portugal/data/aggregated_data.csv'

# Загрузка данных
if not DATA_PATH.exists():
    print(f"Ошибка: файл {DATA_PATH} не найден.")
else:
    df = pd.read_csv(DATA_PATH, low_memory=False)

# Преобразование колонок в числовой формат
df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
df['LivingArea'] = pd.to_numeric(df['LivingArea'], errors='coerce')

# Удаление строки с пропущенными значениями
df = df.dropna(subset=['Price', 'LivingArea'])

# Обработка потока данных
def process_stream():
    aggregated_data = pd.DataFrame()

    for chunk in data_stream(df, chunk_size=10, delay=1):  # 10 записей в секунду
        filtered_chunk = filter_data(chunk)
        aggregated_chunk = aggregate_data(filtered_chunk)
        aggregated_data = pd.concat([aggregated_data, aggregated_chunk])

        # Вывод промежуточного результата
        print(aggregated_chunk)

        # Сохранение результата в CSV
        save_to_csv(aggregated_data, OUTPUT_PATH)

        time.sleep(30)  # Обновление каждые 30 секунд

# Запуск
if __name__ == "__main__":
    process_stream()