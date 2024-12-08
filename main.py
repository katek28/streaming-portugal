from pathlib import Path
import pandas as pd
from data_stream import data_stream
from data_processing import filter_data, aggregate_data
from utils import save_to_csv
import time

# Устанавливаем базовый путь
BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / 'data' / 'portugal_listings.csv'
OUTPUT_PATH = BASE_DIR / 'data' / 'aggregated_results.csv'

# Загрузка данных
df = pd.read_csv(DATA_PATH, low_memory=False)

# Преобразование колонок в числовой формат
df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
df['LivingArea'] = pd.to_numeric(df['LivingArea'], errors='coerce')

# Удаляем строки с пропущенными значениями
df = df.dropna(subset=['Price', 'LivingArea'])

# Основной цикл обработки потока данных
def process_stream():
    aggregated_data = pd.DataFrame()

    for batch in data_stream(df, batch_size=10, delay=1):  # 10 записей в секунду
        filtered_batch = filter_data(batch)
        aggregated_batch = aggregate_data(filtered_batch)
        aggregated_data = pd.concat([aggregated_data, aggregated_batch])

        # Вывод промежуточного результата
        print(aggregated_batch)

        # Сохраняем результат в CSV
        save_to_csv(aggregated_data, OUTPUT_PATH)

        time.sleep(30)  # Обновление каждые 30 секунд

# Запуск
if __name__ == "__main__":
    process_stream()