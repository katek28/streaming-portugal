import pandas as pd

def filter_data(df):
    """
    Фильтрует данные: удаляет записи с ценой < 50000 и площадью < 20 м².

    """
    return df[(df['Price'] >= 50000) & (df['LivingArea'] >= 20)]

def aggregate_data(df):
    """
    Агрегирует данные по типу и району недвижимости.

    """
    grouped = df.groupby(['Type', 'District']).agg(
        avg_price=('Price', 'mean'),
        avg_area=('LivingArea', 'mean'),
        total_listings=('Price', 'count'),
        avg_price_per_m2=('Price', lambda x: x.mean() / df['LivingArea'].mean())
    ).reset_index()
    return grouped