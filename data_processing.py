def filter_data(batch):
    """
    Фильтрует данные: оставляет только объявления с ценой >= 50,000
    и площадью >= 20 м².

    """
    return batch[(batch['Price'] >= 50000) & (batch['LivingArea'] >= 20)]

def aggregate_data(batch):
    """
    Агрегирует данные по типу недвижимости и району.

    """
    grouped = batch.groupby(['Type', 'District']).agg(
        avg_price=('Price', 'mean'),
        avg_area=('LivingArea', 'mean'),
        total_listings=('Price', 'count'),
        avg_price_per_m2=('Price', lambda x: (x / batch.loc[x.index, 'LivingArea']).mean())
    ).reset_index()
    return grouped
