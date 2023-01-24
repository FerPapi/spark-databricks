import datetime
import os
from copy import deepcopy

from propdesk_estimators.exchange_storage import ExchangeStorage
from propdesk_estimators.utils import get_dates_dataframe

def upload_dataframe_daily(dataframe, exchange_str, dataset_type, pair_str, base_tags_dict):
    if not 'datetime' in dataframe.columns:
        raise Exception('There is no column named datetime to upload to this container')

    for d in get_dates_dataframe(str(min(dataframe['datetime'])), str(max(dataframe['datetime'])), '1D', inclusive=True)['datetime']:
            year = str(d.year).zfill(2)
            month = str(d.month).zfill(2)
            day = str(d.day).zfill(2)

            day_ref_str = f'{year}-{month}-{day}'

            daily_slice_df = dataframe[dataframe['datetime'].between(d, d + datetime.timedelta(days=1), inclusive='left')]
            print(daily_slice_df)

            daily_slice_parquet_data = daily_slice_df.to_parquet()

            tags_dict = deepcopy(base_tags_dict)
            tags_dict.update({'date_ref': day_ref_str})

            dest_path = f'{pair_str}/{dataset_type}/{year}/{month}/{pair_str}_{dataset_type}_{day_ref_str}'
            print(tags_dict)
            print('dest_path', dest_path)
            exchange_storage = ExchangeStorage(exchange_str, auto_create=True)
            exchange_storage.upload_dataset(daily_slice_parquet_data, dest_path, tags_dict)
            print('done')
            print('====xxxx=====')