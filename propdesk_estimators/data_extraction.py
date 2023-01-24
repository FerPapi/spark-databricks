import tempfile
import datetime
import os
import re

import pandas as pd

from propdesk_services import tardis as tta
from propdesk_estimators.utils import get_date_in_many_formats
from propdesk_estimators.utils import get_dates_dataframe
from propdesk_estimators.utils import remove_dir

from propdesk_estimators.data_raw_transformation import drop_columns_df
from propdesk_estimators.data_raw_transformation import calc_mid_quote
from propdesk_estimators.data_raw_transformation import get_normalized_df


def download_exchange_raw_data(exchange, pair_str, date_from, date_to, type_of_data, tmp_folder=None):
    pair_str = pair_str.lower()
    datasets = tta.get_all_exchange_datasets(exchange)
    if pair_str not in datasets.keys():
        raise Exception(f'Did not find {pair_str} in datasets of exchange {exchange}')

    dataset_info = datasets[pair_str]
    if type_of_data not in dataset_info['dataTypes']:
        raise Exception(f'Did not find {type_of_data} in dataset {dataset_info}')

    if not tmp_folder:
        tmp_folder = tempfile.mkdtemp()
        print('downloading raw data to:', tmp_folder)

    # tardis API only returns integral data until the previous day, i.e,
    # requesting data until 2021-05-10T12:00:00 will only get data until
    # 2021-05-09T23:59:59. This makes date_to inclusive until
    # date to at 23:59:59. We later filter the data
    date_to_inclusive = get_date_in_many_formats(date_to) + datetime.timedelta(days=1)
    date_to_inclusive = date_to_inclusive.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    tta.download_raw_datasets(exchange, pair_str, tmp_folder, type_of_data=type_of_data,
                              start_date=date_from, end_date=date_to_inclusive)

    return tmp_folder


def get_exchange_data(exchange, pair_str, date_from, date_to, type_of_data):
    pair_str = pair_str.lower()
    datasets = tta.get_all_exchange_datasets(exchange)

    if pair_str not in datasets.keys():
        raise Exception(f'Did not find {pair_str} in datasets of exchange {exchange}')

    dataset_info = datasets[pair_str]
    if type_of_data not in dataset_info['dataTypes']:
        raise Exception(f'Did not find {type_of_data} in dataset {dataset_info}')

    tmp_folder = tempfile.mkdtemp() + '/'

    # tardis API only returns integral data until the previous day, i.e,
    # requesting data until 2021-05-10T12:00:00 will only get data until
    # 2021-05-09T23:59:59. This makes date_to inclusive until
    # date to at 23:59:59. We later filter the data
    date_to_inclusive = get_date_in_many_formats(date_to) + datetime.timedelta(days=1)
    date_to_inclusive = date_to_inclusive.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    dataset_df = tta.get_all_table(exchange, pair_str, tmp_folder, type_of_data=type_of_data,
                                  start_date=date_from, end_date=date_to_inclusive)
    return dataset_df

def get_quotes_dataframe(start_date_str, end_date_str, exchange_str, pair_str, lookback_seconds_int=60):

    try:
        # make sure dates are standard
        start_date_datetime = get_date_in_many_formats(start_date_str)
        start_date_str = start_date_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # to guarantee we have enough data to calculate volatility over required time
        start_date_inclusive = start_date_datetime - datetime.timedelta(seconds=lookback_seconds_int)
        start_date_inclusive_str = start_date_inclusive.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        end_date_datetime = get_date_in_many_formats(end_date_str)
        end_date_str = end_date_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # get raw data
        quotes_df = get_exchange_data(exchange_str, pair_str, start_date_inclusive_str, end_date_str, 'quotes')
        # transform raw data
        quotes_df = get_normalized_df(quotes_df, start_date_inclusive_str, end_date_str)
        # create volatility df by merging trades and quotes on
        # datetimes from trades
        quotes_df = calc_mid_quote(quotes_df)
        # quotes_df = quotes_df[['datetime', 'mid_quote']]
    except Exception as e:
        raise Exception(f"Error in get_quotes_dataframe:\n{e}")
    return quotes_df

def get_book_with_depth(book_df, start_date_str, end_date_str, book_depth):

    def extract_book_depth_from_col_name(col_name):
        return int(col_name.split('[')[-1].split(']')[0])

    book_df = calc_mid_quote(book_df, col_suffix='s[0].price')

    depth_to_drop_list = [col for col in book_df.columns if ('asks' in col or 'bids' in col) and (book_depth <= extract_book_depth_from_col_name(col))]
    book_df = drop_columns_df(book_df, depth_to_drop_list)

    book_df = get_normalized_df(book_df, start_date_str, end_date_str)
    return book_df


def get_book_dataframe(start_date_str, end_date_str, exchange_str, pair_str, book_depth=-1):
    # make sure dates are standard
    start_date_str = get_date_in_many_formats(start_date_str).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    end_date_str = get_date_in_many_formats(end_date_str).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    book_depth = max(0, min(book_depth, 25))

    book_df = get_exchange_data(exchange_str, pair_str, start_date_str, end_date_str, 'book_snapshot_25')
    book_df = get_book_with_depth(book_df, start_date_str, end_date_str, book_depth)

    return book_df

def get_trades_dataframe(start_date_str, end_date_str, exchange_str, pair_str, lookback_seconds_int=0):
    # make sure dates are standard
    start_date_datetime = get_date_in_many_formats(start_date_str)
    start_date_str = start_date_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # to guarantee we have enough data to calculate backtracking data, e.g volatility, over required time
    start_date_inclusive = start_date_datetime - datetime.timedelta(seconds=lookback_seconds_int)
    start_date_inclusive_str = start_date_inclusive.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    end_date_datetime = get_date_in_many_formats(end_date_str)
    end_date_str = end_date_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    trades_df = get_exchange_data(exchange_str, pair_str, start_date_inclusive_str, end_date_str, 'trades')
    print(trades_df)
    return trades_df


def get_trades_with_mid_quote_dataframe(start_date_str, end_date_str, exchange_str, pair_str, book_depth=-1):
    # make sure dates are standard
    start_date_datetime = get_date_in_many_formats(start_date_str)
    start_date_str = start_date_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    day_before = start_date_datetime - datetime.timedelta(days=1)
    day_before_str = day_before.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    end_date_datetime = get_date_in_many_formats(end_date_str)
    end_date_str = end_date_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # get raw data
    tmp_folder_trades = download_exchange_raw_data(exchange_str, pair_str, start_date_str, end_date_str, 'trades')
    tmp_folder_books = download_exchange_raw_data(exchange_str, pair_str, day_before_str, end_date_str, 'book_snapshot_25')

    trades_dict = {re.search(r'(\d+-\d+-\d+)', file).group():os.path.join(tmp_folder_trades, file) for file in os.listdir(tmp_folder_trades) if 'trades' in file}
    books_dict = {re.search(r'(\d+-\d+-\d+)', file).group():os.path.join(tmp_folder_books, file) for file in os.listdir(tmp_folder_books) if 'book_snapshot_25' in file}

    merged_trades_df = pd.DataFrame()
    for day in sorted(trades_dict.keys()):
        day_datetime = get_date_in_many_formats(day)
        day_after = (day_datetime + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        day_before = (day_datetime - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        day_trades_df = pd.read_csv(trades_dict[day])
        day_trades_df = get_normalized_df(day_trades_df, start_date_str, end_date_str)

        # transform raw data

        # get book with 1 day before
        book_df_y = pd.read_csv(books_dict[day_before])
        book_df_t = pd.read_csv(books_dict[day])
        book_df = book_df_y.append(book_df_t)
        book_df = get_book_with_depth(book_df.copy(), day_before, day_after, book_depth)

        del(book_df_y)
        del(book_df_t)

        # create volatility df by merging trades and quotes on
        # datetimes from trades
        day_trades_df = pd.merge_asof(day_trades_df, book_df, on='datetime', allow_exact_matches=True)
        day_trades_df = drop_columns_df(day_trades_df, ['timestamp_x', 'timestamp_y'])

        merged_trades_df = merged_trades_df.append(day_trades_df)


    # remove_dir(tmp_folder_trades)
    # remove_dir(tmp_folder_books)
    return merged_trades_df


def get_derivatives_dataframe(start_date_str, end_date_str, exchange_str, pair_str, lookback_seconds_int=0):
    # make sure dates are standard
    start_date_datetime = get_date_in_many_formats(start_date_str)
    start_date_str = start_date_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # to guarantee we have enough data to calculate volatility over required time
    start_date_inclusive = start_date_datetime - datetime.timedelta(seconds=lookback_seconds_int)
    start_date_inclusive_str = start_date_inclusive.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    end_date_datetime = get_date_in_many_formats(end_date_str)
    end_date_str = end_date_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # get raw data
    derivatives_df = get_exchange_data(exchange_str, pair_str, start_date_inclusive_str, end_date_str, 'derivative_ticker')
    # transform raw data
    derivatives_df = get_normalized_df(derivatives_df, start_date_inclusive_str, end_date_str)

    return derivatives_df
