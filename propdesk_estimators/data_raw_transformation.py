import pandas as pd
import datetime

from propdesk_estimators.utils import get_date_in_many_formats

def make_datetime_index(dataframe):
    dataframe.index = dataframe['datetime']
    return dataframe


def normalize_datetime_df(dataframe, date_from_str=None, date_to_str=None):

    if 'local_timestamp' in dataframe.columns:
        dataframe['datetime'] = pd.to_datetime(dataframe['local_timestamp'], unit = 'us')

    if date_from_str:
        dataframe = dataframe[dataframe['datetime'] >= get_date_in_many_formats(date_from_str)]
    if date_to_str:
        dataframe = dataframe[dataframe['datetime'] < get_date_in_many_formats(date_to_str)]

    dataframe = dataframe.sort_values('datetime')

    if len(dataframe)==0:
        raise Exception(f"Empty dataframe when normalizing from {date_from_str} to: {date_to_str}")

    return dataframe


def drop_columns_df(dataframe, column_list):
    return dataframe.drop(column_list, axis=1, errors='ignore')


def resample_mid_quote(quote_df, resample_rule):
    quote_df = quote_df.resample(resample_rule).ffill()
    quote_df = quote_df.dropna()
    return quote_df


def calc_mid_quote(quote_df, col_suffix='_price'):
    quote_df['mid_quote'] = quote_df[f'ask{col_suffix}']*0.5 + quote_df[f'bid{col_suffix}']*0.5
    return quote_df


def get_normalized_df(dataframe, start_date_str, end_date_str):
    """
    Make datasets standard for manipulation by converting timestamp (long int) to
    datetime, chopping datasets exactly on start_date : end_date and dropping
    unnecessary columns
    """
    dataframe = normalize_datetime_df(dataframe, start_date_str, end_date_str)
    dataframe = drop_columns_df(dataframe, ['symbol','exchange','local_timestamp', 'timestamp', 'id'])

    return dataframe


def calc_volume(trades_df, resampling_rule_str):
    trades_df = trades_df[['amount']].resample(resampling_rule_str).sum(min_count=1)
    # trades_df = trades_df.fillna(value=0)
    trades_df = trades_df.dropna()
    return trades_df
