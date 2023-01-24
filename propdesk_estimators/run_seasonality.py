import argparse
import datetime
import json
import numpy as np
import os
import pandas as pd
import tempfile
import time
import traceback
import warnings
warnings.filterwarnings("ignore")  # Ignore warnings coming from Arrow optimizations.

from pandarallel import pandarallel as pl
pl.initialize()

from propdesk_services.microsoft_teams import alert_teams_error_spark
from propdesk_services.microsoft_teams import alert_teams_success_spark

from propdesk_estimators.data_extraction import get_trades_dataframe

from propdesk_estimators.data_raw_transformation import make_datetime_index
from propdesk_estimators.data_raw_transformation import get_normalized_df
from propdesk_estimators.data_raw_transformation import calc_volume

from propdesk_estimators.data_upload import upload_dataframe_daily

from propdesk_estimators.utils import get_date_in_many_formats
from propdesk_estimators.utils import get_dates_dataframe


def calc_seasonality(t, volume_df, lookback_days):
    hour = t.hour
    minute = t.minute

    # get volume where [t-lookback_days, t) and in the same bucket (hour, minute)
    lookback_volume_df = volume_df[(volume_df.index >= t-datetime.timedelta(days=lookback_days))
                                   & (volume_df.index < t)
                                   & (volume_df['hour'] == hour)
                                   & (volume_df['minute'] == minute)]
    # calc mean per bucket
    average_volume = np.median(lookback_volume_df['amount'])

    return average_volume


def seasonality_estimation(volume_df, dates_df, resampling_rule_str='60S', lookback_days=None):

    volume_df['hour'] = volume_df.index.hour
    volume_df['minute'] = volume_df.index.minute

    seasonality_estimation_df = make_datetime_index(dates_df)
    # calculate estimation
    seasonality_estimation_df['seasonality_estimation'] = seasonality_estimation_df['datetime'].parallel_apply(lambda t: calc_seasonality(t, volume_df, lookback_days))
    seasonality_estimation_df = seasonality_estimation_df.dropna()

    return seasonality_estimation_df


def main(exchange_str, pair_str,
         start_date_str, end_date_str,
         resampling_rule_str, lookback_days,
         live=False, tmp_folder=None):

    lookback_date = get_date_in_many_formats(start_date_str) - datetime.timedelta(days=lookback_days)
    lookback_date_str = lookback_date.strftime('%Y-%m-%d')

    dataset_type = 'seasonality'

    if not tmp_folder:
        tmp_folder = tempfile.mkdtemp()

    trades_df = get_trades_dataframe(lookback_date_str, end_date_str, exchange_str, pair_str)

    trades_df = get_normalized_df(trades_df, lookback_date_str, end_date_str)
    trades_df = make_datetime_index(trades_df)
    volume_df = calc_volume(trades_df, resampling_rule_str)

    dates_df = get_dates_dataframe(start_date_str, end_date_str, resampling_rule_str)
    seasonality_df = seasonality_estimation(volume_df, dates_df, resampling_rule_str, lookback_days)

    if live:
        base_tags_dict = {'dataset_type': dataset_type,
                          'pair': pair_str,
                          'resampling_rule': resampling_rule_str,
                          'lookback_days': lookback_days
                         }

        upload_dataframe_daily(seasonality_df, exchange_str, dataset_type, pair_str, base_tags_dict)

    else:
        for d in get_dates_dataframe(str(min(seasonality_df['datetime'])), str(max(seasonality_df['datetime'])), '1D', inclusive=True)['datetime']:
            year = d.year
            month = d.month
            day = d.day

            day_ref_str = f'{year}-{month}-{day}'

            daily_slice_df = seasonality_df[seasonality_df['datetime'].between(d, d + datetime.timedelta(days=1), inclusive='left')]
            daily_slice_df = daily_slice_df.reset_index(drop=True)
            print('daily_slice_df')
            print(daily_slice_df)
            dest_path = os.path.join(tmp_folder, pair_str)
            os.makedirs(dest_path, exist_ok=True)

            daily_slice_df.to_parquet(os.path.join(dest_path, day_ref_str))
            print(f'Saved dataset to {os.path.join(dest_path, day_ref_str)}')

    return seasonality_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # execution params - 1 letter
    parser.add_argument("-l", "--live", action='store_true', help='pass -l only if you want to make state changes')

    # general info - 2 letters
    parser.add_argument("-ex", "--exchange", type=str, help='exchange name to retrieve data from')
    parser.add_argument("-pa", "--pair", type=str, help='pair name, e.g., btcusdt')
    parser.add_argument("-sd", "--start_date", type=str, help='date to retrieve data from')
    parser.add_argument("-ed", "--end_date", type=str, help='date to retrieve data until')

    # data processing parameters - 3 letters
    parser.add_argument("-rsr", "--resampling_rule", type=str, help='pandas resampling rule, Default: 60S', default='60S')
    parser.add_argument("-lbt", "--lookback_days", type=int, help='loookback time in days, Default: 30', default=60)

    # databricks argument parser
    parser.add_argument("-params", "--params", type=str, help="pass a dict of params as a json string", default='{}')

    args = parser.parse_args()

    live = args.live

    exchange = args.exchange
    pair = args.pair

    start_date = args.start_date
    end_date = args.end_date

    resampling_rule = args.resampling_rule
    lookback_days = int(args.lookback_days)

    try:
        print('----xxxx----')
        print(args.params)
        params_dict = json.loads(args.params)

        # override params when passing params_dict (to be used in databricks jobs and when amending a dataset timeline)
        if params_dict:
            live = params_dict.get('live') or live
            exchange = params_dict.get('exchange') or exchange
            pair = params_dict.get('pair') or pair
            start_date = params_dict.get('start_date') or start_date
            end_date = params_dict.get('end_date') or end_date
            resampling_rule = params_dict.get('resampling_rule') or resampling_rule
            lookback_seconds = params_dict.get('lookback_days') or lookback_seconds

        print('live', live)
        print('exchange', exchange)
        print('pair', pair)
        print('start_date', start_date)
        print('end_date', end_date)
        print('resampling_rule', resampling_rule)
        print('lookback_days', lookback_days)

        start = time.time()
        seasonality_df = main(exchange, pair,
                              start_date, end_date,
                              resampling_rule, lookback_days,
                              live)
        end = time.time()
        print(seasonality_df)
        success_message = f'Ran seasonality\n -params {json.dumps(params_dict)}\n -time[min] {(end-start)}'
        print(success_message)
        if live:
            alert_teams_success_spark(success_message)

    except Exception as e:
        error_message = f'Error running seasonality_zma: {e}'
        error_message += f'\n -traceback {traceback.format_exc()}'
        error_message += f'\n -params {json.dumps(params_dict)}'

        if live:
            alert_teams_error_spark(error_message)

        print(error_message)
        raise
