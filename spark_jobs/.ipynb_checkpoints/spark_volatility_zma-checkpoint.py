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

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, lit

from propdesk_azure_services.microsoft_teams import alert_teams_error_spark
from propdesk_azure_services.microsoft_teams import alert_teams_success_spark

from propdesk_estimators.data_extraction import get_quotes_dataframe

from propdesk_estimators.data_raw_transformation import make_datetime_index
from propdesk_estimators.data_raw_transformation import resample_mid_quote
from propdesk_estimators.data_upload import upload_dataframe_daily

from propdesk_estimators.estimators import ZMA

from propdesk_estimators.utils import get_date_in_many_formats
from propdesk_estimators.utils import get_dates_dataframe



def make_date_partition(t_datetime):
    return f'{t_datetime.year:04d}-{t_datetime.month:02d}-{t_datetime.day:02d}-{t_datetime.hour:02d}'


def spark_zma_volatility_estimation(quotes_df, dates_df, resample_rule_str='100mS',
                                    lookback_seconds_int=60, bandwidth_int=5,
                                    annualized_volatility_bool=False, log_price_bool=True, mid_quote_change_bool=True):

    @udf(returnType=DoubleType())
    def calc_volatility_spark(t_datetime, resample_rule_str, seconds_int, bandwidth_int, annualized_volatility_bool, log_price_bool, mid_quote_change_bool):
        """
        Function to prepare and apply ZMA estimation to each t_datetime row from volatility_estimation_df
        Inputs:
            - t_datetime : datetime
            - start_date_datetime: first date to calculate volatility
            - quotes_df: the quotes dataset to calculate volatility from
            - resample_rule_str: resampling rule to apply on quotes (e.g. 100mS)
            - seconds_int: lookback time, in seconds, to slice quotes dataset
            - bandwidth_int: ZMA estimator parameter
            - annualized_volatility_bool: ZMA estimator parameter
            - log_price_bool: use log of prices as input
            - mid_quote_change_bool: filter mid quotes on change
        Output:
            - volatility_estimation: single estimation for t_datetime
        """
        lookup_date = make_date_partition(t_datetime)
        quotes_df_partition_broadcast = prices_hashmap.get(lookup_date)

        if quotes_df_partition_broadcast is None:
            return np.NaN

        quotes_df_partition = quotes_df_partition_broadcast.value

        # get exactly the midquotes we need between t_datetime and lookback_seconds
        delta_t_datetime = t_datetime - datetime.timedelta(seconds=seconds_int)
        prices_df = quotes_df_partition['mid_quote'][quotes_df_partition['datetime'].between(delta_t_datetime, t_datetime)]

        resampled_prices_df = resample_mid_quote(prices_df, resample_rule_str)

        if mid_quote_change_bool:
            resampled_prices_df = resampled_prices_df[resampled_prices_df.diff()!=0]

        if resampled_prices_df.shape[0] == 0:
            return np.NaN

        volatility_estimation = ZMA(resampled_prices_df, bandwidth_int, annualized_volatility_bool, log_price_bool)
        return float(volatility_estimation)

    # we'll need datetime indexed df to resample
    quotes_df = quotes_df[['datetime', 'mid_quote']]
    quotes_df = make_datetime_index(quotes_df)

    # drop duplicates on index
    quotes_df = quotes_df[~quotes_df.index.duplicated(keep='first')]

    date_partitions = sorted(quotes_df['datetime'].apply(make_date_partition).unique())
    spark_session = SparkSession.builder.appName("vol_estimation").getOrCreate()
    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)

    spark_context = spark_session.sparkContext

    prices_hashmap = {}
    for date_partition in date_partitions:
        try:
            date_partition_datetime = get_date_in_many_formats(date_partition)
            to_datetime = date_partition_datetime + datetime.timedelta(hours=1)
            delta_t_datetime = date_partition_datetime - datetime.timedelta(seconds=lookback_seconds_int)

            prices_df = quotes_df[quotes_df['datetime'].between(delta_t_datetime, to_datetime)]
            if len(prices_df) > 0:
                prices_hashmap.setdefault(date_partition, spark_context.broadcast(prices_df))
            else:
                raise(f'Empty prices df on {date_partition}')

        except Exception as e:
            print(e)

    prices_hashmap_broadcast = spark_context.broadcast(prices_hashmap)
    sdf = spark_session.createDataFrame(dates_df)

    sdf = sdf.withColumn('volatility_estimation', calc_volatility_spark('datetime', lit(resample_rule_str),
                                                                    lit(lookback_seconds_int), lit(bandwidth_int),
                                                                    lit(annualized_volatility_bool), lit(log_price_bool),
                                                                    lit(mid_quote_change_bool)
                                                                    ))
    volatility_df = sdf.toPandas()
    volatility_df = volatility_df.dropna()

    return volatility_df


def main(exchange_str, pair_str, start_date_str, end_date_str,
         resampling_rule_str, lookback_seconds_int, bandwidth_int,
         annualized_volatility, log_price, mid_quote_change, live=False, tmp_folder=None):

    dataset_type = 'volatility_zma_mqc'
    if not mid_quote_change:
        dataset_type = 'volatility_zma'

    if not tmp_folder:
        tmp_folder = tempfile.mkdtemp()

    quotes_df = get_quotes_dataframe(start_date_str, end_date_str, exchange_str, pair_str, lookback_seconds_int)
    dates_df = get_dates_dataframe(start_date_str, end_date_str, resampling_rule_str)

    volatility_df = spark_zma_volatility_estimation(quotes_df, dates_df, resampling_rule_str,
                                                    lookback_seconds_int, bandwidth_int,
                                                    annualized_volatility, log_price, mid_quote_change)

    if live:
        base_tags_dict = {'dataset_type': dataset_type,
                          'pair': pair_str,
                          'resample': resampling_rule_str,
                          'lookback': lookback_seconds_int, 'bandwidth': bandwidth_int,
                          'annual': annualized_volatility, 'log': log_price, 'mid_quote_change': mid_quote_change}

        upload_dataframe_daily(volatility_df, exchange_str, dataset_type, pair_str, base_tags_dict)

    else:
        for d in get_dates_dataframe(str(min(volatility_df['datetime'])), str(max(volatility_df['datetime'])), '1D', inclusive=True)['datetime']:
            year = d.year
            month = d.month
            day = d.day

            day_ref_str = f'{year}-{month}-{day}'

            daily_slice_df = volatility_df[volatility_df['datetime'].between(d, d + datetime.timedelta(days=1), inclusive='left')]

            dest_path = os.path.join(tmp_folder, pair_str)
            os.makedirs(dest_path, exist_ok=True)

            daily_slice_df.to_parquet(os.path.join(dest_path, day_ref_str))
            print(f'Saved dataset to {os.path.join(dest_path, day_ref_str)}')

    return volatility_df


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
    parser.add_argument("-rsr", "--resampling_rule", type=str, help='pandas resampling rule, Default: 100mS', default='100mS')
    parser.add_argument("-lbt", "--lookback_seconds", type=int, help='loookback time in seconds, e.g., 60 for 1 minute. Default: 60', default=60)

    # estimator specific params - 4 letters
    parser.add_argument("-band", "--bandwidth", type=int, help='bandwidth parameter of zma estimator', default=5)
    parser.add_argument("-annv", "--annualized_volatility", help='calculate volatility annualized', action='store_true')
    parser.add_argument("-logp", "--log_price", help='use log of prices', action='store_false')
    parser.add_argument("-mqch", "--mid_quote_change", help='use only mid quotes on change', action='store_false')

    # databricks argument parser
    parser.add_argument("-params", "--params", type=str, help="pass a dict of params as a json string", default='{}')

    args = parser.parse_args()

    live = args.live

    exchange = args.exchange
    pair = args.pair

    start_date = args.start_date
    end_date = args.end_date

    resampling_rule = args.resampling_rule
    lookback_seconds = int(args.lookback_seconds)

    bandwidth = int(args.bandwidth)
    annualized_volatility = args.annualized_volatility
    log_price = args.log_price
    mid_quote_change = args.mid_quote_change

    try:
        print('----xxxx----')
        print(args.params)
        params_dict = json.loads(args.params)

        # override params when passing params_dict (to be used in databricks jobs and when amending a dataset timeline)
        if params_dict:
            # [fpapi] I fucked up on this one, sorry m8s
            # when params_dict is coming from amend (which searches azure and then computes),
            # they come with different name. We need to check for them too
            # tags on azure blob:
                # annual, bandwidth, log, lookback, pair, resample
            live = params_dict.get('live') or live
            exchange = params_dict.get('exchange') or exchange
            pair = params_dict.get('pair') or pair
            start_date = params_dict.get('start_date') or start_date
            end_date = params_dict.get('end_date') or end_date
            resampling_rule = params_dict.get('resampling_rule') or params_dict.get('resampling_rule') or resampling_rule
            lookback_seconds = params_dict.get('lookback_seconds') or params_dict.get('lookback') or lookback_seconds
            bandwidth = params_dict.get('bandwidth') or bandwidth
            annualized_volatility = params_dict.get('annualized_volatility') or params_dict.get('annual') or annualized_volatility
            log_price = params_dict.get('log_price') or params_dict.get('log') or log_price
            mid_quote_change = params_dict.get('mid_quote_change') or mid_quote_change

        print('live', live)
        print('exchange', exchange)
        print('pair', pair)
        print('start_date', start_date)
        print('end_date', end_date)
        print('resampling_rule', resampling_rule)
        print('lookback_seconds', lookback_seconds)
        print('bandwidth', bandwidth)
        print('annualized_volatility', annualized_volatility)
        print('log_price', log_price)
        print('mid_quote_change', mid_quote_change)

        start = time.time()
        volatility_df = main(exchange,
                             pair, start_date,
                             end_date, resampling_rule,
                             lookback_seconds, bandwidth,
                             annualized_volatility, log_price, mid_quote_change, live)
        end = time.time()
        success_message = f'Ran volatility_zma\n -params {json.dumps(params_dict)}\n -time[min] {(end-start)//60}'
        alert_teams_success_spark(success_message)
        print(success_message)

    except Exception as e:
        error_message = f'Error running volatility_zma: {e}'
        error_message += f'\n -traceback {traceback.format_exc()}'
        error_message += f'\n -params {json.dumps(params_dict)}'
        alert_teams_error_spark(error_message)
        print(error_message)
        raise
