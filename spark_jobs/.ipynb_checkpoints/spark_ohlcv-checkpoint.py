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
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import udf, lit

from propdesk_azure_services.microsoft_teams import alert_teams_error_spark
from propdesk_azure_services.microsoft_teams import alert_teams_success_spark

from propdesk_estimators.data_extraction import get_trades_dataframe

from propdesk_estimators.data_raw_transformation import make_datetime_index
from propdesk_estimators.data_raw_transformation import get_normalized_df
from propdesk_estimators.data_raw_transformation import calc_volume

from propdesk_estimators.data_upload import upload_dataframe_daily


from propdesk_estimators.utils import get_date_in_many_formats
from propdesk_estimators.utils import get_dates_dataframe

from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import Window

def calc_ohlcv(trades_df, resampling_rule_str='1H'):
    """
        Creates an open, high, low, close, volume dataset based on trades dataset.
        open, high, low, close columns are given by resample() method from pandas
        volumn is the sum of amount of trades in period
        Inputs:
            df_trades: dataframe which contains all trades in period, with standardized datastam column
            freq: the frequency to resample and aggregated the rows
        Outputs:
            dataframe [datastamp, open, high, low, close, volume]
    """

    # first create ohlc columns from pandas resample() method
    ohlcv_df = trades_df['price'].resample(resampling_rule_str).ohlc()
    volume_df = calc_volume(trades_df, resampling_rule_str)
    # then define volume as the sum of ocurrences on the interval
    ohlcv_df = ohlcv_df.merge(volume_df, left_index=True, right_index=True)
    # rename amount
    ohlcv_df = ohlcv_df.rename(columns={'amount': 'volume'})
    
    return ohlcv_df

def main(exchange_str, pair_str, 
         start_date_str, end_date_str,
         resampling_rule_str,
         live=False, tmp_folder=None):

    dataset_type = 'ohlcv'
    
    if not tmp_folder:
        tmp_folder = tempfile.mkdtemp()

    trades_df = get_trades_dataframe(start_date_str, end_date_str, exchange_str, pair_str)
    trades_df = get_normalized_df(trades_df, start_date_str, end_date_str)
    trades_df = make_datetime_index(trades_df)

    spark_session = SparkSession.builder.appName("ohlcv").getOrCreate()
    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)

    spark_context = spark_session.sparkContext

    trades_sdf = spark_session.createDataFrame(trades_df)
    spark_resampling_rule_dict = {'1H': '1 hour',
                                  '1T': '1 minute',
                                 }

    spark_resample_str = spark_resampling_rule_dict.get(resampling_rule_str.upper())
    if not spark_resample_str:
        raise Exception(f'cannot parse {resampling_rule_str} as spark resampling time')

    trades_window_sdf = trades_sdf.withColumn("window", F.window(F.col("datetime"), spark_resample_str)["start"])

    window_spec = Window.partitionBy("window").orderBy("datetime").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    trades_window_sdf = trades_window_sdf.withColumn("open", F.first(F.col("price")).over(window_spec))\
                                         .withColumn("close", F.last(F.col("price")).over(window_spec))\
                                         .withColumn("high", F.max(F.col("price")).over(window_spec))\
                                         .withColumn("low", F.min(F.col("price")).over(window_spec))\
                                         .withColumn("low", F.min(F.col("price")).over(window_spec))\
                                         .withColumn("volume", spark_round(F.sum(F.col("amount")).over(window_spec), 5))\
                                         .withColumn("rn", F.row_number().over(Window.partitionBy("window").orderBy("datetime")))\
                                         .filter(F.col("rn") == 1)\
                                         .selectExpr("window as datetime", "open", "high", "low", "close", "volume")\
                                         .orderBy("datetime")

    ohlcv_df = trades_window_sdf.toPandas()
    print(ohlcv_df)
    if live:
        base_tags_dict = {'dataset_type': dataset_type,
                          'pair': pair_str,
                          'resampling_rule': resampling_rule_str,
                         }

        upload_dataframe_daily(ohlcv_df, exchange_str, dataset_type, pair_str, base_tags_dict)

    else:
        for d in get_dates_dataframe(str(min(ohlcv_df['datetime'])), str(max(ohlcv_df['datetime'])), '1D', inclusive=True)['datetime']:
            year = d.year
            month = d.month
            day = d.day

            day_ref_str = f'{year}-{month}-{day}'

            daily_slice_df = ohlcv_df[ohlcv_df['datetime'].between(d, d + datetime.timedelta(days=1), inclusive='left')]

            dest_path = os.path.join(tmp_folder, pair_str)
            os.makedirs(dest_path, exist_ok=True)

            daily_slice_df.to_parquet(os.path.join(dest_path, day_ref_str))
            print(f'Saved dataset to {os.path.join(dest_path, day_ref_str)}')

    return ohlcv_df


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
    parser.add_argument("-rsr", "--resampling_rule", type=str, help='pandas resampling rule, Default: 1H', default='1H')

    # databricks argument parser
    parser.add_argument("-params", "--params", type=str, help="pass a dict of params as a json string", default='{}')

    args = parser.parse_args()

    live = args.live

    exchange = args.exchange
    pair = args.pair

    start_date = args.start_date
    end_date = args.end_date

    resampling_rule = args.resampling_rule

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

        print('live', live)
        print('exchange', exchange)
        print('pair', pair)
        print('start_date', start_date)
        print('end_date', end_date)
        print('resampling_rule', resampling_rule)

        start = time.time()
        ohlcv_df = main(exchange, pair, 
                        start_date, end_date,
                        resampling_rule, live)
        
        end = time.time()
        print(ohlcv_df)
        success_message = f'Ran ohlcv\n -params {json.dumps(params_dict)}\n -time[min] {(end-start)/60}'
        print(success_message)
        if live:
            alert_teams_success_spark(success_message)

    except Exception as e:
        error_message = f'Error running ohlcv: {e}'
        error_message += f'\n -traceback {traceback.format_exc()}'
        error_message += f'\n -params {json.dumps(params_dict)}'

        if live:
            alert_teams_error_spark(error_message)

        print(error_message)
        raise
