#!/usr/bin/python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:

import json
import unittest
import tempfile
import os
import pandas as pd

from copy import deepcopy

from propdesk_estimators.run_volatility_zma import calc_volatility
from propdesk_estimators.run_volatility_zma import zma_volatility_estimation

from propdesk_estimators.run_seasonality import calc_seasonality
from propdesk_estimators.run_seasonality import main as seasonality_estimation

from propdesk_estimators.spark_jobs.spark_volatility_zma import main as spark_zma_estimation
from propdesk_estimators.spark_jobs.spark_seasonality import main as spark_seasonality_estimation

from propdesk_estimators.data_extraction import get_exchange_data
from propdesk_estimators.data_extraction import get_trades_dataframe

from propdesk_estimators.data_raw_transformation import calc_mid_quote
from propdesk_estimators.data_raw_transformation import calc_volume
from propdesk_estimators.data_raw_transformation import drop_columns_df
from propdesk_estimators.data_raw_transformation import get_normalized_df
from propdesk_estimators.data_raw_transformation import make_datetime_index
from propdesk_estimators.data_raw_transformation import normalize_datetime_df
from propdesk_estimators.data_raw_transformation import resample_mid_quote

from propdesk_estimators.utils import get_date_in_many_formats

dataframe = get_exchange_data('binance', 'adabrl', '2021-05-10T12:00:32.000Z', '2021-05-11T12:12:21.000Z', 'quotes')

class EstimatorsCommonsTest(unittest.TestCase):

    def test_get_exchange_data(self):

        result = deepcopy(dataframe)

        result_min = result.timestamp.min()
        result_max = result.timestamp.max()
        result_sum = result.bid_price.sum()

        expected_min_timestamp = 1620604801154281
        expected_max_timestamp = 1620863999382000
        expected_check_sum_bid_price = 2070403.634

        self.assertEqual(result_min, expected_min_timestamp)
        self.assertEqual(result_max, expected_max_timestamp)
        self.assertEqual(result_sum, expected_check_sum_bid_price)


    def test_normalize_datetime_df(self):

        result = normalize_datetime_df(deepcopy(dataframe), '2021-05-10T12:00:32.000Z', '2021-05-10T12:12:21.000Z')

        result_min_date_str = result.datetime.iloc[0].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        result_max_date_str = result.datetime.iloc[-1].strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        expected_min_date_str ='2021-05-10T12:00:33.646989Z'
        expected_max_date_str ='2021-05-10T12:12:20.317136Z'

        self.assertTrue('datetime' in list(result.columns))
        self.assertEqual(result_min_date_str, expected_min_date_str)
        self.assertEqual(result_max_date_str, expected_max_date_str)


    def test_drop_columns_df(self):

        result =  drop_columns_df(deepcopy(dataframe), ['exchange', 'symbol', 'timestamp', 'local_timestamp'])

        result_cols = list(result.columns)
        expected_cols = ['ask_amount', 'ask_price', 'bid_price', 'bid_amount']

        self.assertEqual(expected_cols, result_cols)


    def test_get_normalized_df(self):

        result =  get_normalized_df(deepcopy(dataframe), '2021-05-10T12:00:32.000Z', '2021-05-10T12:12:21.000Z')

        result_min_date_str = result.datetime.min().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        result_max_date_str = result.datetime.max().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        result_checksum_bid_amount = result.bid_amount.sum()
        result_columns = list(result.columns)

        expected_min_date_str = '2021-05-10T12:00:33.646989Z'
        expected_max_date_str = '2021-05-10T12:12:20.317136Z'
        expected_checksum_bid_amount = 1748228.56
        expected_columns = ['ask_amount', 'ask_price', 'bid_price', 'bid_amount', 'datetime']

        self.assertEqual(result_min_date_str, expected_min_date_str)
        self.assertEqual(result_max_date_str, expected_max_date_str)
        self.assertEqual(result_checksum_bid_amount, expected_checksum_bid_amount)
        self.assertEqual(result_columns, expected_columns)


    def test_calc_mid_quote(self):

        result = calc_mid_quote(deepcopy(dataframe.head()))

        result_checksum_mid_quote = result.mid_quote.sum()
        expected_checksum_mid_quote = 46.7885

        self.assertTrue('mid_quote' in list(result.columns))
        self.assertEqual(result_checksum_mid_quote, expected_checksum_mid_quote)

    def test_calc_volume(self):
        trades_df = pd.DataFrame.from_dict({'datetime': [get_date_in_many_formats(f'2021-12-01 00:{h}:00') for h in range(0,60) if not 40 <= h < 50], 'amount': [h for h in range(0,60) if not 40 <= h < 50]})
        trades_df = make_datetime_index(trades_df)

        result_volume_df = calc_volume(trades_df, '600S')
        expected_volume_df = pd.DataFrame.from_dict({'datetime': [get_date_in_many_formats(f'2021-12-01 00:00:00'),
                                                                  get_date_in_many_formats(f'2021-12-01 00:10:00'),
                                                                  get_date_in_many_formats(f'2021-12-01 00:20:00'),
                                                                  get_date_in_many_formats(f'2021-12-01 00:30:00'),
                                                                  get_date_in_many_formats(f'2021-12-01 00:50:00')],
                                                     'amount': [45.0,145.0,245.0,345.0,545.0]})

        expected_volume_df = make_datetime_index(expected_volume_df)
        expected_volume_df = drop_columns_df(expected_volume_df, ['datetime'])

        pd.testing.assert_frame_equal(result_volume_df, expected_volume_df)

class VolatilityTest(unittest.TestCase):
    def test_calc_volatility(self):

        t = get_date_in_many_formats('2021-05-10T12:07:27.066000Z')
        mid_quotes_df = calc_mid_quote(make_datetime_index(normalize_datetime_df(deepcopy(dataframe))))
        result_volatility = calc_volatility(t, mid_quotes_df, '1S', 60, 5, True, True, False)
        expected_volatility = 0.5071744265982981

        self.assertEqual(result_volatility, expected_volatility)

    def test_spark_vol_zma(self):
        exchange_str = 'binance'
        pair_str = 'adabrl'
        start_date_str = '2021-04-30T23:59:27.066000'
        end_date_str = '2021-05-01T00:01:27.066000'
        resample_rule_str='100mS'
        lookback_seconds_int=60
        bandwidth_int=5
        annualized_volatility_bool=False
        log_price_bool=True
        mid_quote_change_bool = False

        python_vol_calc = zma_volatility_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                                    resample_rule_str, lookback_seconds_int, bandwidth_int,
                                                    annualized_volatility_bool, log_price_bool, mid_quote_change_bool)

        spark_vol_calc = spark_zma_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                              resample_rule_str, lookback_seconds_int, bandwidth_int,
                                              annualized_volatility_bool, log_price_bool, mid_quote_change_bool)

        self.assertEqual(min(python_vol_calc.index), min(spark_vol_calc['datetime']))
        self.assertEqual(max(python_vol_calc.index), max(spark_vol_calc['datetime']))
        self.assertEqual(sum(python_vol_calc['volatility_estimation']), sum(spark_vol_calc['volatility_estimation']))
        
        resample_rule_str='1S'
        lookback_seconds_int=120
        bandwidth_int=15
        annualized_volatility_bool=True
        log_price_bool=False
        python_vol_calc = zma_volatility_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                                    resample_rule_str, lookback_seconds_int, bandwidth_int,
                                                    annualized_volatility_bool, log_price_bool, mid_quote_change_bool)

        spark_vol_calc = spark_zma_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                              resample_rule_str, lookback_seconds_int, bandwidth_int,
                                              annualized_volatility_bool, log_price_bool, mid_quote_change_bool)

        self.assertEqual(min(python_vol_calc.index), min(spark_vol_calc['datetime']))
        self.assertEqual(max(python_vol_calc.index), max(spark_vol_calc['datetime']))
        self.assertEqual(sum(python_vol_calc['volatility_estimation']), sum(spark_vol_calc['volatility_estimation']))
        
    def test_spark_vol_zma_mqch(self):
        exchange_str = 'binance'
        pair_str = 'adabrl'
        start_date_str = '2021-04-30T23:59:27.066000'
        end_date_str = '2021-05-01T00:01:27.066000'
        resample_rule_str='100mS'
        lookback_seconds_int=60
        bandwidth_int=5
        annualized_volatility_bool=False
        log_price_bool=True
        mid_quote_change_bool = True

        python_vol_calc = zma_volatility_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                                    resample_rule_str, lookback_seconds_int, bandwidth_int,
                                                    annualized_volatility_bool, log_price_bool, mid_quote_change_bool)

        spark_vol_calc = spark_zma_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                              resample_rule_str, lookback_seconds_int, bandwidth_int,
                                              annualized_volatility_bool, log_price_bool, mid_quote_change_bool)

        self.assertEqual(min(python_vol_calc.index), min(spark_vol_calc['datetime']))
        self.assertEqual(max(python_vol_calc.index), max(spark_vol_calc['datetime']))
        self.assertEqual(sum(python_vol_calc['volatility_estimation']), sum(spark_vol_calc['volatility_estimation']))
        
        resample_rule_str='1S'
        lookback_seconds_int=120
        bandwidth_int=15
        annualized_volatility_bool=True
        log_price_bool=False
        python_vol_calc = zma_volatility_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                                    resample_rule_str, lookback_seconds_int, bandwidth_int,
                                                    annualized_volatility_bool, log_price_bool, mid_quote_change_bool)

        spark_vol_calc = spark_zma_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                              resample_rule_str, lookback_seconds_int, bandwidth_int,
                                              annualized_volatility_bool, log_price_bool, mid_quote_change_bool)

        self.assertEqual(min(python_vol_calc.index), min(spark_vol_calc['datetime']))
        self.assertEqual(max(python_vol_calc.index), max(spark_vol_calc['datetime']))
        self.assertEqual(sum(python_vol_calc['volatility_estimation']), sum(spark_vol_calc['volatility_estimation']))

class SeasonalityTest(unittest.TestCase):
    def test_calc_seasonality(self):
        t = get_date_in_many_formats('2021-12-01T00:00:00.000000Z')
        trades_df = get_trades_dataframe('2021-11-01T00:00:00.000000Z', '2021-12-02T00:00:00.000000Z', 'binance', 'usdtbrl')

        trades_df = get_normalized_df(trades_df, '2021-11-01T00:00:00.000000Z', '2021-12-02T00:00:00.000000Z')
        trades_df = make_datetime_index(trades_df)
        volume_df = calc_volume(trades_df, '60S')
        volume_df['hour'] = volume_df.index.hour
        volume_df['minute'] = volume_df.index.minute

        result_seasonality = calc_seasonality(t, volume_df, 30)
        expected_seasonality = 24094.75

        self.assertEqual(result_seasonality, expected_seasonality)

    def test_spark_seasonality(self):
        exchange_str = 'binance'
        pair_str = 'usdtbrl'
        start_date_str = '2021-12-01T00:00:00.000000'
        end_date_str = '2021-12-01T00:10:00.000000'
        resample_rule_str = '60S'
        lookback_days = 30

        python_seas_calc = seasonality_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                                  resample_rule_str, lookback_days)

        spark_seas_calc = spark_seasonality_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                                                       resample_rule_str, lookback_days)

        self.assertEqual(min(python_seas_calc.index), min(spark_seas_calc['datetime']))
        self.assertEqual(max(python_seas_calc.index), max(spark_seas_calc['datetime']))
        self.assertEqual(sum(python_seas_calc['seasonality_estimation']), sum(spark_seas_calc['seasonality_estimation']))

if __name__ == '__main__':
    unittest.main()