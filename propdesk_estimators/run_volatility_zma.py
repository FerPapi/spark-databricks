import argparse
import datetime
import numpy as np
import pandas as pd
import time

from pandarallel import pandarallel as pl
pl.initialize()

from propdesk_estimators.data_extraction import get_quotes_dataframe

from propdesk_estimators.data_raw_transformation import resample_mid_quote
from propdesk_estimators.data_raw_transformation import drop_columns_df
from propdesk_estimators.data_raw_transformation import make_datetime_index

# from propdesk_estimators.estimators import ZMA

from propdesk_estimators.utils import get_dates_dataframe

from propdesk_estimators.estimators import calc_volatility

def zma_volatility_estimation(exchange_str, pair_str, start_date_str, end_date_str,
                              resample_rule_str='100mS',
                              lookback_seconds_int=60, bandwidth_int=5,
                              annualized_volatility_bool=False, log_price_bool=True, mid_quote_change_bool=True,
                              live=False):

    quotes_df = get_quotes_dataframe(start_date_str, end_date_str, exchange_str, pair_str, lookback_seconds_int)
    dates_df = get_dates_dataframe(start_date_str, end_date_str, resample_rule_str)
    # we'll need datetime indexed df to resample
    quotes_df = make_datetime_index(quotes_df)
    volatility_estimation_df = make_datetime_index(dates_df)

    # calculate estimation
    volatility_estimation_df['volatility_estimation'] = volatility_estimation_df['datetime'].parallel_apply(lambda t: calc_volatility(t, quotes_df, resample_rule_str,
                                                                                                                                      lookback_seconds_int, bandwidth_int,
                                                                                                                                      annualized_volatility_bool, log_price_bool, mid_quote_change_bool))
    volatility_estimation_df = volatility_estimation_df.dropna()
    volatility_estimation_df = drop_columns_df(volatility_estimation_df, ['datetime'])

    return volatility_estimation_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # execution params - 1 letter
    parser.add_argument("-l", "--live", action='store_true', help='pass -l only if you want to make state changes')

    # general info - 2 letters
    parser.add_argument("-ex", "--exchange", help='exchange name to retrieve data from')
    parser.add_argument("-pa", "--pair", help='pair name, e.g., btcusdt')
    parser.add_argument("-sd", "--start_date", help='date to retrieve data from')
    parser.add_argument("-ed", "--end_date", help='date to retrieve data until')

    # data processing parameters - 3 letters
    parser.add_argument("-rsr", "--resampling_rule", help='pandas resampling rule, e.g., 100mS')
    parser.add_argument("-lbt", "--lookback_time", help='loookback time in seconds, e.g., 60 for 1 minute')

    # estimator specific params - 4 letters
    parser.add_argument("-band", "--bandwidth", help='bandwidth parameter of zma estimator')
    parser.add_argument("-annv", "--annualized_volatility", help='calculate volatility annualized', action='store_true')
    parser.add_argument("-logp", "--log_price", help='use log of prices', action='store_false')
    parser.add_argument("-mqch", "--mid_quote_change", help='filter mid quotes on change', action='store_false')

    args = parser.parse_args()

    live = args.live
    exchange_str = args.exchange
    pair_str = args.pair

    start_date_str = args.start_date
    end_date_str = args.end_date

    resampling_rule_str = args.resampling_rule
    lookback_seconds_int = int(args.lookback_time)

    bandwidth_int = int(args.bandwidth or 5)
    annualized_volatility = args.annualized_volatility
    log_price = args.log_price
    mid_quote_change = args.mid_quote_change

    volatility_estimation_df = zma_volatility_estimation(exchange_str,
        pair_str, start_date_str, end_date_str, resampling_rule_str,
        lookback_seconds_int, bandwidth_int, annualized_volatility, log_price, mid_quote_change, live)
