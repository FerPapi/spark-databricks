import numpy as np
import pandas as pd
import datetime

from copy import deepcopy
from scipy.stats import norm

from propdesk_estimators.data_raw_transformation import make_datetime_index, drop_columns_df

from pandarallel import pandarallel
pandarallel.initialize()

def ZMA(prices_df, bandwidth: int = 5, annualized_volatility: bool = False, log_price: bool = True):
    """
    Two-scale realized variance
    Inputs:
        - prices_df: pd.dataframe
        - bandwith: int | size of sliding window
        - annualized_volatility: bool | use annualized volatility if True
    Output:
        - estimation: float
    """
    M = prices_df.shape[0]

    if log_price:
        prices_df = np.log(prices_df)

    returns = prices_df.diff().dropna()

    M_bar = (M - bandwidth + 1)/bandwidth
    gamma01 = (returns**2).sum()
    avg = 0

    for q in range(bandwidth):
        returns = prices_df.iloc[q::bandwidth].diff().dropna()
        avg += (returns**2).sum()

    avg = avg/bandwidth

    estimation = (avg - (M_bar/M) * gamma01) / (1 - M_bar/M)

    estimation = estimation / M

    if annualized_volatility:
        estimation = np.sqrt(estimation / ((prices_df.index[1] - prices_df.index[0]).total_seconds() / 3600) * 24 * 365) / prices_df.mean()

    return estimation

def calc_volatility(t_datetime, quotes_df, resample_rule_str, seconds_int, bandwidth_int, annualized_volatility_bool, log_price_bool, mid_quote_change_bool):
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
    Output:
        - volatility_estimation: single estimation for t_datetime
    """

    # get exactly the midquotes we need between t_datetime and lookback_seconds
    delta_t_datetime = t_datetime - datetime.timedelta(seconds=seconds_int)
    prices_df = quotes_df['mid_quote'][quotes_df['datetime'].between(delta_t_datetime, t_datetime)]
    # resampling mid quotes to compute volatility
    prices_df = resample_mid_quote(prices_df, resample_rule_str)

    if mid_quote_change_bool:
        prices_df = prices_df[prices_df.diff()!=0]

    if not prices_df.any():
        return np.NaN

    volatility_estimation = ZMA(prices_df, bandwidth_int, annualized_volatility_bool, log_price_bool)

    return volatility_estimation



def vpin(bucket_i, vpin_bucket_df, col, volume_bucket_size, n=50):

    n_buckets = bucket_i+n-1
    available_buckets = vpin_bucket_df.index.max()

    # we need at least 80% of specified number of buckets
    if n_buckets > available_buckets:
        if int(0.8*n_buckets) > available_buckets:
            return np.nan
        n_buckets = available_buckets-1

    window_df = vpin_bucket_df.loc[bucket_i: n_buckets]
    vpin = window_df[col].sum()/(n*volume_bucket_size)

    return vpin


def calc_vpin_df(trades_df, resampling_rule_str='15T', original_trades_sampling_rule='1T', n_buckets=50):
    '''
    trades_df must have columns volume and return per time bar
    '''

    if 'volume' not in trades_df.columns or 'return' not in trades_df.columns:
        raise Exception('Input trades df in vpin calculation must contain columns volume and return')

    vpin_df = deepcopy(trades_df)
    return_std = vpin_df['return'].std()

    # calculate volume bars (buckets) looking back on data
    volume_bucket_size = np.mean(vpin_df[['volume']].resample(resampling_rule_str).sum().values)

    vpin_df['buy_prob'] = norm(loc=0, scale=1).cdf(vpin_df['return']/return_std)
    vpin_df['sell_prob'] = 1 - vpin_df['buy_prob']
    vpin_df['buy_volume'] = vpin_df['volume']*vpin_df['buy_prob']
    vpin_df['sell_volume'] = vpin_df['volume']*vpin_df['sell_prob']

    cumm_volume = 0
    bucket = 0
    bucket_array = []

    for volume in vpin_df.volume:
        cumm_volume += volume
        bucket_array.append(bucket)
        if cumm_volume >= volume_bucket_size:
            bucket += 1
            cumm_volume = 0
        if bucket > n_buckets:
            break

    vpin_df = vpin_df.iloc[:len(bucket_array)]
    vpin_df['bucket'] = bucket_array

    vpin_bucket_df = vpin_df[['volume', 'buy_volume', 'sell_volume', 'bucket']].groupby(by=['bucket']).sum()

    vpin_bucket_df['abs_order_imbalance'] = np.abs(vpin_bucket_df['buy_volume'] - vpin_bucket_df['sell_volume'])
    vpin_bucket_df['order_imbalance'] = vpin_bucket_df['buy_volume'] - vpin_bucket_df['sell_volume']

    vpin_bucket_df['datetime_end'] = vpin_df[['datetime', 'bucket']].groupby(by=['bucket']).max()
    vpin_bucket_df['datetime_start'] = vpin_df[['datetime', 'bucket']].groupby(by=['bucket']).min()

    vpin_bucket_df['bucket'] = vpin_bucket_df.index
    vpin_bucket_0 = vpin(0, vpin_bucket_df, 'abs_order_imbalance', volume_bucket_size, n=n_buckets)
    vpin_signed_bucket_0 = vpin(0, vpin_bucket_df, 'order_imbalance', volume_bucket_size, n=n_buckets)
    sign_vpin = np.sign(vpin_signed_bucket_0)

    return pd.Series([vpin_bucket_0, vpin_signed_bucket_0, sign_vpin])


def calc_vpin_out_of_sample_df(trades_df, window_days, real_time=False, resampling_rule_str='15T', original_trades_sampling_rule='1T', n_buckets=50):
    '''
    trades_df must have columns volume and return per time bar
    '''

    if 'volume' not in trades_df.columns or 'return' not in trades_df.columns:
        raise Exception('Input trades_df in vpin calculation must contain columns volume and return')

    if not trades_df.index.is_monotonic_increasing:
        raise Exception('Input trades_df in vpin calculation must be indexed by ascending timestamp')

    vpin_df = deepcopy(trades_df)[['volume', 'return']]

    if trades_df.index.is_monotonic_increasing:
        vpin_df = vpin_df.sort_index(ascending=False)

    vpin_df['datetime'] = vpin_df.index

    last = max(vpin_df.index)

    if real_time:
        real_time_vpin_df = deepcopy(vpin_df.iloc[[0]])
        real_time_vpin_df[['vpin', 'vpin_signed', 'sign_vpin']] = real_time_vpin_df['datetime'].apply(lambda x: calc_vpin_df(vpin_df.loc[x:str(pd.to_datetime(last)-datetime.timedelta(days=window_days))]))
        return real_time_vpin_df[['vpin', 'vpin_signed', 'sign_vpin']]

    vpin_df[['vpin', 'vpin_signed', 'sign_vpin']] = vpin_df['datetime'].parallel_apply(lambda x: calc_vpin_df(vpin_df.loc[x:str(pd.to_datetime(x)-datetime.timedelta(days=window_days))]))
    return vpin_df[['vpin', 'vpin_signed', 'sign_vpin']].sort_index(ascending=True)
