import argparse
import datetime
import json
import tempfile
import time
import warnings
from copy import deepcopy
warnings.filterwarnings("ignore")  # Ignore warnings coming from Arrow optimizations.

from pandarallel import pandarallel as pl
pl.initialize()

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, lit

from propdesk_estimators.data_extraction import get_quotes_dataframe

from propdesk_estimators.data_raw_transformation import make_datetime_index
from propdesk_estimators.data_raw_transformation import resample_mid_quote
from propdesk_estimators.data_upload import upload_dataframe_daily

from propdesk_estimators.estimators import ZMA

from propdesk_estimators.utils import get_date_in_many_formats
from propdesk_estimators.utils import get_dates_dataframe
from propdesk_estimators.utils import get_start_end_date_and_str

from propdesk_azure_services.azure_databricks import run_job


def weekly_handler(params_dict, live=True):
    job_id = params_dict['job_id']
    
    start_date_datetime, end_date_datetime, start_date, end_date = get_start_end_date_and_str(period='last_week', string_format='%Y-%m-%d')
    
    for d in get_dates_dataframe(start_date, end_date, '1D', inclusive=True)['datetime']:
        run_start_date = d
        run_end_date = d + datetime.timedelta(days=1)

        run_start_date_str = run_start_date.strftime("%Y-%m-%d")
        run_end_date_str = run_end_date.strftime("%Y-%m-%d")

        daily_params_dict = deepcopy(params_dict)
        daily_params_dict['start_date'] = run_start_date_str
        daily_params_dict['end_date'] = run_end_date_str
        daily_params_dict['live'] = live
        if live:
            run_job(job_id, daily_params_dict)
        else:
            print(job_id, daily_params_dict)

def daily_handler(params_dict, live=True):
    job_id = params_dict['job_id']

    start_date_datetime, end_date_datetime, start_date, end_date = get_start_end_date_and_str(period='yesterday', string_format='%Y-%m-%d')

    for d in get_dates_dataframe(start_date, end_date, '1D', inclusive=False)['datetime']:
        run_start_date = d
        run_end_date = d + datetime.timedelta(days=1)

        run_start_date_str = run_start_date.strftime("%Y-%m-%d")
        run_end_date_str = run_end_date.strftime("%Y-%m-%d")

        daily_params_dict = deepcopy(params_dict)
        daily_params_dict['start_date'] = run_start_date_str
        daily_params_dict['end_date'] = run_end_date_str
        daily_params_dict['live'] = live
        if live:
            run_job(job_id, daily_params_dict)
        else:
            print(job_id, daily_params_dict)

def hourly_handler(params_dict, live=True):
    job_id = params_dict['job_id']
    start_date_datetime, end_date_datetime, start_date, end_date = get_start_end_date_and_str(period='last_hour')

    daily_params_dict = deepcopy(params_dict)
    daily_params_dict['start_date'] = start_date
    daily_params_dict['end_date'] = end_date
    daily_params_dict['live'] = live
    if live:
        run_job(job_id, daily_params_dict)
    else:
        print(job_id, daily_params_dict)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-params", "--params", type=str, help="pass a dict of params as a json string", default='{}')
    parser.add_argument("-not_live", "--not_live", help='do not submit jobs (live by default). For debugging', action='store_false')

    args = parser.parse_args()

    live = args.not_live
    params_dict = json.loads(args.params)
    
    type_run = params_dict.get('type_run', '')

    if type_run == 'weekly':
        weekly_handler(params_dict, live)
    elif type_run == 'daily':
        daily_handler(params_dict, live)
    elif type_run == 'hourly':
        hourly_handler(params_dict, live)