import datetime
import os
import pathlib
import pandas as pd
import re


def get_date_in_many_formats(datetime_value):
    date = None
    try:
        if "Z" in datetime_value:
            date = datetime.datetime.strptime(datetime_value, "%Y-%m-%dT%H:%M:%S.%fZ")
        elif "T" in datetime_value:
            date = datetime.datetime.strptime(datetime_value, "%Y-%m-%dT%H:%M:%S")
        elif ":" in datetime_value:
            date = datetime.datetime.strptime(datetime_value, "%Y%m%d_%H:%M:%S:%f")
        elif "_" in datetime_value:
            date = datetime.datetime.strptime(datetime_value, "%Y%m%d_%H%M%S%f")
        elif "-" in datetime_value:
            try:
                date = datetime.datetime.strptime(datetime_value, "%Y-%m-%d")
            except:
                date = datetime.datetime.strptime(datetime_value, "%Y-%m")
    except:
        try:
            # for arabic values
            date = datetime.datetime(*map(int, re.findall(r'\d+', datetime_value)))
        except Exception as e:
            date = None
    return date


def get_dates_dataframe(start_date_str, end_date_str, resampling_rule_str, inclusive=False):

    dates_df = pd.period_range(start=get_date_in_many_formats(start_date_str),
                               end=get_date_in_many_formats(end_date_str),
                               freq=resampling_rule_str, name='datetime').to_timestamp()

    dates_df = dates_df.to_frame()
    if not inclusive:
        dates_df = dates_df[dates_df.index < end_date_str]
 
    return dates_df



def get_start_end_date_and_str(period=None, start_date_str=None, end_date_str=None, max_days=None, timezone_offset=None, string_format='%Y-%m-%dT%H:%M:%S.%fZ'):
    start_date = None
    end_date = None
    if period:
        start_date, end_date = set_start_end_date_based_on_period(period)
    elif start_date_str and end_date_str:
        start_date = get_date_in_many_formats(start_date_str)
        end_date = get_date_in_many_formats(end_date_str)
    elif max_days and end_date_str:
        end_date = get_date_in_many_formats(end_date_str)
        start_date = end_date - datetime.timedelta(days=max_days)
    elif max_days:
        date_now = datetime.datetime.now()
        start_date = date_now - datetime.timedelta(days=max_days)
        end_date = date_now

    total_days_to_report = (end_date - start_date).days
    if max_days and total_days_to_report > max_days:
        start_date = end_date - datetime.timedelta(days=max_days)

    if timezone_offset:
        start_date = start_date - datetime.timedelta(minutes=timezone_offset)
        end_date = end_date - datetime.timedelta(minutes=timezone_offset)

    start_date_str = start_date.strftime(string_format)
    end_date_str = end_date.strftime(string_format)

    return start_date, end_date, start_date_str, end_date_str

        
def set_start_end_date_based_on_period(period):
    now = datetime.datetime.utcnow()
    if period == 'last_month':
        first_this_month = datetime.datetime(now.year, now.month, day=1)
        last_day_last_month = first_this_month - datetime.timedelta(seconds=1)
        first_day_last_month = datetime.datetime(last_day_last_month.year, last_day_last_month.month, day=1)
        return first_day_last_month, last_day_last_month

    elif period == 'this_month':
        first_this_month = datetime.datetime(now.year, now.month, day=1)
        return first_this_month, now

    elif period == 'current_month':
        first_this_month = datetime.datetime(now.year, now.month, day=1)
        next_month = now.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
        last_this_month = next_month - datetime.timedelta(days=next_month.day)
        return first_this_month, last_this_month

    elif period == 'last_week':
        one_week_ago = now - datetime.timedelta(days=7)
        first_day_last_week = one_week_ago - datetime.timedelta(days=one_week_ago.isoweekday() % 7)
        first_day_last_week = datetime.datetime(first_day_last_week.year, first_day_last_week.month, day=first_day_last_week.day)
        last_day_last_week = first_day_last_week + datetime.timedelta(days=7) - datetime.timedelta(seconds=1)
        return first_day_last_week, last_day_last_week

    elif period == 'last_two_weeks':
        two_weeks_ago = now - datetime.timedelta(days=14)
        first_day_last_week = two_weeks_ago - datetime.timedelta(days=two_weeks_ago.isoweekday() % 7)
        first_day_last_week = datetime.datetime(first_day_last_week.year, first_day_last_week.month, day=first_day_last_week.day)
        last_day_last_week = first_day_last_week + datetime.timedelta(days=14) - datetime.timedelta(seconds=1)
        return first_day_last_week, last_day_last_week

    elif period == 'this_week':
        first_day_this_week = now - datetime.timedelta(days=now.isoweekday() % 7)
        first_day_this_week = datetime.datetime(first_day_this_week.year, first_day_this_week.month, day=first_day_this_week.day)
        last_day_this_week = first_day_this_week + datetime.timedelta(days=7) - datetime.timedelta(seconds=1)
        return first_day_this_week, last_day_this_week

    elif period == 'this_year':
        first_this_year = datetime.datetime(now.year, month=1, day=1)
        return first_this_year, now

    elif period == 'last_year':
        first_this_year = datetime.datetime(now.year, month=1, day=1)
        last_day_last_year = first_this_year - datetime.timedelta(seconds=1)
        first_day_last_year = datetime.datetime(last_day_last_year.year, month=1, day=1)
        return first_day_last_year, last_day_last_year

    elif period == 'last_24_hours':
        yesterday = now - datetime.timedelta(days=7)
        return yesterday, now

    elif period == 'last_hour':
        round_hour_now = datetime.datetime(now.year, now.month, now.day, now.hour) - datetime.timedelta(days=1)
        round_hour_before = round_hour_now - datetime.timedelta(hours=1)
        return round_hour_before, round_hour_now

    elif period == 'yesterday':
        yesterday = now - datetime.timedelta(days=1)
        yesterday_midnight = datetime.datetime(yesterday.year, yesterday.month, yesterday.day)
        today_midnight = datetime.datetime(now.year, now.month, now.day)
        return yesterday_midnight, today_midnight

    else:
        return None, None


def remove_dir(directory):
    [os.remove(os.path.join(directory, f)) for f in os.listdir(directory)]
    os.rmdir(directory)
    return True

def create_recursive_path(basepath):
    path = pathlib.Path(basepath)
    path.parent.mkdir(parents=True, exist_ok=True)