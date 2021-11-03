"""Ad Observatory API routes and methods specific to the API.
"""
import datetime
import decimal
import itertools
from operator import itemgetter

from flask import Blueprint, request, Response, abort
from flask_caching import Cache
import humanize
import numpy as np
import pandas as pd
import simplejson as json

import db_functions
from common import date_utils, cache

URL_PREFIX = '/api/v1'

blueprint = Blueprint('ad_observatory_api', __name__)

SPEND_ESTIMATE_OLDEST_DATE = datetime.date(year=2020, month=6, day=22)
TOTAL_SPEND_OLDEST_ALLOWED_DATE = datetime.date(year=2020, month=7, day=1)

OBSCURE_OBSERVATION_COUNT_AT_OR_BELOW = 5
OBSCURE_OBSERVATION_COUNT_MESSAGE = '%s or less' % OBSCURE_OBSERVATION_COUNT_AT_OR_BELOW

def get_default_end_date():
    # Spend from most recent 7 days is unreliable due to delay in Facebook accounting. So we only
    # report data starting more than 7 days ago.
    return datetime.date.today() - datetime.timedelta(days=7)

def parse_end_date_request_arg(end_date):
    if end_date:
        return date_utils.parse_date_arg(end_date)

    return get_default_end_date()

def get_aggregate_by_request_arg(request_args):
    if 'raw_page_id_query' in request.args:
        return (db_functions.AGGREGATE_BY_PAGE_ID if bool(request.args['raw_page_id_query'])
                else db_functions.AGGREGATE_BY_PAGE_OWNER)
    aggregate_by = request_args.get('aggregate_by', db_functions.AGGREGATE_BY_PAGE_OWNER)
    if aggregate_by not in db_functions.PAGE_AGGREGATION_MODES:
        abort(400, description='Unknown aggregate_by arg {}'.format(aggregate_by))
    return aggregate_by

def parse_time_span_arg(arg_str):
    """Parse request arg as a time span and provide number of days in it.

    Accepts one of: 'week'/'month'/'quarter'.

    Args:
        arg_str: str request arg to parse.
    Returns:
        int number of days in the time period.
    """
    arg_to_days = {'week': 7, 'month': 30, 'quarter': '120'}

    return arg_to_days.get(arg_str, 0)

def get_active_days_in_range(
        range_start, range_end, ad_delivery_start_time_series, last_active_date_series):
    """Calculate the days an ad was active in a given range.

    Args:
        range_start: datetime.date Start of period of interest
        range_end: datetime.date  End of period of interest
        ad_delivery_start_time: pandas.Series[datetime.date] The date an ad started serving
        last_active_date: pandas.Series[datetime.date] The date the ad was last active
    Returns:
        pandas.Series[int] of days ads were active in the range of interest
    """
    def active_days_in_range(row):
        """Get days active in range. If ad_delivery_start_time -> last_active_date range does not
        overlap with range_start -> range_end, return 0.
        """
        ad_delivery_start_time = row['ad_delivery_start_time']
        last_active_date = row['last_active_date']
        if ad_delivery_start_time > range_end and last_active_date > range_end:
            return 0
        if ad_delivery_start_time < range_start and last_active_date < range_start:
            return 0
        min_date_in_range = min(max(ad_delivery_start_time, range_start), range_end)
        max_date_in_range = min(max(last_active_date, range_start), range_end)
        # add 1 to timedelta.days because we include both ad_delivery_start_time and last_active_day
        return (max_date_in_range - min_date_in_range).days + 1

    dates = pd.DataFrame(
            {'ad_delivery_start_time': ad_delivery_start_time_series,
             'last_active_date': last_active_date_series})

    days_in_range = dates.apply(active_days_in_range, axis=1)
    return days_in_range.replace([np.inf, -np.inf], 0).fillna(0)

def get_spend_per_day(ad_spend_records):
    """Calculate average spend per day over the life of an ad in a dataframe."""
    timedeltas = ad_spend_records['last_active_date'].sub(
        ad_spend_records['ad_delivery_start_time'])
    # add 1 to timedelta.days because we include both ad_delivery_start_time and last_active_day
    timedeltas = timedeltas.apply(lambda x: float(max(x.days + 1, 0)))
    # Divide by zero below is treated as +inf which we replace with 0
    spends = ad_spend_records['spend'].astype('float').div(timedeltas)
    return spends.replace([np.inf, -np.inf], 0).fillna(0)

@cache.global_cache.memoize()
def generate_time_periods(max_date, min_date, span_in_days=7):
    """Generate list of datetime.date span_in_days apart [max_date, min_date). Starting at max_date
    and working backwards.

    Args:
        max_date: datetime.date latest date from which to work backwards from. Included in list.
        min_date: datetime.date date which list should not pass. Only included in list if it occurs
            exactly N weeks from max_date.
        span_in_days: int number of days each span should be
    Returns:
        list of datetime.dates starting with max_date and all dates 7 days apart after that until
        min_date.
    """
    def date_n_days_ago(days):
        return max_date - datetime.timedelta(days=days)
    return list(
        itertools.takewhile(
            lambda x: x >= min_date, map(date_n_days_ago, range(0, 365, span_in_days))))

@blueprint.route('/total_spend/by_page/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_top_spenders_for_region(region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-02'),
                          oldest_allowed_date=TOTAL_SPEND_OLDEST_ALLOWED_DATE)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)

    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)

        results = db_interface.get_spender_for_region(region_name, start_date, end_date,
                                                      aggregate_by)
    response_data = json.dumps({'spenders': results.results,
                       'region_name': region_name,
                       'start_date': results.start_date.isoformat(),
                       'end_date': results.end_date.isoformat(),
                      })
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

@blueprint.route('/pages/<int:page_id>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_page_data(page_id):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        page_data = db_interface.get_page_data(page_id)
        if page_data:
            page_data['owned_pages'] = db_interface.owned_pages(page_id)

    if page_data:
        return Response(json.dumps(page_data), mimetype='application/json')
    return Response(status=404, mimetype='application/json')

@blueprint.route('/total_spend/of_page/<int:page_id>/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_total_spending_by_spender_in_region_since_date(page_id, region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-02'),
                                oldest_allowed_date=TOTAL_SPEND_OLDEST_ALLOWED_DATE)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)
    if not start_date:
        abort(400)

    respone_data = None

    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        page_owner = db_interface.page_owner(page_id)
        results = db_interface.page_spend_in_region_since_date(
            page_id, region_name, start_date, end_date, aggregate_by)
        owned_pages = db_interface.owned_pages(page_id)

    if results:
        page_name = results.results[0]['page_name']
        # TODO(macpd): remove this once FE uses /pages/<int:page_id> to get owned page IDs
        results.results[0]['page_ids'] = owned_pages 
        response_data = json.dumps(
            {'start_date': results.start_date.isoformat(),
             'end_date': results.end_date.isoformat(),
             'page_id': page_id,
             'page_owner': page_owner,
             'page_name': page_name,
             'region_name': region_name,
             'spenders': results.results})

    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

@blueprint.route('/spend_by_time_period/of_page/<int:page_id>/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_spending_by_week_by_spender_of_region(page_id, region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-01'),
                                oldest_allowed_date=SPEND_ESTIMATE_OLDEST_DATE)
    if not start_date:
        abort(400)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)
    response_data = spending_by_week_by_spender_of_region(page_id, region_name,
                                                                 start_date, end_date, aggregate_by)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def spending_by_week_by_spender_of_region(page_id, region_name, start_date, end_date,
                                              aggregate_by):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        if not end_date:
            end_date = db_interface.page_and_region_latest_last_7_days_report_date(
                page_id, region_name, aggregate_by)
            if not end_date:
                return None
        weeks = generate_time_periods(
            max_date=end_date, min_date=start_date, span_in_days=7)
        page_spend_by_week = db_interface.page_spend_in_region_by_week(
            page_id, region_name, weeks=weeks, aggregate_by=aggregate_by)
        if aggregate_by == db_functions.AGGREGATE_BY_PAGE_OWNER:
            page_name = db_interface.page_owner_page_name(page_id)
        else:
            page_name = db_interface.page_name(page_id)

    if not page_spend_by_week:
        return None

    spend_by_week = []
    disclaimers = set()
    for row in page_spend_by_week:
        spend_by_week.append(
            {'week': row['report_date'].isoformat(), 'spend': row['spend']})
        disclaimers.update(set(row['disclaimers']))

    # Fill in missing time periods with spend of 0
    all_weeks_isoformat = {week.isoformat() for week in weeks if week < datetime.date.today()}
    weeks_with_spend = {row['week'] for row in spend_by_week}
    weeks_without_spend = all_weeks_isoformat - weeks_with_spend
    for week in weeks_without_spend:
        spend_by_week.append({'week': week, 'spend': 0.0})
    spend_by_week.sort(key=lambda x: x.get('week'))

    return json.dumps(
        {'time_unit': 'week',
         'date_range': [min(weeks).isoformat(), max(weeks).isoformat()],
         'page_id': page_id,
         'spend_by_week': spend_by_week,
         'region_name': region_name,
         'page_name': page_name,
         'disclaimers': list(disclaimers)})

def discount_spend_outside_daterange(start_date, end_date, ad_spend_records):
    for ad_spend in ad_spend_records:
        ad_delivery_start_time = ad_spend['ad_delivery_start_time']
        last_active_date = ad_spend['last_active_date']
        if ad_delivery_start_time >= start_date and last_active_date <= end_date:
            # ad active only active within timerange of concern. No discounting required.
            continue

        days_in_range = (
            min(last_active_date, end_date) - max(start_date, ad_delivery_start_time)).days
        days_ad_active = (last_active_date  - ad_delivery_start_time).days

        if days_ad_active == 0:
            days_ad_active = 1

        if ad_spend['spend'] is None:
            ad_spend['spend'] = 0
        else:
            ad_spend['spend'] = ad_spend['spend'] * days_in_range/days_ad_active
    return ad_spend_records

@blueprint.route('/total_spend/by_page/of_topic/<path:topic_name>/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_spenders_for_topic_in_region(topic_name, region_name):
    record_count = int(request.args.get('count', '10'))
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-22'),
                                oldest_allowed_date=TOTAL_SPEND_OLDEST_ALLOWED_DATE)
    if not start_date:
        abort(400)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)
    response_data = spenders_for_topic_in_region(
        topic_name, region_name, start_date, end_date, aggregate_by, max_records=record_count)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def spenders_for_topic_in_region(topic_name, region_name, start_date, end_date, aggregate_by,
                                 max_records=None):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        topics = db_interface.topics()
        topic_id = topics.get(topic_name, -1)
        if topic_id < 0:
            abort(404)
        ad_spend_records = db_interface.total_spend_by_page_of_topic_in_region(
            region_name, start_date, end_date, topic_id, aggregate_by)
    if ad_spend_records is None:
        return None
    discounted_spend_records = discount_spend_outside_daterange(start_date, end_date,
                                                                ad_spend_records)
    spend_data = pd.DataFrame.from_records(
        discounted_spend_records, exclude=['last_active_date', 'ad_delivery_start_time'])
    spend_data = spend_data.groupby('page_id', as_index=False).agg(
        {'page_name':'min', 'spend':'sum'})
    sorted_spend_data = spend_data.sort_values(by='spend', ascending=False)

    if max_records:
        sorted_spend_data = sorted_spend_data.head(max_records)
    return json.dumps({
        'spenders': sorted_spend_data.to_dict('records'),
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'topic_name': topic_name,
        'region_name': region_name})

@blueprint.route('/total_spend/by_topic/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_top_topics_in_region(region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-22'),
                                oldest_allowed_date=TOTAL_SPEND_OLDEST_ALLOWED_DATE)
    if not start_date:
        abort(400)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    response_data = top_topics_in_region(region_name, start_date, end_date)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def top_topics_in_region(region_name, start_date, end_date, max_records=None):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        topic_map = db_interface.topic_id_to_name_map()
        ad_spend_records = db_interface.get_spend_for_topics_in_region(region_name, start_date,
                                                                       end_date)
    if not ad_spend_records:
        return None
    discounted_spend_records = discount_spend_outside_daterange(start_date, end_date,
                                                                ad_spend_records)
    spend_data = pd.DataFrame.from_records(
        discounted_spend_records, exclude=['last_active_date', 'ad_delivery_start_time'])
    spend_data = spend_data.groupby('topic_id', as_index=False).agg({'spend':'sum'})
    spend_data['topic_name'] = spend_data['topic_id'].map(topic_map)
    spend_data = spend_data.drop(columns=['topic_id'])
    sorted_spend_data = spend_data.sort_values(by='spend', ascending=False)

    if max_records:
        sorted_spend_data = sorted_spend_data.head(max_records)
    return json.dumps({
        'spend_by_topic': sorted_spend_data.to_dict('records'),
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'region_name': region_name})

@blueprint.route('/spend_by_time_period/of_topic/<path:topic_name>/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_spend_by_week_for_topic(topic_name, region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-22'),
                                oldest_allowed_date=SPEND_ESTIMATE_OLDEST_DATE)
    time_period_unit = request.args.get('time_unit', 'week')
    time_period_length = parse_time_span_arg(time_period_unit)
    if not start_date or not time_period_length:
        abort(400)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    response_data = spend_by_week_for_topic(topic_name, region_name, start_date, end_date,
                                                   time_period_unit, time_period_length)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def spend_by_week_for_topic(topic_name, region_name, start_date, end_date, time_period_unit,
                                time_period_length):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        topics = db_interface.topics()
        ad_spend_records = db_interface.total_spend_of_topic_in_region(
            region_name, start_date, end_date, topics[topic_name])
    if ad_spend_records is None:
        return None

    periods = generate_time_periods(
        max_date=end_date, min_date=start_date, span_in_days=time_period_length)

    ad_spend_data = pd.DataFrame.from_records(ad_spend_records)

    # Clean up None values
    ad_spend_data['last_active_date'] = ad_spend_data['last_active_date'].apply(
        lambda x: x if x else datetime.date.today()+datetime.timedelta(days=1))

    #  ad_spend_data['spend_per_day'] = get_spend_per_day(ad_spend_data)
    for i in range(len(periods)-1):
        period_end_date = periods[i]
        period_start_date = periods[i+1] + datetime.timedelta(days=1)
        ad_spend_data[period_end_date] = (
            ad_spend_data['spend_per_day'] * get_active_days_in_range(
                period_start_date, period_end_date,
                ad_spend_data['ad_delivery_start_time'],
                ad_spend_data['last_active_date']))
    ad_spend_data = ad_spend_data.drop(columns=[
        'last_active_date',
        'ad_delivery_start_time',
        'spend_per_day',
        'spend'])
    spend_in_timeperiod = {}
    for date, amount in ad_spend_data.sum(axis=0).to_dict().items():
        spend_in_timeperiod[date.isoformat()] = int(amount)
    result = {
        'spend_in_timeperiod': spend_in_timeperiod,
        'time_unit': time_period_unit,
        'start_date': min(periods).isoformat(),
        'end_date': max(periods).isoformat(),
        'topic_name': topic_name,
        'region_name': region_name,}

    return json.dumps(result)

def assign_spend_to_timewindows(weeks_list, grouping_name, spend_query_result):
    result = {}
    #setup week windows
    day_to_week_window = {}
    counter = 0
    for week in weeks_list:
        week_days = pd.date_range(start=week, periods=7)
        for date in week_days:
            day_to_week_window[date.date()] = counter

        counter += 1


    for row in spend_query_result:
        grouping = row[grouping_name]
        spend_start = row['start_day']
        spend_end = row['end_day']
        spend = 0
        if row['spend']:
            spend = row['spend']

        run_days = spend_end - spend_start
        run_days = max(run_days.days + 1, 1)

        spend_per_day = decimal.Decimal(spend / run_days)
        date_list = pd.date_range(start=spend_start, end=spend_end)
        for spend_day in date_list:
            if spend_day.date() in day_to_week_window:
                spend_week = weeks_list[day_to_week_window[spend_day.date()]]
                if grouping in result:
                    week_found = False
                    for tps in result[grouping]:
                        if spend_week.isoformat() == tps['time_period']:
                            tps['spend'] += spend_per_day
                            week_found = True

                    if not week_found:
                        result[grouping].append(
                            {'time_period':spend_week.isoformat(), 'spend':spend_per_day})

                else:
                    time_period_dict = {'time_period':spend_week.isoformat(),
                                        'spend':spend_per_day}
                    result[grouping] = [time_period_dict]

    # Fill in time periods where spend is not present in query results. Exclude today's date since
    # our pipeline does not yet include data collected today.
    week_list_isoformat = {week.isoformat() for week in weeks_list if week < datetime.date.today()}
    for grouping in result:
        missing_weeks = week_list_isoformat - {row['time_period'] for row in result[grouping]}
        result[grouping].extend([{'time_period': week, 'spend': 0} for week in missing_weeks])
        result[grouping].sort(key=lambda x: x.get('time_period'))
    return result

@blueprint.route('/spend_by_time_period/by_topic/of_page/<int:page_id>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_spend_by_time_period_by_topic_of_page(page_id):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-23'),
                                oldest_allowed_date=SPEND_ESTIMATE_OLDEST_DATE)
    if not start_date:
        abort(400)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)
    response_data = spend_by_time_period_by_topic_of_page(page_id, start_date, end_date,
                                                                 aggregate_by)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def spend_by_time_period_by_topic_of_page(page_id, start_date, end_date, aggregate_by):
    #TODO(LAE):read this parameter instead of hardcoding when this can be tested
    #time_unit = request.args.get('time_unit', 'week')
    time_unit = 'week'

    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        page_spend_over_time = db_interface.page_spend_by_topic_since_date(
            page_id, start_date, end_date, aggregate_by)
        if aggregate_by == db_functions.AGGREGATE_BY_PAGE_OWNER:
            page_name = db_interface.page_owner_page_name(page_id)
        else:
            page_name = db_interface.page_name(page_id)
    if not page_spend_over_time:
        return None

    weeks = generate_time_periods(max_date=end_date, min_date=start_date, span_in_days=7)

    spend_by_time_period = assign_spend_to_timewindows(weeks, 'topic_name', page_spend_over_time)

    return json.dumps(
        {'time_unit': time_unit,
         'date_range': [min(weeks).isoformat(), max(weeks).isoformat()],
         'page_id': page_id,
         'page_name': page_name,
         'spend_by_time_period': spend_by_time_period})

@blueprint.route('/spend_by_time_period/by_topic/of_page/<int:page_id>/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_spend_by_time_period_by_topic_of_page_in_region(page_id, region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-23'),
                                oldest_allowed_date=SPEND_ESTIMATE_OLDEST_DATE)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)

    if not start_date:
        abort(400)
    response_data = spend_by_time_period_by_topic_of_page_in_region(page_id, region_name,
                                                                    start_date, end_date,
                                                                    aggregate_by)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def spend_by_time_period_by_topic_of_page_in_region(page_id, region_name, start_date, end_date,
                                                    aggregate_by):
    #TODO(LAE):read this parameter instead of hardcoding when this can be tested
    #time_unit = request.args.get('time_unit', 'week')
    time_unit = 'week'

    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        page_spend_over_time = db_interface.spend_by_topic_of_page_in_region(
            page_id, region_name, start_date, end_date, aggregate_by)

        if aggregate_by == db_functions.AGGREGATE_BY_PAGE_OWNER:
            page_name = db_interface.page_owner_page_name(page_id)
        else:
            page_name = db_interface.page_name(page_id)
    if not page_spend_over_time:
        return None

    # Get max end_day from results.
    max_end_day = max(map(itemgetter('end_day'), page_spend_over_time))
    max_end_day = min(max_end_day, end_date)

    weeks = generate_time_periods(max_date=max_end_day, min_date=start_date,
                                  span_in_days=7)

    spend_by_time_period = assign_spend_to_timewindows(weeks, 'topic_name', page_spend_over_time)

    return json.dumps(
        {'time_unit': time_unit,
         'date_range': [min(weeks).isoformat(), max(weeks).isoformat()],
         'page_id': page_id,
         'page_name': page_name,
         'region_name': region_name,
         'spend_by_time_period': spend_by_time_period})

@blueprint.route('/spend_by_time_period/by_topic/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_spend_by_time_period_by_topic_of_region(region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-23'),
                                oldest_allowed_date=SPEND_ESTIMATE_OLDEST_DATE)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))

    if not start_date:
        abort(400)
    response_data = spend_by_time_period_by_topic_of_region(region_name, start_date, end_date)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def spend_by_time_period_by_topic_of_region(region_name, start_date, end_date):
    #TODO(LAE):read this parameter instead of hardcoding when this can be tested
    #time_unit = request.args.get('time_unit', 'week')
    time_unit = 'week'

    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        region_spend_over_time = db_interface.spend_by_topic_in_region(
            region_name, start_date, end_date)
    if not region_spend_over_time:
        return None

    # Get max end_day from results.
    max_end_day = max(map(itemgetter('end_day'), region_spend_over_time))
    max_end_day = min(max_end_day, end_date)

    weeks = generate_time_periods(max_date=max_end_day, min_date=start_date, span_in_days=7)

    spend_by_time_period = assign_spend_to_timewindows(weeks, 'topic_name', region_spend_over_time)

    return json.dumps(
        {'time_unit': time_unit,
         'date_range': [min(weeks).isoformat(), max(weeks).isoformat()],
         'region_name': region_name,
         'spend_by_time_period': spend_by_time_period})

@blueprint.route('/total_spend/by_purpose/of_page/<int:page_id>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_total_spend_by_purpose_of_page(page_id):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-23'),
                                oldest_allowed_date=TOTAL_SPEND_OLDEST_ALLOWED_DATE)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)

    if not start_date:
        abort(400)
    response_data = total_spend_by_purpose_of_page(page_id, start_date, end_date,
                                                          aggregate_by)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def total_spend_by_purpose_of_page(page_id, start_date, end_date, aggregate_by):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        total_page_spend_by_type = db_interface.total_page_spend_by_type(page_id, start_date,
                                                                         end_date, aggregate_by)
        if aggregate_by == db_functions.AGGREGATE_BY_PAGE_OWNER:
            page_name = db_interface.page_owner_page_name(page_id)
        else:
            page_name = db_interface.page_name(page_id)

    return json.dumps(
        {'start_date': start_date.isoformat(),
         'end_date': end_date.isoformat(),
         'page_id': page_id,
         'page_name': page_name,
         'spend_by_purpose': total_page_spend_by_type})

@blueprint.route('/total_spend/by_purpose/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_total_spend_by_purpose_of_region(region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-23'),
                                oldest_allowed_date=TOTAL_SPEND_OLDEST_ALLOWED_DATE)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))

    if not start_date:
        abort(400)
    response_data = total_spend_by_purpose_of_region(region_name, start_date, end_date)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def total_spend_by_purpose_of_region(region_name, start_date, end_date):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        total_spend_by_type_in_region = db_interface.total_spend_by_type_in_region(
            region_name, start_date, end_date)

    return json.dumps(
        {'start_date': start_date.isoformat(),
         'end_date': end_date.isoformat(),
         'region_name': region_name,
         'spend_by_purpose': total_spend_by_type_in_region})

@blueprint.route('/total_spend/by_purpose/of_page/<int:page_id>/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_total_spend_by_purpose_of_page_of_region(page_id, region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-23'),
                                oldest_allowed_date=TOTAL_SPEND_OLDEST_ALLOWED_DATE)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)

    if not start_date:
        abort(400)
    response_data = total_spend_by_purpose_of_page_of_region(
        page_id, region_name, start_date, end_date, aggregate_by)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def total_spend_by_purpose_of_page_of_region(page_id, region_name, start_date, end_date,
                                                    aggregate_by):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        total_page_spend_by_type = db_interface.total_spend_by_purpose_of_page_of_region(
                page_id, region_name, start_date, end_date, aggregate_by)
        page_name = db_interface.page_owner_page_name(page_id)

    return json.dumps(
        {'start_date': start_date.isoformat(),
         'end_date': end_date.isoformat(),
         'page_id': page_id,
         'page_name': page_name,
         'region_name': region_name,
         'spend_by_purpose': total_page_spend_by_type})

@blueprint.route('/spend_by_time_period/by_purpose/of_page/<int:page_id>/of_region/<region_name>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_spend_by_time_period_by_purpose_of_page_in_region(page_id, region_name):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-23'),
                                oldest_allowed_date=SPEND_ESTIMATE_OLDEST_DATE)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)

    if not start_date:
        abort(400)
    response_data = spend_by_time_period_by_purpose_of_page_in_region(
        page_id, region_name, start_date, end_date, aggregate_by)

    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def spend_by_time_period_by_purpose_of_page_in_region(page_id, region_name, start_date,
                                                             end_date, aggregate_by):
    #TODO(LAE):read this parameter instead of hardcoding when this can be tested
    #time_unit = request.args.get('time_unit', 'week')
    time_unit = 'week'

    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        page_spend_over_time = db_interface.spend_by_purpose_of_page_in_region(
            page_id, region_name, start_date, end_date, aggregate_by)
        if aggregate_by == db_functions.AGGREGATE_BY_PAGE_OWNER:
            page_name = db_interface.page_owner_page_name(page_id)
        else:
            page_name = db_interface.page_name(page_id)
    if not page_spend_over_time:
        return None

    weeks = generate_time_periods(max_date=end_date, min_date=start_date, span_in_days=7)

    spend_by_time_period = assign_spend_to_timewindows(weeks, 'purpose', page_spend_over_time)

    return json.dumps(
        {'time_unit': time_unit,
         'date_range': [min(weeks).isoformat(), max(weeks).isoformat()],
         'page_id': page_id,
         'page_name': page_name,
         'region_name': region_name,
         'spend_by_time_period': spend_by_time_period})

@blueprint.route('/total_spend/of_page/<int:page_id>/by_region')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_total_spend_of_page_by_region(page_id):
    start_date = date_utils.parse_date_arg(request.args.get('start_date', '2020-06-23'),
                                oldest_allowed_date=TOTAL_SPEND_OLDEST_ALLOWED_DATE)
    if not start_date:
        abort(400)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)
    response_data = total_spend_of_page_by_region(page_id, start_date, end_date,
                                                         aggregate_by)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def total_spend_of_page_by_region(page_id, start_date, end_date, aggregate_by):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        results = db_interface.page_spend_by_region_since_date(
            page_id, start_date, end_date, aggregate_by)
        if not results:
            return None
        if aggregate_by == db_functions.AGGREGATE_BY_PAGE_OWNER:
            page_name = db_interface.page_owner_page_name(page_id)
        else:
            page_name = db_interface.page_name(page_id)

    return json.dumps(
        {'start_date': results.start_date.isoformat(),
         'end_date': results.end_date.isoformat(),
         'page_id': page_id,
         'page_name': page_name,
         'spend_by_region': results.results})


def obscure_too_low_count_or_convert_count_to_humanized_int(rows):
    """Obscures or converts "count" value in each row.

    Values <= OBSCURE_OBSERVATION_COUNT_AT_OR_BELOW are obscured with 'N or less', and other values
    are converted to humanized int with commas (ie "N,NNN") or int in works (N million)."""
    for row in rows:
        if 'count' in row:
            count = row['count']
            if count <= OBSCURE_OBSERVATION_COUNT_AT_OR_BELOW:
                row['count'] = OBSCURE_OBSERVATION_COUNT_MESSAGE
            else:
                try:
                    row['count'] = humanize.intcomma(count)
                except OverflowError:
                    row['count'] = humanize.intword(count)


@blueprint.route('/targeting/of_page/<int:page_id>')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_targeting_category_counts_for_page(page_id):
    start_date =date_utils. parse_date_arg(request.args.get('start_date', '2020-06-22'),
                                oldest_allowed_date=TOTAL_SPEND_OLDEST_ALLOWED_DATE)
    if not start_date:
        abort(400)
    end_date = parse_end_date_request_arg(request.args.get('end_date', None))
    aggregate_by = get_aggregate_by_request_arg(request.args)
    response_data = targeting_category_counts_for_page(page_id, start_date, end_date,
                                                              aggregate_by)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def targeting_category_counts_for_page(page_id, start_date, end_date, aggregate_by):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        targeting_category_count_records = db_interface.get_targeting_category_counts_for_page(
            page_id, start_date, end_date, aggregate_by)
        if aggregate_by == db_functions.AGGREGATE_BY_PAGE_OWNER:
            page_name = db_interface.page_owner_page_name(page_id)
        else:
            page_name = db_interface.page_name(page_id)
    if targeting_category_count_records is None:
        return None
    obscure_too_low_count_or_convert_count_to_humanized_int(targeting_category_count_records)

    return json.dumps({
        'targeting': targeting_category_count_records,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
         'page_name': page_name,
        'page_id': page_id})

@blueprint.route('/race_pages')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def race_pages():
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        data = {row['race_id']: row['page_ids'] for row in db_interface.race_pages()}
    return Response(json.dumps(data), mimetype='application/json')

@blueprint.route('/race/<race_id>/candidates')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_candidates_in_race(race_id):
    response_data = candidates_in_race(race_id)
    if not response_data:
        return Response(status=204, mimetype='application/json')
    return Response(response_data, mimetype='application/json')

def candidates_in_race(race_id):
    data = {'race_id': race_id, 'candidates': []}
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        candidates_info = db_interface.candidates_in_race(race_id)
        if not candidates_info:
            return None
        for row in candidates_info:
            pages_info = db_interface.owned_page_info(row['page_owner'])
            if pages_info:
                # TODO(macpd): add open secrets ID
                data['candidates'].append({'pages': pages_info, 'short_name': row['short_name'],
                                           'full_name': row['full_name'], 'party': row['party']})
    return json.dumps(data)


@blueprint.route('/races')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_races():
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        data = json.dumps({row['state']: list(filter(None, row['races']))
                           for row in db_interface.state_races()})
    return Response(data, mimetype='application/json')

@blueprint.route('/missed_ads')
@cache.global_cache.cached(query_string=True, response_filter=cache.cache_if_response_no_server_error,
              timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_missed_ads():
    country = request.args.get('country', 'US')
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        data = json.dumps(list(db_interface.missed_ads(country)))
    return Response(data, mimetype='application/json')
