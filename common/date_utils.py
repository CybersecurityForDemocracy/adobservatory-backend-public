import datetime
import logging
import itertools

import simplejson as json

from common import caching

SIX_HOURS_IN_SECONDS = int(datetime.timedelta(hours=6).total_seconds())
ONE_HOUR_IN_SECONDS = int(datetime.timedelta(hours=1).total_seconds())
ONE_DAY_IN_SECONDS = int(datetime.timedelta(days=1).total_seconds())

def parse_date_arg(arg_str, oldest_allowed_date=None, max_days_since_now=None):
    """Parse request arg as date with limits on oldest allowed date or max days since now.

    Accepts date format %Y-%m-%d.

    Args:
        arg_str: str request arg to parse.
        oldest_allowed_date: datetime.date of the oldest date that will be allowed. If parsed date
            is older than this, oldest_allowed_date is returned instead.
        max_days_since_now: int, max days since today to allow. If parsed date exceeds this limit
            now - max_days_since_now is returned.
    Returns:
        datetime.date of either parsed arg, oldest_allowed_date, or now - max_days_since_now.
        if arg cannot be parsed returns None.
    """
    try:
        parsed_date = datetime.datetime.strptime(arg_str, '%Y-%m-%d').date()
    except ValueError as error:
        logging.error('Unable to parse start_time arg. %s', error)
        return None

    if oldest_allowed_date and parsed_date < oldest_allowed_date:
        return oldest_allowed_date

    if max_days_since_now:
        max_days_since_now_date = (
            datetime.date.today() - datetime.timedelta(days=max_days_since_now))
        if parsed_date < max_days_since_now_date:
            return max_days_since_now_date

    return parsed_date

class DatetimeISOFormatJSONEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            if isinstance(o, (datetime.date, datetime.datetime)):
                return o.isoformat()
        except TypeError:
            pass
        return json.JSONEncoder.default(self, o)

@caching.global_cache.memoize()
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
