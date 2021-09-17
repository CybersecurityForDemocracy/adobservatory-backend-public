import datetime
import logging

SIX_HOURS_IN_SECONDS = datetime.timedelta(hours=6).seconds
ONE_HOUR_IN_SECONDS = datetime.timedelta(hours=1).seconds

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
