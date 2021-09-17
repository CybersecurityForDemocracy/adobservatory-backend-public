import datetime
import logging

import sqlalchemy as db
from sqlalchemy.dialects import postgresql
from memoization import cached
import memoization.caching.general.keys_order_independent as keys_toolkit_order_independent

from blueprints.google_dashboard import models


ONE_HOUR_IN_SECONDS = datetime.timedelta(hours=1).total_seconds()
ONE_DAY_IN_SECONDS = datetime.timedelta(days=1).total_seconds()

PAGE_SIZE = 30
row2dict = lambda r: {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}

POLITICAL_THRESHOLD = 0.65
JOB_THRESHOLD = 0.65

def js_date_string_to_datetime(date_str):
    # parse a JS datetime string (in approximately ISO format)
    # to a Python datetime, e.g. 2020-08-04T04:00:00.000Z
    #                            2021-03-03T20:51:06.415Z 
    return datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))

# TODO: needs transformation to use advertiser_name not advertiser_id.
# top_political_advertisers_since_date


def top_political_advertisers_since_date_cache_key_maker(
    session,
    start_date=datetime.date(1900, 1, 1),
    end_date=datetime.date.today(),
    region=None,
    page=1,
    page_size=PAGE_SIZE,
):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key(
        [start_date, end_date, region, page], []
    )


@cached(
    ttl=ONE_DAY_IN_SECONDS,
    custom_key_maker=top_political_advertisers_since_date_cache_key_maker,
)
def top_political_advertisers_since_date(
    session,
    start_date=datetime.date(1900, 1, 1),
    end_date=datetime.date.today(),
    region=None,
    page=1,
    page_size=PAGE_SIZE,
):
    """
    session: sqlalchemy session
    start_date: datetime.date or "YYYY-MM-DD"
    end_date: datetime.date or "YYYY-MM-DD"
    region: "US" or a state postal abbrev
    page: 1-indexed
    """
    if region == "US":
        query = session.query(models.AdvertiserStat.advertiser_name).join(
            models.AdvertiserWeeklySpend,
            models.AdvertiserStat.advertiser_id
            == models.AdvertiserWeeklySpend.advertiser_id,
        )
        query = query.filter(models.AdvertiserWeeklySpend.week_start_date >= start_date)
        query = query.filter(
            models.AdvertiserWeeklySpend.week_start_date
            + db.func.cast(
                db.sql.functions.concat(6, " days"), db.dialects.postgresql.INTERVAL
            )
            <= end_date
        )
        query = query.group_by(models.AdvertiserStat.advertiser_name,).add_columns(
            db.func.sum(models.AdvertiserWeeklySpend.spend_usd).label("spend_usd")
        )
        query = query.order_by(
            db.func.sum(models.AdvertiserWeeklySpend.spend_usd).desc()
        )

        effective_start_date = (
            session.query(db.func.min(models.AdvertiserWeeklySpend.week_start_date))
            .filter(models.AdvertiserWeeklySpend.week_start_date >= start_date)
            .one()[0]
        )
        effective_end_date = (
            session.query(
                db.func.max(models.AdvertiserWeeklySpend.week_start_date)
                + db.func.cast(
                    db.sql.functions.concat(6, " days"), db.dialects.postgresql.INTERVAL
                )
            )
            .filter(
                models.AdvertiserWeeklySpend.week_start_date
                + db.func.cast(
                    db.sql.functions.concat(6, " days"), db.dialects.postgresql.INTERVAL
                )
                <= end_date
            )
            .one()[0]
        )
    else:
        earliest_date_after_start_date = (
            session.query(db.func.min(models.AdvertiserRegionalSpend.report_date))
            .filter(models.AdvertiserRegionalSpend.report_date >= start_date)
            .subquery("earliest_date_after_start_date")
        )
        latest_date_before_end_date = (
            session.query(db.func.max(models.AdvertiserRegionalSpend.report_date))
            .filter(models.AdvertiserRegionalSpend.report_date <= end_date)
            .subquery("latest_date_before_end_date")
        )

        most_recent_totals_query = session.query(
            db.func.sum(models.AdvertiserRegionalSpend.spend_usd).label(
                "most_recent_spend_usd"
            ),
        )
        most_recent_totals_query = most_recent_totals_query.join(
            models.AdvertiserStat,
            models.AdvertiserRegionalSpend.advertiser_id
            == models.AdvertiserStat.advertiser_id,
        ).add_columns(models.AdvertiserStat.advertiser_name)
        most_recent_totals_query = most_recent_totals_query.filter(
            models.AdvertiserRegionalSpend.region == region
        )
        most_recent_totals_query = most_recent_totals_query.filter(
            models.AdvertiserRegionalSpend.report_date == latest_date_before_end_date
        )
        most_recent_totals_query = most_recent_totals_query.group_by(
            models.AdvertiserStat.advertiser_name,
        )
        most_recent_totals_query = most_recent_totals_query.subquery(
            "most_recent_query"
        )

        start_date_totals_query = session.query(
            db.func.sum(models.AdvertiserRegionalSpend.spend_usd).label(
                "start_date_spend_usd"
            ),
        )
        start_date_totals_query = start_date_totals_query.join(
            models.AdvertiserStat,
            models.AdvertiserRegionalSpend.advertiser_id
            == models.AdvertiserStat.advertiser_id,
        ).add_columns(models.AdvertiserStat.advertiser_name)
        start_date_totals_query = start_date_totals_query.filter(
            models.AdvertiserRegionalSpend.region == region
        )
        start_date_totals_query = start_date_totals_query.filter(
            models.AdvertiserRegionalSpend.report_date == earliest_date_after_start_date
        )
        start_date_totals_query = start_date_totals_query.group_by(
            models.AdvertiserStat.advertiser_name,
        )
        start_date_totals_query = start_date_totals_query.subquery("start_date_query")

        query = session.query(
            most_recent_totals_query.join(
                start_date_totals_query,
                most_recent_totals_query.c.advertiser_name
                == start_date_totals_query.c.advertiser_name,
            )
        ).filter(
            most_recent_totals_query.c.advertiser_name
            == start_date_totals_query.c.advertiser_name
        )  # repeating the join condition because SQLAlchemy otherwise ignores it?!
        query = query.order_by(
            (
                most_recent_totals_query.c.most_recent_spend_usd
                - start_date_totals_query.c.start_date_spend_usd
            ).desc()
        ).with_entities(
            most_recent_totals_query.c.advertiser_name,
            (
                most_recent_totals_query.c.most_recent_spend_usd
                - start_date_totals_query.c.start_date_spend_usd
            ).label("spend_usd"),
        )

        effective_start_date = session.query(earliest_date_after_start_date).one()[0]
        effective_end_date = session.query(latest_date_before_end_date).one()[0]
        # from sqlalchemy.dialects import postgresql
        # print(query.statement.compile(dialect=postgresql.dialect()))
    return (
        list(query.slice((page - 1) * page_size, page * page_size)),
        effective_start_date,
        effective_end_date,
    )


def top_ad_uploaders_since_date_cache_key_maker(
    session, start_date=None, end_date=None, page=1, page_size=PAGE_SIZE
):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key([start_date, end_date, page], [])


@cached(
    ttl=ONE_DAY_IN_SECONDS, custom_key_maker=top_ad_uploaders_since_date_cache_key_maker
)
def top_ad_uploaders_since_date(
    session, start_date=None, end_date=None, page=1, page_size=PAGE_SIZE
):
    """
    session: sqlalchemy session
    start_date: datetime.date or "YYYY-MM-DD"
    end_date: datetime.date or "YYYY-MM-DD"
    page: 1-indexed
    """
    query = session.query(
        models.YoutubeVideo.uploader,
        models.YoutubeVideo.uploader_id,
        db.func.count(db.distinct(models.ObservedYoutubeAd.observedat)),
    )
    query = query.join(
        models.YoutubeVideo,
        models.YoutubeVideo.id == models.ObservedYoutubeAd.platformitemid,
    )
    if start_date:
        query = query.filter(models.ObservedYoutubeAd.observedat > start_date)
    if end_date:
        query = query.filter(models.ObservedYoutubeAd.observedat < end_date)
    query = query.order_by(
        db.func.count(db.distinct(models.ObservedYoutubeAd.observedat)).desc()
    )
    query = query.group_by(
        models.YoutubeVideo.uploader, models.YoutubeVideo.uploader_id
    )
    return list(query.slice((page - 1) * page_size, page * page_size))


def spend_of_advertiser_by_week_cache_key_maker(
    session, advertiser_name, start_date=None, end_date=None
):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key(
        [advertiser_name, start_date, end_date], []
    )


def zeroes_for_weeks_since_date(most_recent_date, end_date=None):
    """
        for weeks where nothing was spent, the database does not record a row
        in order to return a zero for that week, we have to iterate over the weeks and fill in a zero

        most_recent_date is the most_recent_date FOR WHICH WE HAVE DATA
        end_date is the last date for which we want data (zeroes or real numbers)
    """
    data = []
    end_date = (end_date or datetime.date.today()) - datetime.timedelta(days=7)
    while most_recent_date < end_date:
        most_recent_date += datetime.timedelta(days=7)        
        data.append({"spend": 0, "week_start_date": most_recent_date})
    return data

@cached(
    ttl=ONE_DAY_IN_SECONDS, custom_key_maker=spend_of_advertiser_by_week_cache_key_maker
)
def spend_of_advertiser_by_week(
    session, advertiser_name, start_date=None, end_date=None
):
    start_date =  js_date_string_to_datetime(start_date) or datetime.date(1900, 1, 1)
    end_date = js_date_string_to_datetime(end_date) or datetime.date.today()
    query = session.query(
        db.func.sum(models.AdvertiserWeeklySpend.spend_usd).label("spend"),
        models.AdvertiserWeeklySpend.week_start_date,
    )
    query = query.filter(
        models.AdvertiserWeeklySpend.advertiser_name == advertiser_name
    )
    query = query.filter(models.AdvertiserWeeklySpend.week_start_date >= start_date)
    query = query.filter(models.AdvertiserWeeklySpend.week_start_date <= end_date)
    query = query.group_by(models.AdvertiserWeeklySpend.week_start_date)
    query = query.order_by(models.AdvertiserWeeklySpend.week_start_date)
    logging.info('spend_of_advertiser_by_week query: %s\n%s',
                 query.statement.compile(dialect=postgresql.dialect()), query.statement.compile().params)

    res = query.all()
    spend_by_week =  [{"spend": row[0], "week_start_date": row[1]} for row in res]
    zeroes_for_missing_dates = zeroes_for_weeks_since_date(max([row["week_start_date"] for row in spend_by_week]), end_date.date()) if len(spend_by_week) else []
    return {
        "total_spend": sum(row.spend for row in res),
        "spend_by_week": spend_by_week + zeroes_for_missing_dates,
        "start_date": min((row.week_start_date for row in res), default=start_date),
        "end_date": max((row.week_start_date for row in res), default=end_date)
        + datetime.timedelta(days=6),
    }


def spend_of_advertiser_id_by_week_cache_key_maker(
    session, advertiser_id, start_date=None, end_date=None
):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key(
        [advertiser_id, start_date, end_date], []
    )


@cached(
    ttl=ONE_DAY_IN_SECONDS,
    custom_key_maker=spend_of_advertiser_id_by_week_cache_key_maker,
)
def spend_of_advertiser_id_by_week(
    session, advertiser_id, start_date=None, end_date=None
):
    """
    this is distinct from spend_of_advertiser_by_week because it takes an advertiser_id (one advertiser may have two advertiser_ids.)
    """
    start_date =  js_date_string_to_datetime(start_date) or datetime.date(1900, 1, 1)
    end_date = js_date_string_to_datetime(end_date) or datetime.date.today()

    query = session.query(models.AdvertiserWeeklySpend)
    query = query.filter(models.AdvertiserWeeklySpend.advertiser_id == advertiser_id)
    query = query.filter(models.AdvertiserWeeklySpend.week_start_date >= start_date)
    query = query.filter(models.AdvertiserWeeklySpend.week_start_date <= end_date)
    query = query.order_by(models.AdvertiserWeeklySpend.week_start_date)

    res = query.all()
    return {
        "total_spend": sum(row.spend_usd for row in res),
        "spend_by_week": [{**row2dict(row), "spend": row.spend_usd} for row in res],
        "start_date": min((row.week_start_date for row in res), default=start_date),
        "end_date": max((row.week_start_date for row in res), default=end_date)
        + datetime.timedelta(days=6),
    }


def spend_of_advertiser_by_region_cache_key_maker(
    session, advertiser_name, start_date=None, end_date=None
):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key(
        [advertiser_name, start_date, end_date], []
    )


@cached(
    ttl=ONE_DAY_IN_SECONDS,
    custom_key_maker=spend_of_advertiser_by_region_cache_key_maker,
)
def spend_of_advertiser_by_region(
    session, advertiser_name, start_date=None, end_date=None
):
    """
    gets the total regional spend nearest to start_date and nearest to end_date, then subtracts the former from the latter.
    """
    start_date =  js_date_string_to_datetime(start_date) or datetime.date(1900, 1, 1)
    end_date = js_date_string_to_datetime(end_date) or datetime.date.today()

    earliest_date_after_start_date = session.query(
        db.func.min(models.AdvertiserRegionalSpend.report_date)
    ).filter(models.AdvertiserRegionalSpend.report_date >= start_date)
    earliest_date_after_start_date_sq = earliest_date_after_start_date.subquery(
        "earliest_date_after_start_date"
    )
    latest_date_before_end_date = session.query(
        db.func.max(models.AdvertiserRegionalSpend.report_date)
    ).filter(models.AdvertiserRegionalSpend.report_date <= end_date)
    latest_date_before_end_date_sq = latest_date_before_end_date.subquery(
        "latest_date_before_end_date"
    )

    most_recent_totals_query = session.query(
        models.AdvertiserRegionalSpend.region,
        db.func.sum(models.AdvertiserRegionalSpend.spend_usd).label(
            "most_recent_spend_usd"
        ),
    )
    most_recent_totals_query = most_recent_totals_query.join(
        models.AdvertiserStat,
        models.AdvertiserRegionalSpend.advertiser_id
        == models.AdvertiserStat.advertiser_id,
    ).add_columns(models.AdvertiserStat.advertiser_name)
    most_recent_totals_query = most_recent_totals_query.filter(
        models.AdvertiserStat.advertiser_name == advertiser_name
    )
    most_recent_totals_query = most_recent_totals_query.group_by(
        models.AdvertiserRegionalSpend.region,
        models.AdvertiserStat.advertiser_name,
    )
    most_recent_totals_query = most_recent_totals_query.filter(
        models.AdvertiserRegionalSpend.report_date == latest_date_before_end_date_sq
    )
    most_recent_totals_query = most_recent_totals_query.subquery("most_recent_query")

    start_date_totals_query = session.query(
        models.AdvertiserRegionalSpend.region,
        db.func.sum(models.AdvertiserRegionalSpend.spend_usd).label(
            "start_date_spend_usd"
        ),
    )
    start_date_totals_query = start_date_totals_query.join(
        models.AdvertiserStat,
        models.AdvertiserRegionalSpend.advertiser_id
        == models.AdvertiserStat.advertiser_id,
    ).add_columns(models.AdvertiserStat.advertiser_name)
    start_date_totals_query = start_date_totals_query.filter(
        models.AdvertiserStat.advertiser_name == advertiser_name
    )
    start_date_totals_query = start_date_totals_query.group_by(
        models.AdvertiserRegionalSpend.region,
        models.AdvertiserStat.advertiser_name,
    )
    start_date_totals_query = start_date_totals_query.filter(
        models.AdvertiserRegionalSpend.report_date == earliest_date_after_start_date_sq
    )
    start_date_totals_query = start_date_totals_query.subquery("start_date_query")

    query = (
        session.query(
            most_recent_totals_query.join(
                start_date_totals_query,
                most_recent_totals_query.c.region == start_date_totals_query.c.region,
            ).join(models.RegionPopulation, models.RegionPopulation.region_abbr == most_recent_totals_query.c.region)
        )
        .filter(
            most_recent_totals_query.c.region == start_date_totals_query.c.region)
        .filter(
            models.RegionPopulation.region_abbr == most_recent_totals_query.c.region
        )  # repeating the join condition because SQLAlchemy otherwise ignores it?!
        .add_columns(
            (
                most_recent_totals_query.c.most_recent_spend_usd
                - start_date_totals_query.c.start_date_spend_usd
            ).label("spend_usd"),
            (
                (most_recent_totals_query.c.most_recent_spend_usd
                                - start_date_totals_query.c.start_date_spend_usd) / (models.RegionPopulation.population.cast(db.Float) / 1000000)
            ).label("spend_usd_per_1m")            
        )
    )
    query = query.order_by(
        (
            most_recent_totals_query.c.most_recent_spend_usd
            - start_date_totals_query.c.start_date_spend_usd
        ).desc()
    )

    effective_start_date = earliest_date_after_start_date.one()[0]
    effective_end_date = latest_date_before_end_date.one()[0]

    logging.info('spend_of_advertiser_by_region query: %s\n%s',
                 query.statement.compile(dialect=postgresql.dialect()), query.statement.compile().params)
    spend_by_region = []
    for row in query.all():
        spend_by_region.append({"spend": row[9], "region": row[0], "spend_per_1m": int(row[10]) })

    return {
        "spend_by_region": spend_by_region,
        "advertiser_name": advertiser_name,
        "start_date": effective_start_date,
        "end_date": effective_end_date,
    }


def spend_of_advertiser_id_by_region_cache_key_maker(
    session, advertiser_id, start_date=None, end_date=None
):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key(
        [advertiser_id, start_date, end_date], []
    )


@cached(
    ttl=ONE_DAY_IN_SECONDS,
    custom_key_maker=spend_of_advertiser_id_by_region_cache_key_maker,
)
def spend_of_advertiser_id_by_region(
    session, advertiser_id, start_date=None, end_date=None
):
    """
    this is distinct from spend_of_advertiser_by_region because it takes an advertiser_id (one advertiser may have two advertiser_ids.)
    """
    start_date =  js_date_string_to_datetime(start_date) or datetime.date(1900, 1, 1)
    end_date = js_date_string_to_datetime(end_date) or datetime.date.today()

    earliest_date_after_start_date = session.query(
        db.func.min(models.AdvertiserRegionalSpend.report_date)
    ).filter(models.AdvertiserRegionalSpend.report_date >= start_date)
    earliest_date_after_start_date_sq = earliest_date_after_start_date.subquery(
        "earliest_date_after_start_date"
    )
    latest_date_before_end_date = session.query(
        db.func.max(models.AdvertiserRegionalSpend.report_date)
    ).filter(models.AdvertiserRegionalSpend.report_date <= end_date)
    latest_date_before_end_date_sq = latest_date_before_end_date.subquery(
        "latest_date_before_end_date"
    )

    most_recent_totals_query = session.query(
        models.AdvertiserRegionalSpend.region,
        models.AdvertiserRegionalSpend.advertiser_id,
        models.AdvertiserRegionalSpend.spend_usd.label("most_recent_spend_usd"),
    )
    most_recent_totals_query = most_recent_totals_query.filter(
        models.AdvertiserRegionalSpend.advertiser_id == advertiser_id
    )
    most_recent_totals_query = most_recent_totals_query.filter(
        models.AdvertiserRegionalSpend.report_date == latest_date_before_end_date_sq
    ).subquery("most_recent_query")

    start_date_totals_query = session.query(
        models.AdvertiserRegionalSpend.region,
        models.AdvertiserRegionalSpend.advertiser_id,
        models.AdvertiserRegionalSpend.spend_usd.label("start_date_spend_usd"),
    )
    start_date_totals_query = start_date_totals_query.filter(
        models.AdvertiserRegionalSpend.advertiser_id == advertiser_id
    )
    start_date_totals_query = start_date_totals_query.filter(
        models.AdvertiserRegionalSpend.report_date == earliest_date_after_start_date_sq
    ).subquery("start_date_query")

    query = (
        session.query(
            most_recent_totals_query.join(
                start_date_totals_query,
                most_recent_totals_query.c.region == start_date_totals_query.c.region,
            ),
            models.AdvertiserStat.advertiser_name,
        )
        .join(
            models.AdvertiserStat,
            models.AdvertiserStat.advertiser_id
            == start_date_totals_query.c.advertiser_id,
        )
        .add_columns(
            (
                most_recent_totals_query.c.most_recent_spend_usd
                - start_date_totals_query.c.start_date_spend_usd
            ).label("spend_usd")
        )
    )
    query = query.order_by(
        (
            most_recent_totals_query.c.most_recent_spend_usd
            - start_date_totals_query.c.start_date_spend_usd
        ).desc()
    )

    effective_start_date = earliest_date_after_start_date.one()[0]
    effective_end_date = latest_date_before_end_date.one()[0]

    spend_by_region = []
    advertiser_name = None
    for row in query.all():
        if not advertiser_name:
            advertiser_name = row[6]
        spend_by_region.append({"spend": row[7], "region": row[0]})

    return {
        "spend_by_region": spend_by_region,
        "advertiser_name": advertiser_name,
        "start_date": effective_start_date,
        "end_date": effective_end_date,
    }


def search_political_ads_cache_key_maker(
    session,
    querystring=None,
    advertiser_id=None,
    advertiser_name=None,
    start_date=None,
    end_date=None,
    page=1,
    page_size=PAGE_SIZE,
):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key(
        [
            querystring,
            advertiser_id,
            advertiser_name,
            start_date,
            end_date,
            page,
            page_size,
        ],
        [],
    )


@cached(ttl=ONE_DAY_IN_SECONDS, custom_key_maker=search_political_ads_cache_key_maker)
def search_political_ads(
    session,
    querystring=None,
    advertiser_id=None,
    advertiser_name=None,
    start_date=None,
    end_date=None,
    page=1,
    page_size=PAGE_SIZE,
):
    """
    session: sqlalchemy session
    querystring: postgresql FTS plainto_tsquery search query.
    advertiser_id: a Google advertiser ID, like AR534531769731383296
    start_date: datetime.date or "YYYY-MM-DD"
    end_date: datetime.date or "YYYY-MM-DD"
    page: 1-indexed

    TODO: add filter for disappearing ads.
    TODO: add filter for state/region (showing ads SHOWN in that state/region, not necessarily ONLY in that state/region)

    All arguments optional (except session).

    N.B. the date range search is complicated, because the ads are ranges, as is the specified date range.
    (we check that the ad's date range OVERLAPS the query range, not that ad-date-range IS CONTAINED BY the query range)
    """
    query = session.query(models.GoogleAdCreative)
    query = query.outerjoin(
        models.YoutubeVideo
    )  # outerjoin because we want text/image ads too!
    query = query.outerjoin(
        models.YoutubeVideoSub
    )  # outerjoin because we want YouTube ads that don't have subs too!
    query = query.join(models.CreativeStat)
    query = query.options(db.orm.joinedload(models.GoogleAdCreative.creative_stat))
    query = query.options(db.orm.joinedload(models.GoogleAdCreative.advertiser))
    query = query.options(db.orm.joinedload(models.GoogleAdCreative.youtube_video).joinedload(models.YoutubeVideo.youtube_video_sub))
    if advertiser_id:
        query = query.filter(models.GoogleAdCreative.advertiser_id == advertiser_id)
    if advertiser_name:
        query = query.join(models.AdvertiserStat)
        query = query.filter(models.AdvertiserStat.advertiser_name == advertiser_name)
    if start_date:
        query = query.filter(models.CreativeStat.last_served_timestamp >= start_date)
    if end_date:
        query = query.filter(models.CreativeStat.first_served_timestamp <= end_date)
    if querystring:
        query = query.filter(
            db.literal_column(
                """( -- TODO: add searching of OCR'ed ad image data
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(ad_text, '')) ||
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(youtube_videos.title, '')) ||
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(youtube_videos.description, '')) ||
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(youtube_videos.uploader, '')) ||
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(youtube_video_subs.subs, ''))
     )"""
            ).op("@@")(db.func.plainto_tsquery("english", querystring))
        )
    query = query.order_by(
        models.CreativeStat.last_served_timestamp.desc(), models.GoogleAdCreative.ad_id
    )
    # from sqlalchemy.dialects import postgresql
    # print(query.statement.compile(dialect=postgresql.dialect()))

    return list(query.slice((page - 1) * page_size, page * page_size))


def search_observed_video_ads_cache_key_maker(
    session,
    querystring=None,
    uploader_id=None,
    start_date=None,
    end_date=None,
    targeting=None,
    observed_only=False,
    political_only=False,
    missing_ads_only=False,
    page=1,
    page_size=PAGE_SIZE,
):
    """ignore the session in caching"""
    if targeting is None:
        targeting = {}
    return keys_toolkit_order_independent.make_key(
        [
            querystring,
            uploader_id,
            start_date,
            end_date,
            targeting,
            observed_only,
            political_only,
            missing_ads_only,
            page,
            page_size,
        ],
        [],
    )


@cached(
    ttl=ONE_DAY_IN_SECONDS, custom_key_maker=search_observed_video_ads_cache_key_maker
)
def search_observed_video_ads(
    session,
    querystring=None,
    uploader_id=None,
    start_date=None,
    end_date=None,
    targeting=None,
    observed_only=False,
    political_only=False,
    missing_ads_only=False,
    page=1,
    page_size=PAGE_SIZE,
):  # returns YoutubeVideo, but only video ones.
    """
    session: sqlalchemy session
    querystring: postgresql FTS plainto_tsquery search query.
    uploader_id: a YouTube user ID, like "Google" or "UCGdbbgHYS1Azgci6UH8xl3w"
    start_date: datetime.date or "YYYY-MM-DD"
    end_date: datetime.date or "YYYY-MM-DD"
    targeting: TODO
    page: 1-indexed

    All arguments optional (except session).

    Note that this can return "false positive" results:

    if uploader_id is specified, the assumption is that the uploader_id is one-to-one with the advertiser_id. but that assumptoin may not hold, like when a digital consultant uploads ads for multiple candidates.
    """
    if targeting is None:
        targeting = {}

    query = session.query(models.YoutubeVideo)
    query = (
        query.join(models.ObservedYoutubeAd)
        if observed_only
        else query.outerjoin(models.ObservedYoutubeAd)
    ).filter(models.ObservedYoutubeAd.itemtype != "recommendedVideo")
    query = (
        query.join(models.GoogleAdCreative)
        if political_only
        else query.outerjoin(models.GoogleAdCreative)
    )
    query = (
        query.join(models.CreativeStat)
        if political_only
        else query.outerjoin(models.CreativeStat)
    )
    # timing:
    # with no joinedloads, took 0:00:01.826996
    # with only the observed_youtube_ad joinedload, took 0:00:12.363979
    # with only the google_ad_creative, creative_stat joinedload, took 0:00:03.253938
    # with only the google_ad_creative (and no creative_stat) joinedload, took took 0:00:03.558274
    # with both joinedloads:  took 0:00:15.434288
    # with the google_ad_creative, creative_stat joinedload and observed_youtube_ad subqueryload, took 0:00:09.017823
    query = query.options(
        db.orm.joinedload(models.YoutubeVideo.google_ad_creative).joinedload(
            models.GoogleAdCreative.creative_stat
        )
    )
    query = query.options(
        db.orm.subqueryload(models.YoutubeVideo.observed_youtube_ad)
    )  # subquery load is like twice as fast as joinedload.

    if querystring:
        query = query.filter(
            db.literal_column(
                """(
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(observed_youtube_ads.advertiser, '')) ||
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(youtube_videos.title, '')) ||
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(youtube_videos.description, '')) ||
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(youtube_videos.uploader, '')) ||
       to_tsvector(CASE youtube_videos.subtitle_lang WHEN 'en' THEN 'english'::regconfig WHEN 'es' THEN 'spanish'::regconfig ELSE 'english'::regconfig END, coalesce(youtube_videos.subs, ''))
     )"""
            ).op("@@")(db.func.plainto_tsquery("english", querystring))
        )
    if uploader_id:
        query = query.filter(models.YoutubeVideo.uploader_id == uploader_id)
    if start_date:
        query = query.filter(models.ObservedYoutubeAd.observedat >= start_date)
    if end_date:
        query = query.filter(models.ObservedYoutubeAd.observedat <= end_date)
    if missing_ads_only:
        query = query.filter(models.GoogleAdCreative.youtube_ad_id == None)

    # TODO: targeting
    return list(query.slice((page - 1) * page_size, page * page_size))


def autocomplete_advertiser_name_cache_key_maker(session, advertiser_name_substr):
    return keys_toolkit_order_independent.make_key([advertiser_name_substr], [])


@cached(
    ttl=ONE_DAY_IN_SECONDS,
    custom_key_maker=autocomplete_advertiser_name_cache_key_maker,
)
def autocomplete_advertiser_name(session, advertiser_name_substr):
    query = session.query(
        models.AdvertiserStat.advertiser_name, models.AdvertiserStat.advertiser_id
    )
    query = query.filter(
        models.AdvertiserStat.advertiser_name.ilike(
            "%{}%".format(advertiser_name_substr)
        )
    )
    return list(query.slice(0, 50))


def get_uploader_ids_for_advertiser_cache_key_maker(session, advertiser_name):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key([advertiser_name], [])


@cached(
    ttl=ONE_DAY_IN_SECONDS,
    custom_key_maker=get_uploader_ids_for_advertiser_cache_key_maker,
)
def get_uploader_ids_for_advertiser(session, advertiser_name):
    """
    a political advertiser's video ads have an uploader (like a user id/user name for the account that uploaded the video).
    the assumption is that . this assumption may not hold, like when a digital consultant uploads ads for multiple candidates.

    this is a helper method for when the UI requests an advertiser_id's ads, we can return the uploader_ids that uploaded the advertiser_id's ads. (so we can later search for ads from those uploaders)
    """
    query = session.query(
        models.YoutubeVideo.uploader_id, models.YoutubeVideo.uploader
    ).distinct(models.YoutubeVideo.uploader_id, models.YoutubeVideo.uploader)
    query = query.join(models.GoogleAdCreative)
    query = query.join(models.AdvertiserStat)
    query = query.filter(models.AdvertiserStat.advertiser_name == advertiser_name)
    query = query.filter(models.YoutubeVideo.uploader != None)
    # from sqlalchemy.dialects import postgresql
    # print(query.statement.compile(dialect=postgresql.dialect()))

    return list(a[0] for a in query.all())


#  - see missed ads:
#  - ads that were marked political in a political transparency report, then disappeared
#  - ads that seem political but didn't appear in a report
#  - violating ads: A page for ads that have been removed by Google (but which we scraped beforehand)


def all_kinds_of_missed_ads_cache_key_maker(session, kind=None, page=1, page_size=PAGE_SIZE, advertiser_substring=None):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key([kind, page, page_size, advertiser_substring], [])


@cached(ttl=ONE_DAY_IN_SECONDS, custom_key_maker=all_kinds_of_missed_ads_cache_key_maker)
def all_kinds_of_missed_ads(session, kind=None, page=1, page_size=PAGE_SIZE, advertiser_substring=None):
    """
        there are four kinds of missed ads:

        - political missed ads from Ad Observer [youtube_video]
            (the OLD, unused way of calculating these is just with the classifier model; the new way relies on only showing classified-political ads from advertisers who've also done political ads on FB)
        - disappearing ads (likely a bug?)      [creative_stat, google_ad_creative, maybe youtube_video]
        - violative ads                         [creative_stat, google_ad_creative, maybe youtube_video]
        - ads from FB social issue advertisers  [youtube_video]

        this method returns a union of all of them (if possible?)

        we will have to do the union in Python? Or do the sorting on a temp table of IDs and dates?

        slice and dice (and/or sort) by date, advertiser, type of missed ad

        can't do state/region (missing from youtube_video) [is it worthwhile or confusing to have it for just violating/disappearing ads?]
        can't do money (missing from youtube_video)
        can't do keyword search (expensive? maybe doable)

        search options: advertiser_substring:  add advertiser name filtering to args (substring, so search for "Biden" returns Joe Biden the YouTube uploader, BIDEN FOR PRESIDENT the payer and Amanda Bidenstein the hypothetical rando state auditor candidate)
        TODO: make this a mat view
        TODO: add ads from FB social issue advertisers
        
    """
    query = all_kinds_of_missed_ads_ids_query(session, kind=kind, advertiser_substring=advertiser_substring)
    page_of_results = list(query.slice((page - 1) * page_size, page * page_size))


    # this is just a complicated way of getting the YouTubeVideo and GoogleAdCreative objects from the join above
    # without fetching each one with a separate request, but while retaining their sort order.
    missed_ad_result_sorts = {row[0]: i for i, row in enumerate(page_of_results) if row[2] == "missed"}
    nonmissed_ad_result_sorts = {row[0]: i for i, row in enumerate(page_of_results) if row[2] != "missed"}
    result_kinds = [row[2] for row in page_of_results]
    result_sort_keys = {}
    result_sort_keys.update(missed_ad_result_sorts)
    result_sort_keys.update(nonmissed_ad_result_sorts)
    missed_youtube_videos = session.query(models.YoutubeVideo).options(db.orm.joinedload(models.YoutubeVideo.values).options(db.orm.joinedload(models.InferenceValue.model))).filter(models.YoutubeVideo.id.in_(missed_ad_result_sorts.keys())).all()
    missed_google_ad_creatives = session.query(models.GoogleAdCreative).options(db.orm.joinedload(models.GoogleAdCreative.creative_stat)).options(db.orm.joinedload(models.GoogleAdCreative.advertiser)).filter(models.GoogleAdCreative.ad_id.in_(nonmissed_ad_result_sorts.keys())).all()

    def missed_ad_to_object(kind, obj):
        if kind in ["violative", "disappearing"]:
            return {
                "google_ad_creative": obj,
                "advertiser": obj.advertiser,
                "creative_stat": obj.creative_stat,
                "youtube_video": obj.youtube_video,
                "missed_ad_kind": kind
            }
        elif kind in ["missed", "issue_advertiser"]:
            return {
                "youtube_video": obj,
                "political_value": [value.value for value in sorted(obj.values, key=lambda val: val.model.created_at) if value.model.model_name == "politics"][0],
                "missed_ad_kind": kind
            }
        else:
            raise TypeError("unknown missed ad type `{}`".format(kind))

    sorted_missed_ad_objects = sorted(missed_youtube_videos + missed_google_ad_creatives, key=lambda obj: result_sort_keys[obj.id if hasattr(obj, 'id') else obj.ad_id] )
    return [(kind, missed_ad_to_object(kind, ad_obj)) for kind, ad_obj  in zip(result_kinds, sorted_missed_ad_objects)]


def all_kinds_of_missed_ads_ids_query(session, kind=None, advertiser_substring=None):
    """
    """
    with_fake_columns = lambda query: query.with_entities(db.literal_column("''").label('id'), db.literal_column("''").label('advertiser_name'), db.literal_column("''").label('kind'), db.literal_column("'2021-01-01'::timestamp").label('date'))
    disappearing = disappearing_ads_query(session, advertiser_substring=advertiser_substring).with_entities(
            models.CreativeStat.ad_id.label("id"),
            models.AdvertiserStat.advertiser_name,
            db.literal_column("'disappearing'").label("kind"),
            models.CreativeStat.first_served_timestamp.label("date")
        ) if (kind is None or kind == "disappearing")  else with_fake_columns(session.query(models.CreativeStat)).filter(db.sql.false())
    missed = political_seeming_missed_ads_query(session, advertiser_substring=advertiser_substring).with_entities(
            models.YoutubeVideo.id.label("id"),
            models.YoutubeVideo.uploader,
            db.literal_column("'missed'").label("kind"),
            models.YoutubeVideo.upload_date.label("date")
        ) if (kind is None or kind == "missed")  else with_fake_columns(session.query(models.YoutubeVideo)).filter(db.sql.false())
    violative = violating_ads_query(session, advertiser_substring=advertiser_substring, include_advertiser_name=True).with_entities(
            models.CreativeStat.ad_id.label("id"),
            models.AdvertiserStat.advertiser_name,
            db.literal_column("'violative'").label("kind"),
            models.CreativeStat.first_served_timestamp.label("date")
        ) if (kind is None or kind == "violative")  else with_fake_columns(session.query(models.CreativeStat)).filter(db.sql.false())
    query = disappearing.union_all(missed, violative)
    query = query.order_by(db.literal_column("date").desc())
    return query


@cached(ttl=ONE_DAY_IN_SECONDS)
def disappearing_ads_query(session, advertiser_substring=None):
    """
    factoring out the query for disappearing_ads(), disappearing_ads_counts() to avoid repetition. (see docs for those methods)
    """
    query = session.query(models.GoogleAdCreative)
    query = query.join(models.CreativeStat)
    query = query.join(models.AdvertiserStat)
    if advertiser_substring is not None: 
        query = query.filter(models.AdvertiserStat.advertiser_name.ilike(f'%{advertiser_substring}%'))    
    query = query.outerjoin(models.YoutubeVideo)
    query = query.filter(
        models.CreativeStat.report_date
        != session.query(db.func.max(models.CreativeStat.report_date)).subquery()
    )
    query = query.filter(models.GoogleAdCreative.policy_violation_date == None)
    return query


def disappearing_ads_cache_key_maker(session, advertiser_name=None, page=1, page_size=PAGE_SIZE):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key([advertiser_name, page, page_size], [])


@cached(ttl=ONE_DAY_IN_SECONDS, custom_key_maker=disappearing_ads_cache_key_maker)
def disappearing_ads(session, advertiser_name=None, page=1, page_size=PAGE_SIZE):
    """
    session: sqlalchemy session
    page: 1-indexed

    Oddly, some rows in the CreativeStats sheet appear one day, then disappear the next. Some of these are probably false-positives that aren't really political,
    but others are real political ads from candidates. This returns the GoogleAdCreative rows for them (so, the content, if we have it). (My def'n of
    a "disappearing ad" excludes those that are present in the archive, but with missing content, because Google says they violated a Google policy)
    """
    # returns GoogleAdCreative
    query = disappearing_ads_query(session)
    query = query.options(db.orm.joinedload(models.GoogleAdCreative.creative_stat))
    query = query.options(db.orm.joinedload(models.GoogleAdCreative.advertiser))
    if advertiser_name:
        query = query.filter(models.AdvertiserStat.advertiser_name == advertiser_name)
    query = query.order_by(models.CreativeStat.report_date)
    return list(query.slice((page - 1) * page_size, page * page_size))


def disappearing_ads_counts_cache_key_maker(session, page=1, page_size=PAGE_SIZE):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key([page, page_size], [])


@cached(
    ttl=ONE_DAY_IN_SECONDS, custom_key_maker=disappearing_ads_counts_cache_key_maker
)
def disappearing_ads_counts(session, page=1, page_size=PAGE_SIZE):
    """
    session: sqlalchemy session
    page: 1-indexed

    Count per advertiser of disappearing_ads()
    """
    query = disappearing_ads_query(session)
    query = query.with_entities(
        models.AdvertiserStat.advertiser_name,
        models.YoutubeVideo.uploader,
        db.func.count(),
        db.func.sum(models.CreativeStat.spend_range_min_usd)
    )
    query = query.group_by(
        models.AdvertiserStat.advertiser_name, models.YoutubeVideo.uploader
    )
    query = query.order_by(db.func.sum(models.CreativeStat.spend_range_min_usd).desc())
    return query.all()  # list(query.slice((page - 1) * page_size, page * page_size))


@cached(ttl=ONE_DAY_IN_SECONDS)
def disappearing_youtube_ads(session):
    """
    session: sqlalchemy session

    Oddly, some rows in the CreativeStats sheet appear one day, then disappear the next. Some of these are probably false-positives that aren't really political,
    but others are real political ads from candidates. This returns videos only -- and only videos that don't otherwise appear in the archive, so it's a political
    video ad that isn't visible in the archive.

    Note that this query, for some reason I don't understand, takes forever (hours!) with a LIMIT
    so we're not going to paginate it.
    """
    youtube_ads_query = session.query(
        models.GoogleAdCreative.youtube_ad_id, models.CreativeStat.ad_id
    )
    youtube_ads_query = youtube_ads_query.join(models.GoogleAdCreative)
    present_youtube_ads_query = youtube_ads_query.filter(
        models.CreativeStat.report_date
        == session.query(db.func.max(models.CreativeStat.report_date)).subquery()
    )
    removed_youtube_ads_query = youtube_ads_query.filter(
        models.CreativeStat.report_date
        != session.query(db.func.max(models.CreativeStat.report_date)).subquery()
    )
    present_youtube_ads_query = present_youtube_ads_query.subquery(
        "present_youtube_ads_query"
    )
    removed_youtube_ads_query = removed_youtube_ads_query.subquery(
        "removed_youtube_ads_query"
    )

    query = session.query(models.GoogleAdCreative)
    query = query.join(models.CreativeStat)

    query = query.join(
        removed_youtube_ads_query,
        models.GoogleAdCreative.youtube_ad_id
        == removed_youtube_ads_query.c.youtube_ad_id,
    )
    query = query.outerjoin(
        present_youtube_ads_query,
        models.GoogleAdCreative.youtube_ad_id
        == present_youtube_ads_query.c.youtube_ad_id,
    )
    query = query.filter(present_youtube_ads_query.c.ad_id == None)
    query = query.order_by(
        models.GoogleAdCreative.advertiser_id
    )  # not strictly necessary, but some wacky feature of Postgresql means that an unordered query with LIMIT 30 set takes ages and ages.

    query = query.options(db.orm.subqueryload(models.GoogleAdCreative.youtube_video))
    query = query.options(db.orm.subqueryload(models.GoogleAdCreative.creative_stat))
    query = query.options(db.orm.subqueryload(models.GoogleAdCreative.advertiser))
    # from sqlalchemy.dialects import postgresql
    # print(query.statement.compile(dialect=postgresql.dialect()))

    return query.all()


def political_seeming_missed_ads_query(session, threshold=POLITICAL_THRESHOLD, advertiser_substring=None):
    """
    As with Facebook, some video ads that appear to be political are observed by Ad Observer. These are videos whose subtitles/descriptions/etc. appear to be political,
    (and which are coded as such by the predictive model), but which didn't appear in the archive (i.e. don't appear in GoogleAdCreatives table.)

    To cut down on false positives, we join the (cleaned) advertiser names to FB advertiser names.

    """
    query = session.query(models.YoutubeVideo)
    query = (
        query.join(models.InferenceValue)
        .join(models.Model)
        .filter(
            models.Model.model_id
            == session.query(models.Model.model_id)
            .filter(models.Model.model_name == "politics")
            .order_by(models.Model.created_at.desc())
            .limit(1)
            .subquery()
        )
    )
    query = query.options(db.orm.joinedload(models.YoutubeVideo.values).options(db.orm.joinedload(models.InferenceValue.model)))

    query = query.outerjoin(models.GoogleAdCreative).filter(
        models.GoogleAdCreative.ad_id == None
    )
    if advertiser_substring is not None:
        query = query.filter(models.YoutubeVideo.uploader.ilike(f'%{advertiser_substring}%'))
    query = query.filter(
        models.InferenceValue.value > threshold
    )  # TODO: figure out what the right threshold is.

    def clean_advertiser_name_for_matching(name):
        return db.func.trim(db.func.replace(db.func.replace(db.func.replace(db.func.replace(db.func.replace(db.func.upper(name), ',', ''), '.', ''), ' LLC', ''), ' INC', ''), ' ', ''))


    # To cut down on false positives, we join the (cleaned) advertiser names to FB advertiser names
    uploaders_of_political_google_ads_sq = session.query(db.func.distinct(models.YoutubeVideo.uploader_id).label("uploader_id")) \
        .join(models.GoogleAdCreative).subquery()
    query_by_fb_disclaimer = query.join(
        models.LatestUsLifelongAdLibraryReportPage, clean_advertiser_name_for_matching(models.YoutubeVideo.uploader) == models.LatestUsLifelongAdLibraryReportPage.clean_disclaimer)
    # and also we join them to uploaders who have previously uploaded known-political ads to Google.
    query_by_google_uploader_name = query.join(
        uploaders_of_political_google_ads_sq, models.YoutubeVideo.uploader_id == uploaders_of_political_google_ads_sq.c.uploader_id
    )
    query = query_by_fb_disclaimer.union(query_by_google_uploader_name)
    return query

def political_seeming_missed_ads_cache_key_maker(
    session, page=1, page_size=PAGE_SIZE, threshold=POLITICAL_THRESHOLD
):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key([page, page_size, threshold], [])


@cached(
    ttl=ONE_DAY_IN_SECONDS,
    custom_key_maker=political_seeming_missed_ads_cache_key_maker,
)
def political_seeming_missed_ads(session, page=1, page_size=PAGE_SIZE, threshold=POLITICAL_THRESHOLD):
    """
    session: sqlalchemy session
    page: 1-indexed
    threshold: float, range from 0 to 1; the number spat out by the political-or-not regression model. the higher the number, the more confident the model is that the ad is political.

    As with Facebook, some video ads that appear to be political are observed by Ad Observer. These are videos whose subtitles/descriptions/etc. appear to be political,
    but which didn't appear in the archive (i.e. don't appear in GoogleAdCreatives table.)
    """
    query = political_seeming_missed_ads_query(session, threshold=threshold)
    query = query.order_by(models.InferenceValue.value.desc())
    # returns YoutubeVideo
    return list(query.slice((page - 1) * page_size, page * page_size))


def jobby_seeming_ads_cache_key_maker(
    session, page=1, page_size=PAGE_SIZE, threshold=JOB_THRESHOLD
):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key([page, page_size, threshold], [])


@cached(ttl=ONE_DAY_IN_SECONDS, custom_key_maker=jobby_seeming_ads_cache_key_maker)
def jobby_seeming_ads(session, page=1, page_size=PAGE_SIZE, threshold=JOB_THRESHOLD):
    """
    session: sqlalchemy session
    page: 1-indexed
    threshold: float, range from 0 to 1; the number spat out by the job-ad-or-not regression model. the higher the number, the more confident the model is that the ad is a job ad.

    Some ads are advertising job openings. This tries to find them.
    """
    query = session.query(models.YoutubeVideo)
    query = (
        query.join(models.InferenceValue)
        .join(models.Model)
        .filter(
            models.Model.model_id
            == session.query(models.Model.model_id)
            .filter(models.Model.model_name == "jobs")
            .order_by(models.Model.created_at.desc())
            .limit(1)
            .subquery()
        )
    )
    query = query.options(db.orm.joinedload(models.YoutubeVideo.values))
    query = query.filter(models.InferenceValue.value > threshold)
    query = query.order_by(models.InferenceValue.value.desc())
    # returns YoutubeVideo
    return list(query.slice((page - 1) * page_size, page * page_size))


def violating_ads_cache_key_maker(session, page=1, page_size=PAGE_SIZE):
    """ignore the session in caching"""
    return keys_toolkit_order_independent.make_key([page, page_size], [])

def violating_ads_query(session, include_advertiser_name=False, advertiser_name=None, advertiser_substring=None):
    """
        params:
            include_advertiser_name: whether or not to include the advertiser name in the response (True for all_missed_ads(), False for violating_ads())
            advertiser_name: exact match. Used on advertiser pages.
            advertiser_substring: Used in filtering on the all-missed-ads page, matches a substring with LIKE (so "Bide" matches "Joe Biden" or "Biden for President")

            at least one of advertiser_name and advertiser_substring must be None.
    """
    query = session.query(models.GoogleAdCreative)
    query = query.join(models.CreativeStat)
    if include_advertiser_name or advertiser_name or advertiser_substring:
        query = query.join(models.AdvertiserStat)
        if advertiser_name and advertiser_substring:
            raise TypeError("invalid arguments; at least one of advertiser_name and advertiser_substring must be None")
        if advertiser_name is not None:
            query = query.filter(models.AdvertiserStat.advertiser_name == advertiser_name)
        elif advertiser_substring is not None:
            query = query.filter(models.AdvertiserStat.advertiser_name.ilike(f'%{advertiser_substring}%'))
    query = query.outerjoin(models.YoutubeVideo) # this has to be an outerjoin because some violating ads aren't videos (and therefore don't join to the videos table)
    query = query.filter(models.GoogleAdCreative.policy_violation_date != None)
    query = query.filter(
        models.GoogleAdCreative.ad_type != "unknown"
    )  # exclude ads for which we don't have creative.
    query = query.filter(
        db.or_(
            models.GoogleAdCreative.ad_type != "video",
            db.not_(
                db.or_(
                    models.YoutubeVideo.video_unavailable,
                    models.YoutubeVideo.video_private,
                    models.YoutubeVideo.error,
                )
            ),
        )
    )
    return query


@cached(ttl=ONE_DAY_IN_SECONDS, custom_key_maker=violating_ads_cache_key_maker)
def violating_ads(session, page=1, page_size=PAGE_SIZE):
    """
    Some political ads appear in the archive, then disappear behind a violation screen. Those might be interesting!

    We only return the ones where we have some sort of creative, though.
    """
    # returns GoogleAdCreative
    query = violating_ads_query(session)
    query = query.options(db.orm.subqueryload(models.GoogleAdCreative.youtube_video))
    query = query.options(db.orm.subqueryload(models.GoogleAdCreative.advertiser))
    query = query.options(db.orm.subqueryload(models.GoogleAdCreative.creative_stat))
    query = query.order_by(models.GoogleAdCreative.policy_violation_date.desc())
    return list(query.slice((page - 1) * page_size, page * page_size))
