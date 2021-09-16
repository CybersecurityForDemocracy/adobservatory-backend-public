import json
import datetime
import logging
from urllib.parse import unquote

from flask import (
    redirect,
    request,
    Blueprint,
    url_for,
    abort
)
from flask.json import JSONEncoder

import blueprints.google_dashboard.queries as queries

import db_functions
#  from blueprints.common import authentication

DEFAULT_PAGE_SIZE = "30"
MAX_PAGE_SIZE = 300
URL_PREFIX = "/ytapi/v1"
DEFAULT_START_DATE = "2021-01-07"

class CustomJSONEncoder(JSONEncoder):
    def default(self, o):
        try:
            if isinstance(o, (datetime.date, datetime.datetime)):
                return o.isoformat()
            iterable = iter(o)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, o)


google_dashboard_blueprint = Blueprint(
    "google_dashboard", __name__, template_folder="templates"
)

google_dashboard_blueprint.json_encoder = CustomJSONEncoder


# NOT YET DONE
#  - Regions: political ads targeted to a state/CBSA.. (only for political ads, based on the targeting in creative stats...),  search regions on search page (maybe)
#  - multiple ossoff problem (many Senate races have duplicate EIN/FEC ID pairs)
#  - design integration of Google ads data into the rest of the site


# REQUIRES ADDITIONAL PIPELINE STEPS
#  - OCR image ads
#  - transcribe unsubtitled video ads
#  - on-screen text from video ads
#  - search by topics
#  - show topics on advertiser pages

# THINGS I'VE CHOSEN NOT TO IMPLEMENT FOR NOW, BUT WHICH WE MIGHT RETHINK
# - advertiser-in-region pages.
#  - see top advertisers overall
#  - do we want to allow filtering/searching the violating/disappearing/missed ads? (checkboxes on search page, checkboxes on advertiser pages, links from disappeared ads counts to search page). I'm not sure that these lacunae are interesting in a per-candidate way... since they're a Google goof, as far as I can tell

@google_dashboard_blueprint.route("/top_political_advertisers/region/<region>")
def top_political_advertisers_since_date(region="US"):
    page = int(request.args.get("page", 1))
    page_size = min(int(request.args.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
    start_date = request.args.get("start_date", DEFAULT_START_DATE)
    end_date = request.args.get("end_date", datetime.date.today())
    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        (
            query_results,
            effective_start_date,
            effective_end_date,
        ) = queries.top_political_advertisers_since_date(
            session,
            start_date=start_date,
            end_date=end_date,
            region=region,
            page=page,
            page_size=page_size,
        )
        return {
            "political_advertisers": [
                {"advertiser_name": name, "spend": spend}
                for name, spend in query_results
            ],
            "region": region,
            "start_date": effective_start_date.isoformat(),
            "end_date": effective_end_date.isoformat(),
            "page": page,
        }


@google_dashboard_blueprint.route("/top_advertisers")
def top_advertisers_since_date():
    page = int(request.args.get("page", 1))
    page_size = min(int(request.args.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
    start_date = request.args.get("start_date", DEFAULT_START_DATE)
    end_date = request.args.get("end_date", datetime.date.today())
    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        query_results = queries.top_ad_uploaders_since_date(
            session,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
        )
        return {"result": query_results, "page": page}


flatten = lambda t: [item for sublist in t for item in sublist]

# e.g. http://localhost:5000/advertiser/AR227673879898750976/political_ads for Warnock
@google_dashboard_blueprint.route("/advertiser/<advertiser_name>/political_ads")
def advertiser_ads(advertiser_name):
    page = int(request.args.get("page", 1))
    page_size = min(int(request.args.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
    start_date = request.args.get("start_date", DEFAULT_START_DATE)
    end_date = request.args.get("end_date", None)
    advertiser_name = unquote(advertiser_name)

    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        political_ads = queries.search_political_ads(
            session,
            advertiser_name=advertiser_name,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
        )

        return {
            "political_ads": [
                {
                    "google_ad_creative": google_ad_creative,
                    "youtube_video": google_ad_creative.youtube_video,
                    "creative_stat": google_ad_creative.creative_stat,
                    "advertiser": google_ad_creative.advertiser,
                }
                for google_ad_creative in political_ads
            ],
        }


@google_dashboard_blueprint.route("/advertiser/<advertiser_name>/ads_by_same_uploader")
def advertiser_ads_by_same_uploader(advertiser_name):
    page = int(request.args.get("page", 1))
    page_size = min(int(request.args.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
    start_date = request.args.get("start_date", DEFAULT_START_DATE)
    advertiser_name = unquote(advertiser_name)

    end_date = request.args.get("end_date", None)
    missing_ads_only = request.args.get(
        "missing_ads_only", True
    )  # the reason we use this endpoint is to find ads from the same uploader that aren't in the archive (since those are maybe missed ads, or non-political ads by this advertiser)

    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        return {
            "ads_by_same_uploader": [
                {"youtube_video": yv}
                for yv in flatten(
                    queries.search_observed_video_ads(
                        session,
                        uploader_id=uploader_id,
                        start_date=start_date,
                        end_date=end_date,
                        missing_ads_only=True,
                    )
                    for uploader_id in queries.get_uploader_ids_for_advertiser(
                        session, advertiser_name
                    )
                )
            ]
        }


@google_dashboard_blueprint.route("/advertiser/<advertiser_name>/spend_by_week")
def spend_of_advertiser(advertiser_name):
    start_date = request.args.get("start_date", DEFAULT_START_DATE)
    end_date = request.args.get("end_date", None)
    advertiser_name = unquote(advertiser_name)

    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        spend = queries.spend_of_advertiser_by_week(
            session,
            advertiser_name,
            start_date=start_date,
            end_date=end_date,
        )

        return spend


@google_dashboard_blueprint.route("/advertiser/<advertiser_name>/spend_by_region")
def spend_of_advertiser_by_region(advertiser_name):
    start_date = request.args.get("start_date", DEFAULT_START_DATE)
    end_date = request.args.get("end_date", None)
    advertiser_name = unquote(advertiser_name)

    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        spend_by_region = queries.spend_of_advertiser_by_region(
            session,
            advertiser_name,
            start_date=start_date,
            end_date=end_date,
        )

        return spend_by_region


@google_dashboard_blueprint.route("/search")
def search():
    page = int(request.args.get("page", 1))
    page_size = min(int(request.args.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
    region = request.args.get(
        "region"
    )  # TODO: should this replace political_ads_in_state, political_ads_in_cbsa? (not implemented)
    if region:
        assert False, "region not yet implemented"
    start_date = request.args.get("start_date", DEFAULT_START_DATE)
    end_date = request.args.get("end_date", None)
    videos_only = request.args.get("videos_only", False)
    political_only = request.args.get("political_only", False)
    observed_videos_only = request.args.get("observed_only", False)
    ad_type = request.args.get("ad_type")  # text/video/image
    query = request.args.get("keyword")
    advertiser_name = json.loads(request.args.get("advertiser", '{"label": null}'))[
        "label"
    ]
    # there are three kinds of things you could search:
    # 1. ads (video, text, image) marked political by Google and contained in the ad archive (search_political_ads / GoogleAdCreative)
    # 2. videos we've collected, the union of observed ads and ads in the ad archive (search_observed_video_ads / YoutubeVideo)
    # 3. videos we've observed from AdObserver (search_observed_video_ads(observed_only=True) / YoutubeVideo)
    # 4. videos we've observed from AdObserver that Google considers political (search_observed_video_ads(observed_only=True, political_only=True).

    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        if not videos_only:
            return {
                "results": [
                    {
                        "google_ad_creative": google_ad_creative,
                        "advertiser": google_ad_creative.advertiser,
                        "creative_stat": google_ad_creative.creative_stat,
                        "youtube_video": google_ad_creative.youtube_video,
                    }
                    for google_ad_creative in queries.search_political_ads(
                        session,
                        querystring=query,
                        start_date=start_date,
                        end_date=end_date,
                        page=page,
                        page_size=page_size,
                        advertiser_name=advertiser_name,
                    )
                ]
                # advertiser_id=None kwarg is available, but not settable via the search box (for now?)
            }
        else:
            return {
                "results": [
                    {"youtube_video": yv}
                    for yv in queries.search_observed_video_ads(
                        session,
                        querystring=query,
                        start_date=start_date,
                        end_date=end_date,
                        targeting={},
                        observed_only=observed_videos_only,
                        political_only=political_only,
                        page=page,
                        page_size=page_size,
                    )
                ]
                # uploader_id=None kwarg is available, but not settable via the search box (for now?)
            }


@google_dashboard_blueprint.route("/advertiser_name_autocomplete")
def autocomplete_advertiser_name():
    advertiser_name_substr = request.args.get("advertiser_name_substr")
    if len(advertiser_name_substr) <= 2:
        return {"matches": []}
    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        return {
            "matches": queries.autocomplete_advertiser_name(
                session, advertiser_name_substr
            )
        }


# TODO should political_ads_in_state, political_ads_in_cbsa be replaced by search(region=______) ?
@google_dashboard_blueprint.route("/")
def political_ads_in_state():
    pass


@google_dashboard_blueprint.route("/")
def political_ads_in_cbsa():
    pass

@google_dashboard_blueprint.route("/all_missed_ads")
def all_missed_ads():
    page = int(request.args.get("page", 1))
    page_size = min(int(request.args.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
    kind = request.args.get("kind")
    if kind and kind not in ["missed", "violative", "disappearing", "issue_advertiser"]:
       abort(400, "unknown kind of missed ad `{}`".format(kind))

    advertiser_substring = request.args.get("advertiser_substring")

    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        return {
            "result": queries.all_kinds_of_missed_ads(session, page=page, page_size=page_size, kind=kind, advertiser_substring=advertiser_substring)
        }    

@google_dashboard_blueprint.route("/missed_ads")
def missed_ads():
    page = int(request.args.get("page", 1))
    page_size = min(int(request.args.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        return {
            "result": (
                {"youtube_video": yv, "political_value": [value.value for value in sorted(yv.values, key=lambda val: val.model.created_at) if value.model.model_name == "politics"][0]}
                for yv in queries.political_seeming_missed_ads(
                    session, page=page, page_size=page_size
                )
            )
        }


@google_dashboard_blueprint.route("/disappeared_ads")
def disappeared_ads():
    page = int(request.args.get("page", 1))
    page_size = min(int(request.args.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
    advertiser_name = request.args.get("advertiser_name", None)
    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        return {
            "disappeared_ads": [
                {
                    "google_ad_creative": google_ad_creative,
                    "advertiser": google_ad_creative.advertiser,
                    "creative_stat": google_ad_creative.creative_stat,
                    "youtube_video": google_ad_creative.youtube_video,
                }
                for google_ad_creative in queries.disappearing_ads(session, advertiser_name, page)
            ]
        }


@google_dashboard_blueprint.route("/disappeared_youtube_ads")
def disappeared_youtube_ads():
    # doesn't paginate for dumb reasons
    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        return {
            "disappeared_ads": [
                {
                    "google_ad_creative": google_ad_creative,
                    "advertiser": google_ad_creative.advertiser,
                    "creative_stat": google_ad_creative.creative_stat,
                    "youtube_video": google_ad_creative.youtube_video,
                }
                for google_ad_creative in queries.disappearing_youtube_ads(session)
            ]
        }


@google_dashboard_blueprint.route("/disappeared_ad_counts")
def disappeared_ad_counts():
    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        return {
            "disappeared_ad_counts": [
                {"advertiser_name": item[0], "count": item[2], "min_spend": item[3]}
                for item in queries.disappearing_ads_counts(session)
            ]
        }


@google_dashboard_blueprint.route("/violating_ads")
def violating_ads():
    # doesn't support paging because there are ~16 of these, so we should just do any pagination on the frontend.
    # TODO: support filtering to only the ones for which we have content to show
    page = int(request.args.get("page", 1))
    page_size = min(int(request.args.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
    with db_functions.get_ad_info_database_sqlalchemy_session() as session:
        return {
            "violating_ads": [
                {
                    "google_ad_creative": google_ad_creative,
                    "advertiser": google_ad_creative.advertiser,
                    "creative_stat": google_ad_creative.creative_stat,
                    "youtube_video": google_ad_creative.youtube_video,
                }
                for google_ad_creative in queries.violating_ads(
                    session,
                    page=page,
                    page_size=page_size,
                )
            ]
        }
