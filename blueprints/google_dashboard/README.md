# google-dashboard-api

an API for the Google/YouTube ads data on AdObservatory (adobservatory-fe repo).


## How to run it.

see root-level readme for ad_screener_backend.

## to test it:

`ad_screener_backend$ DATABASE_URL='postgres:///googleads' python -m unittest blueprints/google_dashboard/tests/advertiser_specific_queries.py`
