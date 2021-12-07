"""Main server entrypoint.

- Registers blueprints for Ad Screener and Ad Observatory API.
- Implements and registers login manager handlers for loading users and handling API keys.
- Other miscellaneous initializations.
"""
import logging
import os

from dotenv import load_dotenv
from flask import Flask
from flask_cors import CORS
from google.cloud import secretmanager
import google.cloud.logging
from google.cloud.logging_v2.handlers.handlers import EXCLUDED_LOGGER_DEFAULTS
from elasticsearch import Elasticsearch

from blueprints.ad_observatory_api import ad_observatory_api
from blueprints.common import ads_search
from blueprints.google_dashboard import blueprint as google_dashboard
from common.elastic_search import ElasticSearchApiParams
from common import caching, running_on_app_engine


def get_secret_value(secret_name, utf8_decode=True):
    client = secretmanager.SecretManagerServiceClient()
    resp = client.access_secret_version(request={'name': secret_name})
    if utf8_decode:
        return resp.payload.data.decode('UTF-8')
    return resp.payload.data


def init_server():
    if running_on_app_engine.running_on_app_engine():
        os.environ['FB_ADS_DATABASE_PASSWORD'] = get_secret_value(
            os.environ['FB_ADS_DATABASE_PASSWORD_SECRET_NAME'])
        os.environb[b'FLASK_APP_SECRET_KEY'] = get_secret_value(
            os.environ['FLASK_APP_SECRET_KEY_SECRET_NAME'], utf8_decode=False)
        os.environ['FB_ADS_ELASTIC_SEARCH_API_KEY'] = get_secret_value(
            os.environ['FB_ADS_ELASTIC_SEARCH_API_KEY_SECRET_NAME'])


    server = Flask(__name__)
    server.register_blueprint(ads_search.blueprint,
                                              url_prefix=ad_observatory_api.URL_PREFIX)
    server.register_blueprint(ad_observatory_api.blueprint,
                                              url_prefix=ad_observatory_api.URL_PREFIX)
    server.register_blueprint(
        google_dashboard.google_dashboard_blueprint,
        url_prefix=google_dashboard.URL_PREFIX,
    )
    caching.init_cache(server, cache_blueprint_url_prefix='/')

    if running_on_app_engine.running_on_app_engine():
        os.environ['GOOGLE_ADS_DATABASE_PASSWORD'] = get_secret_value(
            os.environ['GOOGLE_ADS_DATABASE_PASSWORD_SECRET_NAME'])
        os.environ['AD_OBSERVATORY_API_USER_DATABASE_PASSWORD'] = get_secret_value(
            os.environ['AD_OBSERVATORY_API_USER_DATABASE_PASSWORD_SECRET_NAME'])

    fb_ad_creative_gcs_bucket = os.environ['FB_AD_CREATIVE_GCS_BUCKET']
    server.config['FB_AD_CREATIVE_GCS_BUCKET'] = fb_ad_creative_gcs_bucket
    logging.info('Facebook ad creatives GCS bucket name: %s', fb_ad_creative_gcs_bucket)


    if 'DEBUG_CORS' in os.environ and os.environ['DEBUG_CORS']:
        logging.getLogger('flask_cors').level = logging.DEBUG
    # Get comman separate list as python list, removing empty entries
    cors_allowlist = list(filter(None, os.environ['CORS_ORIGINS_ALLOWLIST'].split(',')))
    logging.info('CORS origns allowlist: %r', cors_allowlist)
    CORS(server, origins=cors_allowlist)

    server.config['REMEMBER_COOKIE_SECURE'] = True
    server.secret_key = os.environb[b'FLASK_APP_SECRET_KEY']

    server.config['FB_ADS_ELASTIC_SEARCH_API_PARAMS'] = ElasticSearchApiParams(
        client=Elasticsearch(cloud_id=os.environ['FB_ADS_ELASTIC_SEARCH_CLOUD_ID'],
                             api_key=(os.environ['FB_ADS_ELASTIC_SEARCH_API_ID'],
                                      os.environ['FB_ADS_ELASTIC_SEARCH_API_KEY'])),
        api_id=os.environ['FB_ADS_ELASTIC_SEARCH_API_ID'],
        api_key=os.environ['FB_ADS_ELASTIC_SEARCH_API_KEY'],
        fb_pages_index_name=os.environ['FB_ADS_ELASTIC_SEARCH_FB_PAGES_INDEX_NAME'],
        fb_ad_creatives_index_name=os.environ['FB_ADS_ELASTIC_SEARCH_FB_AD_CREATIVES_INDEX_NAME'])

    logging.debug('Route map: %s', server.url_map)

    return server

LOGGING_FORMAT = (
    '[%(levelname)s\t%(asctime)s] %(process)d %(thread)d {%(filename)s:%(lineno)d} %(message)s')
logging.basicConfig(format=LOGGING_FORMAT)

if running_on_app_engine.running_on_app_engine():
    cloud_logger_client = google.cloud.logging.Client()
    # Retrieves a Cloud Logging handler based on the environment
    # you're running in and integrates the handler with the
    # Python logging module. By default this captures all logs
    # at INFO level and higher
    cloud_logger_handler = cloud_logger_client.get_default_handler()
    cloud_logger_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(cloud_logger_handler)
    # Prevent unnecessary, or noisy, loggers from filling cloud logs. Copied from
    # https://github.com/googleapis/python-logging/blob/47cba8e6706e91164b78e6fcb56806bf19b5ded5/google/cloud/logging_v2/handlers/handlers.py#L244
    for logger_name in EXCLUDED_LOGGER_DEFAULTS:
        # prevent excluded loggers from propagating logs to handler
        logger = logging.getLogger(logger_name)
        logger.propagate = False
else:
    logging.getLogger().setLevel(logging.DEBUG)
    load_dotenv(override=True)

app = init_server()

if __name__ == '__main__':
    app.run(debug=True)
