"""Module to register a global cache (for handler results, or just to memoize function calls) and
related helper functions.

If running on google app engine (according to ENV vars) uses a redis backed cache (if necessary
config vars set).

Exposes 2 endpoints to view cache keys, and clear cache.
"""
import json
import logging
import os

from flask import Blueprint, Response
from flask_caching import Cache

from common import date_utils, running_on_app_engine

global_cache = Cache()
blueprint = Blueprint('caching', __name__)

def cache_if_response_no_server_error(resp):
    """Returns True if resp.status_code is between 200 and 499, False otherwise."""
    logging.debug('cache_if_response_no_server_error: %r, type: %s, status_code: %s', resp,
                  type(resp), resp.status_code)
    if not hasattr(resp, 'status_code'):
        logging.warning('cached handler return value does not have |status_code| attribute. Make sure '
                     'to add one. %r', resp)
        return True
    return resp.status_code >= 200 and resp.status_code < 500

def app_engine_service_cache_key_prefix():
    return '{}-{}-'.format(os.getenv('GOOGLE_CLOUD_PROJECT'), os.getenv('GAE_SERVICE'))

def app_engine_deployment_cache_key_prefix():
    return '{}{}-'.format(app_engine_service_cache_key_prefix(), os.getenv('GAE_DEPLOYMENT_ID'))

def cache_key_prefix():
    """Creates cache key prefix of <GCP project name>-<service-name>-<deployment ID>- if
    running_on_app_engine, else None"""
    if running_on_app_engine.running_on_app_engine():
        return app_engine_deployment_cache_key_prefix()
    return None

@blueprint.route('/cache/keys')
def cache_list():
    # TODO(macpd): don't access protected class members in order to do this.
    # TODO(macpd): add authn and authz for this handler
    # TODO(macpd): fix occassional TypeError: Object of type bytes is not JSON serializable
    if running_on_app_engine.running_on_app_engine():
        return Response(
            json.dumps(
                {'all-deployment-keys': list(map(str, global_cache.cache._read_clients.keys(
                    app_engine_deployment_cache_key_prefix() + '*'))),
                 'all-service-keys': list(map(str, global_cache.cache._read_clients.keys(
                    app_engine_service_cache_key_prefix() + '*')))
                }
            ),
            mimetype='application/json')
    return Response(
        json.dumps(list(map(str, global_cache.cache._cache.keys()))), mimetype='application/json')

@blueprint.route('/cache/clear')
def cache_clear():
    # TODO(macpd): add authn and authz for this handler
    return Response(json.dumps(global_cache.clear()))

def init_cache(server, cache_blueprint_url_prefix):
    """Initialize cache (simple or redis backed depending on env), and register cache blueprint if
    cache_blueprint_url_prefix is not None.

    To disable endpoints to list keys and clear cache, set cache_blueprint_url_prefix=None
    """
    cache_config = {'CACHE_TYPE': 'SimpleCache',
                    'CACHE_DEFAULT_TIMEOUT': date_utils.ONE_DAY_IN_SECONDS}
    if 'REDIS_HOST' in os.environ and 'REDIS_PORT' in os.environ:
        cache_config['CACHE_TYPE'] = 'RedisCache'
        cache_config['CACHE_REDIS_HOST'] = os.environ['REDIS_HOST']
        cache_config['CACHE_REDIS_PORT'] = os.environ['REDIS_PORT']
        cache_config['CACHE_KEY_PREFIX'] = cache_key_prefix()
        # TODO(macpd): figure out how to provide GCP memorystore redis instance CA cert file for
        # ssl_cert_reqs
        if os.getenv('REDIS_CONNECTION_DISABLE_TLS'):
            logging.warning('Disabling TLS for connections to redis instance')
        else:
            cache_config['CACHE_OPTIONS'] = {'ssl': True, 'ssl_cert_reqs': None}
    logging.info('Cache config: %s', cache_config)
    if cache_blueprint_url_prefix is not None:
        server.register_blueprint(blueprint, url_prefix=cache_blueprint_url_prefix)
        logging.info('registered cache endpoints with url_prefix %s', cache_blueprint_url_prefix)
    global_cache.init_app(server, cache_config)
