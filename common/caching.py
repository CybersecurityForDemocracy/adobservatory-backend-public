"""Module to register a global cache (for handler results, or just to memoize function calls) and
related helper functions.

If running on google app engine (according to ENV vars) uses a redis backed cache (if necessary
config vars set).

Exposes 2 endpoints to view cache keys, and clear cache.
"""
import json
import logging
import os
import contextlib
import threading
import functools

from flask import Blueprint, Response
from flask_caching import Cache
import redis.exceptions

from common import date_utils, running_on_app_engine

global_cache = Cache()
blueprint = Blueprint('caching', __name__)
LOCK_NAME_PREFIX = 'lock::'
# TODO(macpd): figure out better way to do this. Only for server run locally.
IN_MEMORY_LOCKS = {}

class Error(Exception):
    pass

class LockError(Error):
    pass

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

def has_redis_cache_env_vars():
    return 'REDIS_HOST' in os.environ and 'REDIS_PORT' in os.environ


@contextlib.contextmanager
def acquire_lock(name, blocking_timeout=-1):
    """Raises LockError if unable to acquire"""
    full_lock_name = ''.join(filter(None, [cache_key_prefix(), LOCK_NAME_PREFIX, name]))
    logging.debug('Acquiring lock: %s', full_lock_name)
    blocking = blocking_timeout == -1
    if has_redis_cache_env_vars():
        if blocking_timeout == -1:
            blocking_timeout = None
        try:
            with global_cache.cache._read_clients.lock(name=full_lock_name, timeout=date_utils.SIX_HOURS_IN_SECONDS, blocking_timeout=blocking_timeout) as lock:
                logging.debug('acquired lock %s', full_lock_name)
                yield lock
        except redis.exceptions.LockError as e:
            raise LockError from e
        logging.debug('released lock %s', full_lock_name)
    else:
        if full_lock_name in IN_MEMORY_LOCKS:
            lock = IN_MEMORY_LOCKS[full_lock_name]
        else:
            lock = threading.Lock()
            IN_MEMORY_LOCKS[full_lock_name] = lock

        acquired = False
        try:
            acquired = lock.acquire(blocking=blocking, timeout=blocking_timeout)
            if acquired:
                logging.debug('acquired lock %s', full_lock_name)
                yield lock
            else:
                raise LockError
        finally:
            if acquired:
                lock.release()
                logging.debug('released lock %s', full_lock_name)

def cache_response_blocking_duplicate_generation_of_cache_payload(
        query_string=True, response_filter=cache_if_response_no_server_error,
        timeout=date_utils.ONE_DAY_IN_SECONDS):
    """If decorated function's response is not in cache, attempt to acquire a lock and execute
    function. This is a wrapper around flask-caching @cached decorator that prevents duplicate work
    for same request URL (path and args).  (example: multiple threads/workers/backend executing an
    expensive DB query at same time to generate result that will be cached, when really only one
    thread/worker/backend needs to do that and other can wait for results of that instead of sending
    a duplicate query to DB).
    """
    def decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            # This wraps the original function f in the caching decorator so that the
            # caching decorator handles all of the cache get/set logic when the lock is acquired
            # (instead of acquiring the lock inside the cache get/set logic which would potentially
            # repeat unneccessary (ie thread tries to acquire lock before cache is populated, but
            # then acquires the lock after the cache is populated).
            cached_function = global_cache.cached(query_string=query_string,
                                                            response_filter=response_filter,
                                                            timeout=timeout)(f)
            cache_key = cached_function.make_cache_key(*args, **kwargs)
            with acquire_lock(cache_key):
                return f(*args, **kwargs)
        return decorated_function
    return decorator

def memoize_response_blocking_duplicate_generation_of_cache_payload(timeout):
    """If decorated function's response is not in cache, attempt to acquire a lock and execute
    function. This is a wrapper around flask-caching @memoize decorator that prevents duplicate work
    for same function call. (example: multiple threads/workers/backend executing an expensive DB
    query at same time to generate result that will be cached, when really only one
    thread/worker/backend needs to do that and other can wait for results of that instead of sending
    a duplicate query to DB).

    """
    def decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            # This wraps the original function f in the memoization decorator so that the
            # memoization decorator handles all of the cache get/set logic when the lock is acquired
            # (instead of acquiring the lock inside the cache get/set logic which would potentially
            # repeat unneccessary (ie thread tries to acquire lock before cache is populated, but
            # then acquires the lock after the cache is populated).
            memoized_function = global_cache.memoize(timeout=timeout)(f)
            cache_key = memoized_function.make_cache_key(f, *args, **kwargs)
            with acquire_lock(cache_key):
                return memoized_function(*args, **kwargs)
        return decorated_function
    return decorator


@blueprint.route('/cache/keys')
def cache_list():
    # TODO(macpd): don't access protected class members in order to do this.
    # TODO(macpd): add authn and authz for this handler
    # TODO(macpd): fix occassional TypeError: Object of type bytes is not JSON serializable
    if has_redis_cache_env_vars():
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
    if has_redis_cache_env_vars():
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
    elif running_on_app_engine.running_on_app_engine():
        logging.error(
            'Server running running on App Engine using in-memory cache. This NOT recommended!!')
    logging.info('Cache config: %s', cache_config)
    if cache_blueprint_url_prefix is not None:
        server.register_blueprint(blueprint, url_prefix=cache_blueprint_url_prefix)
        logging.info('registered cache endpoints with url_prefix %s', cache_blueprint_url_prefix)
    global_cache.init_app(server, cache_config)
