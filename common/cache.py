import logging

from flask_caching import Cache

global_cache = Cache()

def cache_if_response_no_server_error(resp):
    """Returns True if resp.status_code is between 200 and 499, False otherwise."""
    logging.debug('cache_if_response_no_server_error: %r, type: %s, status_code: %s', resp,
                  type(resp), resp.status_code)
    if not hasattr(resp, 'status_code'):
        logging.warn('cached handler return value does not have |status_code| attribute. Make sure '
                     'to add one. %r', resp)
        return True
    return resp.status_code >= 200 and resp.status_code < 500

