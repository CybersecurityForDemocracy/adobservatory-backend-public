import base64
from collections import namedtuple
import datetime
import logging
import time

import requests
from requests.auth import AuthBase
import simplejson as json
from common import date_utils, caching

ElasticSearchApiParams = namedtuple('ElasticSearchApiParams',
                                    ['cluster_base_url',
                                     'api_id',
                                     'api_key',
                                     'fb_pages_index_name',
                                     'fb_ad_creatives_index_name'
                                     ])


class ElasticSearchAuth(AuthBase):
    """Authentication implementation for use with requests, formats API key value from key and
    key_id, and adds it to request Authorization header.
    Use ElasticSearch console to make API key. API key creation docs:
    https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html
    """
    def __init__(self, api_id, api_key):
        """API key_id and Key for ElasticSearch authenticated requests."""
        self._api_id = api_id
        self._api_key = api_key

    def make_authorization_header_value(self):
        """ElasticSearch API requires Authorization header value of "ApiKey (base64 encoded
        API_KEY_ID:API_KEY)"
        """
        raw_api_key_value = '{api_id}:{api_key}'.format(api_id=self._api_id, api_key=self._api_key)
        b64_encoded_api_key_value = base64.b64encode(raw_api_key_value.encode()).decode()
        return 'ApiKey {}'.format(b64_encoded_api_key_value)

    def __call__(self, r):
        r.headers['Authorization'] = self.make_authorization_header_value()
        logging.debug('Added Authorization header: \'%s\'', r.headers['Authorization'])
        return r


def get_int_timestamp(date_obj):
    """Get timestamp as int. if datetime.date returns timestamp of first second of that day.

    Args:
        date_obj: datetime.date or datetime.datetime to convert to timestamp.
    Returns:
        int timestamp on correct conversion. None if other type.
    """
    timestamp = None
    if isinstance(date_obj, datetime.datetime):
        timestamp = int(date_obj.replace(tzinfo=datetime.timezone.utc).timestamp())
    elif isinstance(date_obj, datetime.date):
        timestamp = int(datetime.datetime(year=date_obj.year, month=date_obj.month,
                                          day=date_obj.day, hour=0, minute=0,
                                          second=0, tzinfo=datetime.timezone.utc).timestamp())
    else:
        logging.info('Unsupported type for timestamp conversion: %s', type(date_obj))

    return timestamp


@caching.global_cache.memoize(timeout=date_utils.ONE_DAY_IN_SECONDS)
def query_elastic_search_fb_ad_creatives_index(elastic_search_api_params, ad_creative_query=None,
                                               funding_entity_query=None, page_id_query=None,
                                               ad_delivery_start_time=None,
                                               ad_delivery_stop_time=None, max_results=20,
                                               return_archive_ids_only=True):
    """Queries elastic search for full text search on specified fields in fb ad creatives index.
    """
    start_time = time.time()
    headers = {"content-type": "application/json"}

    query = {}
    query['query'] = {}
    query['query']['bool'] = {}
    query['query']['bool']['must'] = must = []
    query['query']['bool']['should'] = []
    query['aggs'] = {}

    if max_results is not None:
        query['size'] = max_results

    if ad_creative_query is not None:
        sqs = {}
        sqs['simple_query_string'] = {}
        sqs['simple_query_string']['fields'] = [
            'body', 'link_url', 'link_title', 'link_description', 'link_caption', 'page_name',
            'funding_entity']
        sqs['simple_query_string']['query'] = ad_creative_query
        sqs['simple_query_string']['default_operator'] = "and"
        must.append(sqs)
        # Collapse search results by archive ID (ie do not include results with duplicate archie ID)
        query['collapse'] = {'field': 'archive_id'}

    if funding_entity_query is not None:
        sqs = {}
        sqs['simple_query_string'] = {}
        sqs['simple_query_string']['fields'] = ['funding_entity']
        sqs['simple_query_string']['query'] = funding_entity_query
        sqs['simple_query_string']['default_operator'] = "and"
        must.append(sqs)

    if page_id_query is not None:
        match = {}
        match['match'] = {'page_id': page_id_query}
        must.append(match)

    if ad_delivery_start_time is not None:
        time_range = {}
        time_range['range'] = {}
        # Subtract 1 day from start time as rudimentary buffer for timezone issues.
        timestamp = get_int_timestamp(ad_delivery_start_time - datetime.timedelta(days=1))
        time_range['range']['ad_delivery_start_time'] = {'gte': timestamp}
        must.append(time_range)

    if ad_delivery_stop_time is not None:
        time_range = {}
        time_range['range'] = {}
        # Add 1 day from start time as rudimentary buffer for timezone issues.
        timestamp = get_int_timestamp(ad_delivery_stop_time + datetime.timedelta(days=1))
        time_range['range']['ad_delivery_stop_time'] = {'lte': timestamp}
        must.append(time_range)

    request_url = "{cluster_base_url}/{fb_ad_creatives_index_name}/_search".format(
            cluster_base_url=elastic_search_api_params.cluster_base_url,
            fb_ad_creatives_index_name=elastic_search_api_params.fb_ad_creatives_index_name)
    logging.debug('Sending query: %s to %s', query, request_url)
    req = requests.get(request_url, data=json.dumps(query), headers=headers,
                       auth=ElasticSearchAuth(api_id=elastic_search_api_params.api_id,
                                              api_key=elastic_search_api_params.api_key)
                       )
    if not req.ok:
        logging.info('ES query failed. status_code: %s, message: %s.\nrequest query: %s',
                     req.status_code, req.text, query)
        req.raise_for_status()
    data = {}
    data['data'] = []
    for hit in req.json()['hits']['hits']:
        if return_archive_ids_only is True:
            data['data'].append(hit['_source']['archive_id'])
        else:
            data['data'].append(hit['_source'])
    data['metadata'] = {}
    data['metadata']['total'] = req.json()['hits']['total']
    data['metadata']['execution_time_in_millis'] = round((time.time() - start_time) * 1000, 2)
    return data
