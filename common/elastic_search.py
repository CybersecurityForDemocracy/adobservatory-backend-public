from collections import namedtuple
import datetime
import logging
import time

from elasticsearch import helpers

from common import date_utils, caching

ElasticSearchApiParams = namedtuple('ElasticSearchApiParams',
                                    [
                                     'api_id',
                                     'api_key',
                                     'fb_pages_index_name',
                                     'fb_ad_creatives_index_name',
                                     'client',
                                     ])


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

    query = {}
    query['query'] = {}
    query['query']['bool'] = {}
    query['query']['bool']['must'] = must = []
    query['query']['bool']['filter'] = query_filter = []
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
        if return_archive_ids_only:
            query['_source'] = ['archive_id']
        sqs['simple_query_string']['query'] = ad_creative_query
        sqs['simple_query_string']['default_operator'] = "and"
        must.append(sqs)

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
        query_filter.append(match)

    if ad_delivery_start_time is not None:
        time_range = {}
        time_range['range'] = {}
        # Subtract 1 day from start time as rudimentary buffer for timezone issues.
        timestamp = get_int_timestamp(ad_delivery_start_time - datetime.timedelta(days=1))
        time_range['range']['ad_delivery_start_time'] = {'gte': timestamp}
        query_filter.append(time_range)

    if ad_delivery_stop_time is not None:
        time_range = {}
        time_range['range'] = {}
        # Add 1 day from start time as rudimentary buffer for timezone issues.
        timestamp = get_int_timestamp(ad_delivery_stop_time + datetime.timedelta(days=1))
        time_range['range']['ad_delivery_stop_time'] = {'lte': timestamp}
        query_filter.append(time_range)

    results = helpers.scan(
        elastic_search_api_params.client,
        body=query,
        index=elastic_search_api_params.fb_ad_creatives_index_name,
        scroll='60m')

    data = {}
    if return_archive_ids_only:
        data['data'] = list({hit['_source']['archive_id'] for hit in results})
    else:
        data['data'] = [hit['_source'] for hit in results]

    data['metadata'] = {}
    data['metadata']['total'] = len(data['data'])
    data['metadata']['execution_time_in_millis'] = round((time.time() - start_time) * 1000, 2)
    return data
