"""Routes and logic for ad search """
from collections import defaultdict, namedtuple
import datetime
import io
import logging
from operator import itemgetter
import os
import os.path
import time

import dhash
from flask import Blueprint, request, Response, abort, current_app
import humanize
from PIL import Image
import pybktree
import pycountry
import requests
import simplejson as json

import db_functions
from common import elastic_search, date_utils, caching

blueprint = Blueprint('ads_search', __name__)

ArchiveIDAndSimHash = namedtuple('ArchiveIDAndSimHash', ['archive_id', 'sim_hash'])

def load_json_from_path(file_path):
    with open(file_path) as open_file:
        return json.load(open_file)


FILTER_OPTIONS_DATA_DIR = 'data/'
REGION_FILTERS_DATA = load_json_from_path(os.path.join(FILTER_OPTIONS_DATA_DIR, 'regions.json'))
GENDER_FILTERS_DATA = load_json_from_path(os.path.join(FILTER_OPTIONS_DATA_DIR, 'genders.json'))
AGE_RANGE_FILTERS_DATA = load_json_from_path(os.path.join(FILTER_OPTIONS_DATA_DIR,
                                                          'ageRanges.json'))
ORDER_BY_FILTERS_DATA = load_json_from_path(os.path.join(FILTER_OPTIONS_DATA_DIR, 'orderBy.json'))
ORDER_DIRECTION_FILTERS_DATA = load_json_from_path(os.path.join(FILTER_OPTIONS_DATA_DIR,
                                                           'orderDirections.json'))

ALLOWED_ORDER_BY_FIELDS_CLUSTER_SEARCH = set(['min_ad_delivery_start_time', 'max_last_active_date',
                                              'min_ad_creation_time', 'max_ad_creation_time',
                                              'min_spend_sum', 'max_spend_sum',
                                              'min_impressions_sum', 'max_impressions_sum',
                                              'cluster_size', 'num_pages'])
ALLOWED_ORDER_BY_FIELDS_AD_SEARCH = set(['ad_delivery_start_time', 'last_active_date',
                                         'ad_creation_time', 'min_spend', 'max_spend',
                                         'min_impressions',
                                         'max_impressions'])
# TODO(macpd): Update these order_by values in FE.
ORDER_BY_FIELD_REWRITE_MAP = {
        'min_ad_creation_time': 'min_ad_delivery_start_time',
        'max_ad_creation_time': 'max_last_active_date',
        }
ALLOWED_ORDER_DIRECTIONS = set(['ASC', 'DESC'])

AD_SCREENSHOT_URL_TEMPLATE = (
    'https://storage.googleapis.com/%(bucket_name)s/%(archive_id)s.png')

AD_SCREENER_REVERSE_IMAGE_SEARCH_NAME_TO_BIT_THRESHOLD = {
    'very high': 2,
    'high': 8,
    'medium': 16,
    'low': 24,
    'very low': 32}

LANGUAGE_CODE_TO_NAME_OVERRIDE_MAP = {
    'el': 'Modern Greek',
    'ne': 'Nepali',
    'sw': 'Swahili',
    'zh-cn': 'Chinese (Simplified)',
    'zh-tw': 'Chinese (Traditional)'
}
NUM_REQUESTED_ALL = 'ALL'
MAX_AD_SEARCH_QUERY_LIMIT = 1000
MAX_ELASTIC_SEARCH_RESULTS = 10 * MAX_AD_SEARCH_QUERY_LIMIT


def get_image_dhash_as_int(image_file_stream):
    image_file = io.BytesIO(image_file_stream.read())
    image = Image.open(image_file)
    dhash.force_pil()
    return dhash.dhash_int(image)

def humanize_int(i):
    """Format numbers for easier readability. Numbers over 1 million are comma formatted, numbers
    over 1 million will be formatted like "1.2 million"

    Args:
        i: int to format.
    Returns:
        string of formatted number.
    """
    if i < 1000000:
        return humanize.intcomma(i)
    return humanize.intword(i)

def get_topic_id_to_name_map():
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        return db_interface.topics()

def get_cluster_languages_code_to_name():
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        language_code_list = db_interface.cluster_languages()
    return make_language_code_to_name_map(language_code_list)

def get_languages_code_to_name():
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        language_code_list = db_interface.ad_creative_languages()
    return make_language_code_to_name_map(language_code_list)

@caching.global_cache.memoize()
def make_language_code_to_name_map(language_code_list):
    language_code_to_name = {}
    for language_code in language_code_list:
        if language_code in LANGUAGE_CODE_TO_NAME_OVERRIDE_MAP:
            language_code_to_name[language_code] = LANGUAGE_CODE_TO_NAME_OVERRIDE_MAP[language_code]
        else:
            try:
                language_code_to_name[language_code] = pycountry.languages.get(
                    alpha_2=language_code).name
            except AttributeError as err:
                logging.info('Unable to get langauge name for language code %s. error: %s',
                             language_code, err)
                language_code_to_name[language_code] = language_code
    return language_code_to_name

def get_language_filter_options():
    language_code_to_name = get_cluster_languages_code_to_name()
    language_filter_data = [{'label': 'All', 'value': 'all'}]
    # Add languages sorted by langauge name.
    for key, val in sorted(language_code_to_name.items(), key=itemgetter(1)):
        language_filter_data.append({'label': val, 'value': key})
    return language_filter_data


@blueprint.route('/filter-options')
@caching.global_cache.cached(query_string=True,
                             response_filter=caching.cache_if_response_no_server_error,
                             timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_filter_options():
    """Options for filtering. Used by FE to populate filter selectors."""
    topics_filter_data = [{'label': key, 'value': str(val)} for key, val in
                          get_topic_id_to_name_map().items()]
    return Response(
        json.dumps(
            {'topics': topics_filter_data,
            'regions': REGION_FILTERS_DATA,
            'genders': GENDER_FILTERS_DATA,
            'ageRanges': AGE_RANGE_FILTERS_DATA,
            'orderByOptions': ORDER_BY_FILTERS_DATA,
            'orderDirections': ORDER_DIRECTION_FILTERS_DATA,
            'languages': get_language_filter_options()}),
        mimetype='application/json')

@blueprint.route('/topics')
@caching.global_cache.cached(query_string=True,
                             response_filter=caching.cache_if_response_no_server_error,
                             timeout=date_utils.SIX_HOURS_IN_SECONDS)
def topic_names():
    return Response(
        json.dumps(list(get_topic_id_to_name_map().keys())), mimetype='application/json')

def make_ad_screenshot_url(archive_id):
    return AD_SCREENSHOT_URL_TEMPLATE % {
        'bucket_name': current_app.config['FB_AD_CREATIVE_GCS_BUCKET'], 'archive_id': archive_id}


def get_ad_cluster_record(ad_cluster_data_row):
    ad_cluster_data = {}
    ad_cluster_data['ad_cluster_id'] = ad_cluster_data_row['ad_cluster_id']
    ad_cluster_data['canonical_archive_id'] = ad_cluster_data_row['canonical_archive_id']
    ad_cluster_data['archive_ids'] = ad_cluster_data_row['archive_ids']
    # Ad start/end dates are used for display only, never used for computation
    ad_cluster_data['start_date'] = ad_cluster_data_row['min_ad_delivery_start_time'].isoformat()
    ad_cluster_data['end_date'] = ad_cluster_data_row['max_last_active_date'].isoformat()

    # This is the total spend and impression for the ad across all demos/regions
    # Again, used for display and not computation
    # TODO(macpd): use correct currency symbol instead of assuming USD.
    min_spend_sum = ad_cluster_data_row['min_spend_sum']
    max_spend_sum = ad_cluster_data_row['max_spend_sum']
    ad_cluster_data['min_spend_sum'] = min_spend_sum
    ad_cluster_data['max_spend_sum'] = max_spend_sum
    ad_cluster_data['total_spend'] = '$%s - $%s' % (
        humanize_int(int(min_spend_sum)), humanize_int(int(max_spend_sum)))

    min_impressions_sum = ad_cluster_data_row['min_impressions_sum']
    max_impressions_sum = ad_cluster_data_row['max_impressions_sum']
    ad_cluster_data['min_impressions_sum'] = min_impressions_sum
    ad_cluster_data['max_impressions_sum'] = max_impressions_sum
    ad_cluster_data['total_impressions'] = '%s - %s' % (
        humanize_int(int(min_impressions_sum)), humanize_int(int(max_impressions_sum)))

    ad_cluster_data['url'] = make_ad_screenshot_url(ad_cluster_data_row['canonical_archive_id'])
    ad_cluster_data['cluster_size'] = humanize_int(int(ad_cluster_data_row['cluster_size']))
    ad_cluster_data['num_pages'] = humanize_int(int(ad_cluster_data_row['num_pages']))
    ad_cluster_data['currencies'] = ad_cluster_data_row['currencies']

    return ad_cluster_data

def get_ad_record(ad_data_row):
    ad_data = {}
    ad_data['archive_id'] = ad_data_row['archive_id']
    # Ad start/end dates are used for display only, never used for computation
    ad_data['start_date'] = ad_data_row['ad_delivery_start_time'].isoformat()
    ad_data['end_date'] = ad_data_row['last_active_date'].isoformat()

    # This is the total spend and impression for the ad across all demos/regions
    # Again, used for display and not computation
    ad_data['currency'] = ad_data_row['currency']
    min_spend = ad_data_row['min_spend']
    max_spend = ad_data_row['max_spend']
    ad_data['min_spend'] = min_spend
    ad_data['max_spend'] = max_spend
    ad_data['total_spend'] = '%s - %s %s' % (
        humanize_int(int(min_spend)), humanize_int(int(max_spend)), ad_data['currency'])

    min_impressions = ad_data_row['min_impressions']
    max_impressions = ad_data_row['max_impressions']
    ad_data['min_impressions'] = min_impressions
    ad_data['max_impressions'] = max_impressions
    ad_data['total_impressions'] = '%s - %s' % (
        humanize_int(int(min_impressions)), humanize_int(int(max_impressions)))

    ad_data['url'] = make_ad_screenshot_url(ad_data['archive_id'])

    return ad_data

def get_cluster_search_allowed_order_by_and_direction(order_by, direction):
    """Get |order_by| and |direction| which are valid and safe to send to FBAdsDBInterface for
    cluster search.
    Invalid args return None.
    """
    if not (order_by and direction):
        return None, None

    if order_by in ALLOWED_ORDER_BY_FIELDS_CLUSTER_SEARCH and direction in ALLOWED_ORDER_DIRECTIONS:
        if order_by in ORDER_BY_FIELD_REWRITE_MAP:
            return ORDER_BY_FIELD_REWRITE_MAP[order_by], direction
        return order_by, direction

    return None, None

def get_ad_search_allowed_order_by_and_direction(order_by, direction):
    """Get |order_by| and |direction| which are valid and safe to send to FBAdsDBInterface for ads
    search.

    Invalid args return None.
    """
    if not (order_by and direction):
        return None, None

    if order_by in ALLOWED_ORDER_BY_FIELDS_AD_SEARCH and direction in ALLOWED_ORDER_DIRECTIONS:
        return order_by, direction

    return None, None

@caching.global_cache.memoize(timeout=date_utils.ONE_DAY_IN_SECONDS)
def get_ad_cluster_data_from_full_text_search(query, page_id, min_date, max_date, region, gender,
                                              age_group, language, order_by, order_direction,
                                              limit):
    es_api_params = current_app.config['FB_ADS_ELASTIC_SEARCH_API_PARAMS']
    query_results = elastic_search.query_elastic_search_fb_ad_creatives_index(
        elastic_search_api_params=es_api_params,
        ad_creative_query=query,
        max_results=MAX_ELASTIC_SEARCH_RESULTS,
        page_id_query=page_id,
        ad_delivery_start_time=min_date,
        ad_delivery_stop_time=max_date,
        return_archive_ids_only=True)
    logging.debug('Full text search results: %s', query_results)
    archive_ids = query_results['data']
    logging.debug('Full text search returned %d archive_ids: %s', len(archive_ids), archive_ids)
    if not archive_ids:
        logging.info('Full text search returned no results.')
        return []
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        # TODO(macpd): use the archive_ids from search results for screenshot cover photo.
        return db_interface.ad_cluster_details_for_archive_ids(archive_ids, min_date, max_date,
                                                               region, gender, age_group, language,
                                                               order_by, order_direction,
                                                               limit=limit)

@caching.global_cache.memoize(timeout=date_utils.ONE_DAY_IN_SECONDS)
def get_ad_cluster_data_for_page_id(page_id, min_date, max_date, region, gender, age_group,
                                    language, order_by, order_direction, limit):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        return db_interface.ad_cluster_details_for_page_id(
            page_id, min_date=min_date, max_date=max_date, region=region, gender=gender,
            age_group=age_group, language=language, order_by=order_by,
            order_direction=order_direction, limit=limit)

@caching.global_cache.memoize(timeout=date_utils.ONE_DAY_IN_SECONDS)
def get_ad_data_from_full_text_search(query, page_id, min_date, max_date, region, gender, age_group,
                                      language, order_by, order_direction, limit):
    es_api_params = current_app.config['FB_ADS_ELASTIC_SEARCH_API_PARAMS']
    query_results = elastic_search.query_elastic_search_fb_ad_creatives_index(
        elastic_search_api_params=es_api_params,
        ad_creative_query=query,
        max_results=MAX_ELASTIC_SEARCH_RESULTS,
        page_id_query=page_id,
        ad_delivery_start_time=min_date,
        ad_delivery_stop_time=max_date,
        return_archive_ids_only=True)
    logging.debug('Full text search results: %s', query_results)
    archive_ids = query_results['data']
    logging.debug('Full text search returned %d archive_ids: %s', len(archive_ids), archive_ids)
    if not archive_ids:
        logging.info('Full text search returned no results.')
        return []
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        return db_interface.ad_details_of_archive_ids(archive_ids, min_date, max_date, region,
                                                      gender, age_group, language, order_by,
                                                      order_direction, limit=limit)

@caching.global_cache.memoize(timeout=date_utils.ONE_DAY_IN_SECONDS)
def get_ad_data_for_page_id(page_id, min_date, max_date, region, gender, age_group, language,
                            order_by, order_direction, limit):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        return db_interface.ad_details_of_page_id(
            page_id, min_date=min_date, max_date=max_date, region=region, gender=gender,
            age_group=age_group, language=language, order_by=order_by,
            order_direction=order_direction, limit=limit)

@caching.global_cache.memoize(timeout=date_utils.ONE_DAY_IN_SECONDS)
def get_ad_cluster_data_for_topic(
            topic_id, min_date, max_date, region, gender,
            age_group, language, order_by,
            order_direction, limit,
            min_topic_percentage_threshold):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        return db_interface.topic_top_ad_clusters_by_spend(
            topic_id, min_date=min_date, max_date=max_date, region=region, gender=gender,
            age_group=age_group, language=language, order_by=order_by,
            order_direction=order_direction, limit=limit,
            min_topic_percentage_threshold=min_topic_percentage_threshold)

@caching.global_cache.memoize(timeout=date_utils.ONE_DAY_IN_SECONDS)
def get_ad_data_for_topic(topic_id, min_date, max_date, region, gender, age_group, language,
                          order_by, order_direction, limit):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        return db_interface.ad_details_of_topic(
            topic_id, min_date=min_date, max_date=max_date, region=region, gender=gender,
            age_group=age_group, language=language, order_by=order_by,
            order_direction=order_direction, limit=limit)

def get_num_bits_different(archive_id_and_simhash1, archive_id_and_simhash2):
    return dhash.get_num_bits_different(archive_id_and_simhash1.sim_hash,
                                        archive_id_and_simhash2.sim_hash)

@caching.global_cache.memoize(timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_image_simhash_bktree():
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        simhash_to_archive_id_set = db_interface.all_ad_creative_image_simhashes()

    total_sim_hashes = len(simhash_to_archive_id_set)
    logging.info('Got %d image simhashes to process.', total_sim_hashes)

    # Create BKTree with dhash bit difference function as distance_function, used to find similar
    # hashes
    image_simhash_tree = pybktree.BKTree(get_num_bits_different)

    sim_hashes_added_to_tree = 0
    tree_construction_start_time = time.time()
    for sim_hash, archive_id_set in simhash_to_archive_id_set.items():
        # Add single entry in BK tree for simhash with lowest archive_id.
        image_simhash_tree.add(ArchiveIDAndSimHash(sim_hash=sim_hash,
                                                   archive_id=min(archive_id_set)))
        sim_hashes_added_to_tree += 1
        if sim_hashes_added_to_tree % 1000 == 0:
            logging.debug('Added %d/%d simhashes to BKtree.', sim_hashes_added_to_tree,
                          total_sim_hashes)
    logging.info('Constructed BKTree in %s seconds', (time.time() - tree_construction_start_time))
    return image_simhash_tree


def reverse_image_search(image_file_stream, bit_difference_threshold):
    image_dhash = get_image_dhash_as_int(image_file_stream)
    logging.info(
        'Got reverse_image_search request: %s bit_difference_threshold, file with dhash %x',
        bit_difference_threshold, image_dhash)
    image_simhash_tree = get_image_simhash_bktree()

    found = image_simhash_tree.find(ArchiveIDAndSimHash(sim_hash=image_dhash, archive_id=-1),
                                    bit_difference_threshold)
    logging.info('%d similar image archive IDs: %s', len(found), found)
    # BKTree.find returns tuples of form (bit difference, value). This extracts a set of all
    # archive IDs found.
    archive_ids = {x[1].archive_id for x in found}
    if not archive_ids:
        logging.info('Full text search returned no results.')
        return []
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        return db_interface.ad_cluster_details_for_archive_ids(
                list(archive_ids), min_date=None, max_date=None, region=None, gender=None,
                age_group=None, language=None, order_by=None, order_direction=None)

def handle_ad_search(topic_id, min_date, max_date, gender, age_group, region, language, order_by,
                     order_direction, num_requested, offset, full_text_search_query, page_id):
    if topic_id is not None and full_text_search_query is not None:
        abort(400, description='topic cannot be combined with full_text_search.')

    if num_requested == NUM_REQUESTED_ALL:
        offset = 0
        limit = None
    else:
        limit = MAX_AD_SEARCH_QUERY_LIMIT
        try:
            num_requested = int(num_requested)
        except ValueError:
            abort(400, description='numResults must be an integer')
        try:
            offset = int(offset)
        except ValueError:
            abort(400, description='offset must be an integer')
        if offset + num_requested > MAX_AD_SEARCH_QUERY_LIMIT:
            abort(400,
                  description=(
                      'sum of numResults and offset must be less than {offset_max}'
                      ).format(offset_max=MAX_AD_SEARCH_QUERY_LIMIT))


    # This date parsing is needed because the FE passes raw UTC formatted dates in Zulu time
    # We can simplify this by not sending the time at all from the FE. Then we strip the time info
    # and just take the date for simplicity.
    if min_date and max_date:
        try:
            min_date = datetime.datetime.strptime(
                min_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()
        except ValueError:
            min_date = date_utils.parse_date_arg(min_date)

        try:
            max_date = datetime.datetime.strptime(
                max_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()
        except ValueError:
            max_date = date_utils.parse_date_arg(max_date)

    if gender:
        if gender.lower() == 'all':
            gender = None
        elif gender.lower() == 'f':
            gender = 'female'
        elif gender.lower() == 'm':
            gender = 'male'
        elif gender.lower() == 'u':
            gender = 'unknown'
    if region and region.lower() == 'all':
        region = None
    if age_group and age_group.lower() == 'all':
        age_group = None
    if language and language.lower() == 'all':
        language = None

    if full_text_search_query:
        results =  get_ad_data_from_full_text_search(
            full_text_search_query, page_id=page_id, min_date=min_date, max_date=max_date,
            region=region, gender=gender, age_group=age_group, language=language, order_by=order_by,
            order_direction=order_direction, limit=limit)

    elif page_id:
        results = get_ad_data_for_page_id(page_id, min_date=min_date, max_date=max_date, region=region,
                                       gender=gender, age_group=age_group, language=language,
                                       order_by=order_by, order_direction=order_direction,
                                       limit=limit)
    else:
        results = get_ad_data_for_topic(
            topic_id, min_date=min_date, max_date=max_date, region=region, gender=gender,
            age_group=age_group, language=language, order_by=order_by,
            order_direction=order_direction, limit=limit)

    if limit:
        logging.debug('returning results[%s:%s] of %d results', offset, (offset + num_requested),
                      len(results))
        return results[offset:(offset+num_requested)]

    return results



@blueprint.route('/ads', methods=['GET'])
@caching.global_cache.cached(query_string=True,
                             response_filter=caching.cache_if_response_no_server_error,
                             timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_ads():
    topic_id = request.args.get('topic', None)
    min_date = request.args.get('startDate', None)
    max_date = request.args.get('endDate', None)
    gender = request.args.get('gender', 'ALL')
    age_group = request.args.get('ageRange', 'ALL')
    region = request.args.get('region', 'All')
    order_by, order_direction = get_ad_search_allowed_order_by_and_direction(
        request.args.get('orderBy', 'max_spend'),
        request.args.get('orderDirection', 'DESC'))
    num_requested = request.args.get('numResults', 20)
    offset = request.args.get('offset', 0)
    full_text_search_query = request.args.get('full_text_search', None)
    page_id = request.args.get('page_id', None)
    language = request.args.get('language', None)

    ad_data = handle_ad_search(
        topic_id, min_date, max_date, gender, age_group, region, language, order_by,
        order_direction, num_requested, offset, full_text_search_query, page_id)

    ret = [get_ad_record(row) for row in ad_data]

    return Response(json.dumps(ret), mimetype='application/json')

def handle_ad_cluster_search(topic_id, min_date, max_date, gender, age_group, region, language,
                             order_by, order_direction, num_requested, offset,
                             full_text_search_query, page_id):
    if topic_id is not None and full_text_search_query is not None:
        abort(400, description='topic cannot be combined with full_text_search.')

    if num_requested == NUM_REQUESTED_ALL:
        offset = 0
        limit = None
    else:
        limit = MAX_AD_SEARCH_QUERY_LIMIT
        try:
            num_requested = int(num_requested)
        except ValueError:
            abort(400, description='numResults must be an integer')
        try:
            offset = int(offset)
        except ValueError:
            abort(400, description='offset must be an integer')
        if offset + num_requested > MAX_AD_SEARCH_QUERY_LIMIT:
            abort(400,
                  description=(
                      'sum of numResults and offset must be less than {offset_max}'
                      ).format(offset_max=MAX_AD_SEARCH_QUERY_LIMIT))


    # This date parsing is needed because the FE passes raw UTC formatted dates in Zulu time
    # We can simplify this by not sending the time at all from the FE. Then we strip the time info
    # and just take the date for simplicity.
    if min_date and max_date:
        try:
            min_date = datetime.datetime.strptime(
                min_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()
        except ValueError:
            min_date = date_utils.parse_date_arg(min_date)

        try:
            max_date = datetime.datetime.strptime(
                max_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()
        except ValueError:
            max_date = date_utils.parse_date_arg(max_date)

    if gender:
        if gender.lower() == 'all':
            gender = None
        elif gender.lower() == 'f':
            gender = 'female'
        elif gender.lower() == 'm':
            gender = 'male'
        elif gender.lower() == 'u':
            gender = 'unknown'
    if region and region.lower() == 'all':
        region = None
    if age_group and age_group.lower() == 'all':
        age_group = None
    if language and language.lower() == 'all':
        language = None


    if full_text_search_query:
        results = get_ad_cluster_data_from_full_text_search(
            full_text_search_query, page_id=page_id, min_date=min_date, max_date=max_date,
            region=region, gender=gender, age_group=age_group, language=language, order_by=order_by,
            order_direction=order_direction, limit=limit)

    elif page_id:
        results = get_ad_cluster_data_for_page_id(
            page_id=page_id, min_date=min_date, max_date=max_date, region=region, gender=gender,
            age_group=age_group, language=language, order_by=order_by,
            order_direction=order_direction, limit=limit)
    else:
        results = get_ad_cluster_data_for_topic(
                topic_id, min_date=min_date, max_date=max_date, region=region, gender=gender,
                age_group=age_group, language=language, order_by=order_by,
                order_direction=order_direction, limit=limit, min_topic_percentage_threshold=0.25)

    if limit:
        logging.debug('returning results[%s:%s] of %d results', offset, (offset + num_requested),
                      len(results))
        return results[offset:(offset+num_requested)]

    return results



@blueprint.route('/ad-clusters', methods=['GET', 'POST'])
@caching.global_cache.cached(query_string=True,
                             response_filter=caching.cache_if_response_no_server_error,
                             timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_ad_clusters():
    return Response(json.dumps(get_ad_clusters_data(request)), mimetype='application/json')

def get_ad_clusters_data(request):
    if request.method == 'POST':
        if 'reverse_image_search' not in request.files:
            abort(400, description='Client must provide a reverse_image_search file')

        requested_similarity = request.form.get('similarity', 'medium').lower()
        logging.debug('Reverse image search similarity: %s', requested_similarity)
        if requested_similarity not in AD_SCREENER_REVERSE_IMAGE_SEARCH_NAME_TO_BIT_THRESHOLD:
            abort(400, description='Invalid similarity value')

        bit_difference_threshold = AD_SCREENER_REVERSE_IMAGE_SEARCH_NAME_TO_BIT_THRESHOLD.get(
            requested_similarity)
        ad_cluster_data = reverse_image_search(request.files['reverse_image_search'],
                                               bit_difference_threshold=bit_difference_threshold)
    else:
        topic_id = request.args.get('topic', None)
        min_date = request.args.get('startDate', None)
        max_date = request.args.get('endDate', None)
        gender = request.args.get('gender', 'ALL')
        age_group = request.args.get('ageRange', 'ALL')
        region = request.args.get('region', 'All')
        order_by, order_direction = get_cluster_search_allowed_order_by_and_direction(
            request.args.get('orderBy', 'max_spend_sum'),
            request.args.get('orderDirection', 'DESC'))
        num_requested = request.args.get('numResults', 20)
        offset = request.args.get('offset', 0)
        full_text_search_query = request.args.get('full_text_search', None)
        page_id = request.args.get('page_id', None)
        language = request.args.get('language', None)

        ad_cluster_data = handle_ad_cluster_search(
            topic_id, min_date, max_date, gender, age_group, region, language, order_by,
            order_direction, num_requested, offset, full_text_search_query, page_id)

    return [get_ad_cluster_record(row) for row in ad_cluster_data]


def cluster_additional_ads(db_interface, ad_cluster_id):
    return list(db_interface.ad_cluster_archive_ids(ad_cluster_id))

def cluster_advertiser_info(db_interface, ad_cluster_id):
    advertiser_info = db_interface.ad_cluster_advertiser_info(ad_cluster_id)
    return format_advertiser_info(advertiser_info)

def ad_advertiser_info(db_interface, archive_id):
    advertiser_info = db_interface.ad_advertiser_info(archive_id)
    return format_advertiser_info(advertiser_info)

def format_advertiser_info(advertiser_info):
    ret = []
    for row in advertiser_info:
        ret.append({'advertiser_type': row['page_type'], 'advertiser_party': row['party'],
                    'advertiser_fec_id': row['fec_id'], 'advertiser_webiste': row['page_url'],
                    'advertiser_risk_score': str(row['advertiser_score']),
                    'facebook_page_id': row['page_id'], 'facebook_page_name': row['page_name']})
    return ret

@blueprint.route('/ads/<int:archive_id>')
@caching.global_cache.cached(query_string=True,
                             response_filter=caching.cache_if_response_no_server_error,
                             timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_ad_details(archive_id):
    return Response(json.dumps(ad_details(archive_id)), mimetype='application/json')

def ad_details(archive_id):
    db_connection = db_functions.get_fb_ads_database_connection()
    db_interface = db_functions.FBAdsDBInterface(db_connection)

    ad_data = defaultdict(list)
    ad_data['archive_id'] = archive_id
    region_impression_results = db_interface.ad_region_impression_results(archive_id)
    for row in region_impression_results:
        ad_data['region_impression_results'].append(
            {'region': row['region'],
             'min_spend': row['min_spend'],
             'max_spend': row['max_spend'],
             'min_impressions': row['min_impressions'],
             'max_impressions': row['max_impressions']})

    demo_impression_results = db_interface.ad_demo_impression_results(archive_id)
    for row in demo_impression_results:
        ad_data['demo_impression_results'].append({
            'age_group': row['age_group'],
            'gender': row['gender'],
            'min_spend': row['min_spend'],
            'max_spend': row['max_spend'],
            'min_impressions': row['min_impressions'],
            'max_impressions': row['max_impressions']})

    topics = db_interface.ad_topics(archive_id)
    if topics:
        ad_data['topics'] = ', '.join(topics)

    ad_data['advertiser_info'] = ad_advertiser_info(db_interface, archive_id)
    ad_data['funding_entity'] = list(db_interface.ad_funder_names(archive_id))
    ad_timing_and_impressions_data = db_interface.ad_timing_and_impressions_data(archive_id)
    ad_data['min_spend'] = ad_timing_and_impressions_data['min_spend']
    ad_data['max_spend'] = ad_timing_and_impressions_data['max_spend']
    ad_data['min_impressions'] = ad_timing_and_impressions_data['min_impressions']
    ad_data['max_impressions'] = ad_timing_and_impressions_data['max_impressions']
    ad_data['currency'] = ad_timing_and_impressions_data['currency']
    ad_data['ad_creation_date'] = (
        ad_timing_and_impressions_data['ad_delivery_start_time'].isoformat())
    ad_data['last_active_date'] = ad_timing_and_impressions_data['last_active_date'].isoformat()
    ad_data['url'] = make_ad_screenshot_url(archive_id)
    # These fields are generated by NYU and show up in the Metadata tab
    ad_data['type'] = ', '.join(db_interface.ad_types(archive_id))
    ad_data['entities'] = ', '.join(db_interface.ad_recognized_entities(archive_id))
    language_code_to_name = make_language_code_to_name_map(db_interface.ad_languages(archive_id))
    ad_data['languages'] = [language_code_to_name.get(lang, None) for lang in language_code_to_name]

    return ad_data


@blueprint.route('/ad-clusters/<int:ad_cluster_id>')
@caching.global_cache.cached(query_string=True,
                             response_filter=caching.cache_if_response_no_server_error,
                             timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_ad_cluster_details(ad_cluster_id):
    db_connection = db_functions.get_fb_ads_database_connection()
    db_interface = db_functions.FBAdsDBInterface(db_connection)

    ad_cluster_data = defaultdict(list)
    ad_cluster_data['ad_cluster_id'] = ad_cluster_id
    region_impression_results = db_interface.ad_cluster_region_impression_results(ad_cluster_id)
    for row in region_impression_results:
        ad_cluster_data['region_impression_results'].append(
            {'region': row['region'],
             'min_spend': row['min_spend_sum'],
             'max_spend': row['max_spend_sum'],
             'min_impressions': row['min_impressions_sum'],
             'max_impressions': row['max_impressions_sum']})

    demo_impression_results = db_interface.ad_cluster_demo_impression_results(ad_cluster_id)
    for row in demo_impression_results:
        ad_cluster_data['demo_impression_results'].append({
            'age_group': row['age_group'],
            'gender': row['gender'],
            'min_spend': row['min_spend_sum'],
            'max_spend': row['max_spend_sum'],
            'min_impressions': row['min_impressions_sum'],
            'max_impressions': row['max_impressions_sum']})

    cluster_topics = db_interface.ad_cluster_topics(ad_cluster_id)
    if cluster_topics:
        ad_cluster_data['topics'] = ', '.join(cluster_topics)

    ad_cluster_data['advertiser_info'] = cluster_advertiser_info(db_interface, ad_cluster_id)
    ad_cluster_data['funding_entity'] = list(db_interface.ad_cluster_funder_names(ad_cluster_id))
    ad_cluster_metadata = db_interface.ad_cluster_metadata(ad_cluster_id)
    ad_cluster_data['min_spend_sum'] = ad_cluster_metadata['min_spend_sum']
    ad_cluster_data['max_spend_sum'] = ad_cluster_metadata['max_spend_sum']
    ad_cluster_data['min_impressions_sum'] = ad_cluster_metadata['min_impressions_sum']
    ad_cluster_data['max_impressions_sum'] = ad_cluster_metadata['max_impressions_sum']
    ad_cluster_data['cluster_size'] = ad_cluster_metadata['cluster_size']
    ad_cluster_data['num_pages'] = ad_cluster_metadata['num_pages']
    canonical_archive_id = ad_cluster_metadata['canonical_archive_id']
    ad_cluster_data['canonical_archive_id'] = canonical_archive_id
    ad_cluster_data['min_ad_creation_date'] = (
        ad_cluster_metadata['min_ad_delivery_start_time'].isoformat())
    ad_cluster_data['max_ad_creation_date'] = (
        ad_cluster_metadata['max_last_active_date'].isoformat())
    ad_cluster_data['url'] = make_ad_screenshot_url(canonical_archive_id)
    ad_cluster_data['archive_ids'] = cluster_additional_ads(db_interface, ad_cluster_id)
    # These fields are generated by NYU and show up in the Metadata tab
    ad_cluster_data['type'] = ', '.join(db_interface.ad_cluster_types(ad_cluster_id))
    ad_cluster_data['entities'] = ', '.join(db_interface.ad_cluster_recognized_entities(
        ad_cluster_id))
    language_code_to_name = get_cluster_languages_code_to_name()
    ad_cluster_data['languages'] = [language_code_to_name.get(lang, None) for lang in
                                    db_interface.ad_cluster_languages(ad_cluster_id)]
    ad_cluster_data['currencies'] = db_interface.ad_cluster_currencies(ad_cluster_id)

    return Response(json.dumps(ad_cluster_data), mimetype='application/json')

@blueprint.route('/archive-id/<int:archive_id>/cluster')
@caching.global_cache.cached(query_string=True,
                             response_filter=caching.cache_if_response_no_server_error,
                             timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_cluster_id_from_archive_id(archive_id):
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        ad_cluster_id = db_interface.get_cluster_id_from_archive_id(archive_id)
    if ad_cluster_id is None:
        abort(404)
    return Response(json.dumps({'cluster_id': ad_cluster_id}), mimetype='application/json')

@blueprint.route('/search/pages_type_ahead')
@caching.global_cache.cached(query_string=True,
                             response_filter=caching.cache_if_response_no_server_error,
                             timeout=date_utils.SIX_HOURS_IN_SECONDS)
def pages_type_ahead():
    '''
    This endpoint accepts a query parameter (q) and uses that parameter to perform an
    n-gram search for all page names that match the query term. The size parameter can be
    used to limit the size of returned matching page names. An optional (size) parameter
    may also be passed to specify the number of auto-complete results to return.
    '''
    start_time = time.time()
    headers = {"content-type": "application/json"}
    query = {}

    query['query'] = {}

    # Process size parameter if supplied
    size = request.args.get('size', None)
    if size is not None:
        query['size'] = size

    # Process query term
    q_arg = request.args.get('q', None)
    query['query']['bool'] = {
        'must': [{'match': {'page_name.ngram': q_arg}}],
        'should': [{'rank_feature': {'field': 'lifelong_amount_spent',
                                     'log': {'scaling_factor': 1}}}]}

    if q_arg is None:
        abort(400, 'The q_arg parameter is required for this endpoint.')

    elastic_search_api_params = current_app.config['FB_ADS_ELASTIC_SEARCH_API_PARAMS']
    url = "{cluster_base_url}/{fb_pages_index_name}/_search".format(
        cluster_base_url=elastic_search_api_params.cluster_base_url,
        fb_pages_index_name=elastic_search_api_params.fb_pages_index_name)
    data = json.dumps(query)
    logging.debug('Sending type ahead request to %s query: %s', url, data)

    req = requests.get(url, data=data, headers=headers,
                       auth=elastic_search.ElasticSearchAuth(api_id=elastic_search_api_params.api_id,
                                              api_key=elastic_search_api_params.api_key)
                       )
    req.raise_for_status()
    data = {}
    data['data'] = []
    json_response = req.json()
    logging.debug('json_response: %s', json_response)
    hits = json_response.get('hits', {}).get('hits')
    for hit in hits:
        data['data'].append(hit['_source'])
    data['metadata'] = {}
    data['metadata']['total'] = req.json()['hits']['total']
    data['metadata']['execution_time_in_millis'] = round((time.time() - start_time) * 1000, 2)
    return Response(json.dumps(data), mimetype='application/json')

@blueprint.route("/search/archive_ids")
@caching.global_cache.cached(query_string=True,
                             response_filter=caching.cache_if_response_no_server_error,
                             timeout=date_utils.SIX_HOURS_IN_SECONDS)
def get_archive_ids_from_full_text_search():
    '''
    This endpoint returns archive ids that match specific page ids or keywords (matched against the
    ad creative body).
    '''
    # Process size parameter if supplied
    size = request.args.get('size', None)
    body = request.args.get('body', None)
    funding_entity = request.args.get('funding_entity', None)
    page_id = request.args.get('page_id', None)
    ad_delivery_start_time = request.args.get('ad_delivery_start_time', None)
    ad_delivery_stop_time = request.args.get('ad_delivery_stop_time', None)
    archive_ids_only = request.args.get('archive_ids_only', True)
    es_api_params = current_app.config['FB_ADS_ELASTIC_SEARCH_API_PARAMS']
    search_results = elastic_search.query_elastic_search_fb_ad_creatives_index(
        elastic_search_api_params=es_api_params,
        ad_creative_query=body,
        funding_entity_query=funding_entity,
        page_id_query=page_id,
        ad_delivery_start_time=ad_delivery_start_time,
        ad_delivery_stop_time=ad_delivery_stop_time,
        max_results=size,
        return_archive_ids_only=archive_ids_only)
    return Response(json.dumps(search_results), mimetype='application/json')
