import os
import json

import db_functions

FILTER_OPTIONS_DATA_DIR = 'data/'

def load_json_from_path(file_path, base_dir=FILTER_OPTIONS_DATA_DIR):
    with open(os.path.join(base_dir, file_path), encoding='UTF-8') as open_file:
        return json.load(open_file)

REGION_FILTERS_DATA = load_json_from_path('regions.json')

US_REGION_NAMES = [region['value'] for region in REGION_FILTERS_DATA if region['value'] != 'All']

GENDER_FILTERS_DATA = load_json_from_path('genders.json')
AGE_RANGE_FILTERS_DATA = load_json_from_path('ageRanges.json')
AGE_RANGE_LABEL_TO_VALUE = {item['label']: item['value'] for item in AGE_RANGE_FILTERS_DATA}
ORDER_BY_FILTERS_DATA = load_json_from_path('orderBy.json')
ORDER_DIRECTION_FILTERS_DATA = load_json_from_path('orderDirections.json')

AD_SCREENSHOT_URL_TEMPLATE = (
    'https://storage.googleapis.com/%(bucket_name)s/%(archive_id)s.png')


def parse_gender_value(gender):
    if gender is None:
        return None
    gender = gender.lower()
    if gender == 'all':
        return None
    if gender in ('f', 'female)'):
        return 'female'
    if gender in ('m', 'male'):
        return 'male'
    if gender in ('u', 'unknown'):
        return 'unknown'
    raise ValueError('Unknown gender value: %s' % gender)

def parse_age_range_value(age_range):
    if age_range is None:
        return None
    if age_range and age_range.lower() == 'all':
        return None
    if age_range in AGE_RANGE_LABEL_TO_VALUE:
        return AGE_RANGE_LABEL_TO_VALUE[age_range]
    raise ValueError('Unknown age_range value: %s' % age_range)

def parse_region_label_to_value(region):
    if region is None:
        return None
    if region and region.lower() == 'all':
        return None
    if region not in US_REGION_NAMES:
        raise ValueError('Unknown region value: %s' % region)
    return region

def get_topic_id_to_name_map():
    with db_functions.get_fb_ads_database_connection() as db_connection:
        db_interface = db_functions.FBAdsDBInterface(db_connection)
        return db_interface.topics()

def topic_names():
    return list(get_topic_id_to_name_map().keys())

def topics_filter_data():
    return [{'label': key, 'value': str(val)} for key, val in get_topic_id_to_name_map().items()]

def make_ad_screenshot_url(archive_id):
    return AD_SCREENSHOT_URL_TEMPLATE % {
        'bucket_name': current_app.config['FB_AD_CREATIVE_GCS_BUCKET'], 'archive_id': archive_id}
