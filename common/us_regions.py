import os
import json

def load_json_from_path(file_path):
    with open(file_path, encoding='UTF-8') as open_file:
        return json.load(open_file)

FILTER_OPTIONS_DATA_DIR = 'data/'
REGION_FILTERS_DATA = load_json_from_path(os.path.join(FILTER_OPTIONS_DATA_DIR, 'regions.json'))

US_REGION_NAMES = [region['value'] for region in REGION_FILTERS_DATA if region['value'] != 'All']
