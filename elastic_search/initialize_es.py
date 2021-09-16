"""Module to initialize an elastic search instance with indexes for this project."""
import os.path
import sys
import logging

import requests
import ujson as json

import config_utils

MAPPINGS_DIR = os.path.join('elastic_search', 'mappings')

def create_pages_index(es_cluster_name):
    '''Create the fb_pages index'''
    settings = json.load(open(os.path.join(MAPPINGS_DIR, "fb_settings.json"), "r"))
    mappings = json.load(open(os.path.join(MAPPINGS_DIR, "fb_pages_mapping.json"), "r"))
    index = {'settings': settings, 'mappings': mappings}
    headers = {'content-type': 'application/json'}
    url = "https://{es_cluster_name}/fb_pages".format(es_cluster_name=es_cluster_name)
    req = requests.put(url, headers=headers, data=json.dumps(index))
    logging.info("Successfully created pages index. Status code: %s", req.status_code)
    if not req.ok:
        logging.warning("Encountered an error when creating index: %s", req.content)

def create_ads_index(es_cluster_name):
    '''Create the fb_ad_creatives index'''
    settings = json.load(open(os.path.join(MAPPINGS_DIR, "fb_settings.json"), "r"))
    mappings = json.load(open(os.path.join(MAPPINGS_DIR, "fb_ad_creatives_mapping.json"), "r"))
    index = {'settings': settings, 'mappings': mappings}
    headers = {'content-type': 'application/json'}
    url = "https://{es_cluster_name}/fb_ad_creatives".format(es_cluster_name=es_cluster_name)
    print('url:', url)
    print('data:\n',json.dumps(index))
    req = requests.put(url, headers=headers, data=json.dumps(index))
    logging.info("Successfully created ad creatives index. Status code: %s", req.status_code)
    if not req.ok:
        logging.warning("Encountered an error when creating index: %s", req.content)

def main(argv):
    config = config_utils.get_config(argv[0])
    es_cluster_name = config['ELASTIC_SEARCH']['CLUSTER_NAME']
    logging.info("Creating ad screener elasticsearch pages index.")
    create_pages_index(es_cluster_name)
    logging.info("Creating ad screener elasticsearch ad creatives index.")
    create_ads_index(es_cluster_name)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <config file>' % sys.argv[0])
    config_utils.configure_logger("initalize_es.log")
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    main(sys.argv[1:])
