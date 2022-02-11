"""Module to initialize an elastic search instance with indexes for this project."""
import os.path
import sys
import logging
import json

import elasticsearch

import config_utils

MAPPINGS_DIR = os.path.join('elastic_search', 'mappings')

def create_pages_index(elasticsearch_client):
    '''Create the fb_pages index'''
    settings = json.load(open(os.path.join(MAPPINGS_DIR, "fb_settings.json"), "r"))
    mappings = json.load(open(os.path.join(MAPPINGS_DIR, "fb_pages_mapping.json"), "r"))
    elasticsearch_client.create(index='fb_pages', settings=settings, mappings=mappings)
    logging.info("Successfully created pages index.")

def create_ads_index(elasticsearch_client):
    '''Create the fb_ad_creatives index'''
    settings = json.load(open(os.path.join(MAPPINGS_DIR, "fb_settings.json"), "r"))
    mappings = json.load(open(os.path.join(MAPPINGS_DIR, "fb_ad_creatives_mapping.json"), "r"))
    elasticsearch_client.create(index='fb_ad_creatives', settings=settings, mappings=mappings)
    logging.info("Successfully created ad creatives index.")

def main(argv):
    config = config_utils.get_config(argv[0])
    es_cloud_id = config['ELASTIC_SEARCH'].get('CLOUD_ID', fallback=None)
    es_cluster_name = config['ELASTIC_SEARCH'].get('CLUSTER_NAME', fallback=None)
    if not any([es_cloud_id, es_cluster_name]):
        logging.fatal('Must provide elastic search cluster name or cloud ID')
    api_id = config['ELASTIC_SEARCH']['API_ID']
    api_key = config['ELASTIC_SEARCH']['API_KEY']
    if es_cloud_id:
        elasticsearch_client = elasticsearch.client.IndicesClient(
            elasticsearch.Elasticsearch(cloud_id=es_cloud_id, api_key=(api_id, api_key)))
    else:
        elasticsearch_client = elasticsearch.client.IndicesClient(
            elasticsearch.Elasticsearch(es_cluster_name, api_key=(api_id, api_key)))

    logging.info("Creating ad screener elasticsearch pages index.")
    create_pages_index(elasticsearch_client)
    logging.info("Creating ad screener elasticsearch ad creatives index.")
    create_ads_index(elasticsearch_client)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <config file>' % sys.argv[0])
    config_utils.configure_logger("initalize_es.log")
    main(sys.argv[1:])
