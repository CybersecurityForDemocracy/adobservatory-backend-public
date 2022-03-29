"""Module to populate elastic search instance with data from ads database."""
import sys
import time
from collections import defaultdict
from datetime import date, datetime
import logging
import json

from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import elasticsearch

import config_utils
import db_functions

logging.basicConfig(level=logging.INFO)

PAGES_TABLE_FETCH_BATCH_SIZE = 50000
AD_CREATIVES_TABLE_FETCH_BATCH_SIZE = 10000


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))

def insert_rows_into_es(es_client, rows, action, index):
    if not rows:
        return

    records = []
    for record in rows:
        bulk = defaultdict(dict)
        bulk[action]['_index'] = index
        try:
            bulk[action]['_id'] = str(record['id'])
        except KeyError as err:
            logging.error('%s, record: %s', err, record)
            raise

        records.extend(list(map(lambda x: json.dumps(x, ensure_ascii=False), [bulk, record])))

    logging.info("Sending %d records for indexing", len(rows))
    records = '\n'.join(records) + "\n"
    records = records.encode('utf-8')
    response = es_client.bulk(operations=records)
    if response.meta.status != 200 or response.body['errors']:
        logging.error('Bulk updated failed:\n%s\n%s', response.meta, response.body)


def fetch_all_tables(conn):
    '''Fetch all table names from DB'''
    cursor = conn.cursor()
    tables = []
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

    for table in cursor.fetchall():
        tables.append(table[0])

    return tables

def fetch_ad_topics(conn):
    '''Fetch ad topics'''
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * from ad_topics LIMIT 10")
    rows = cursor.fetchall()
    return rows

def fetch_topics(conn):
    '''Fetch topics'''
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * from topics LIMIT 1000")
    rows = cursor.fetchall()
    return rows

def move_pages_to_es(db_connection_params, es_client, pages_index_name, days_in_past_to_sync):
    '''Transfer page data from Postgres to Elasticsearch'''
    db_connection = db_functions.get_database_connection(db_connection_params)
    total_records_inserted = 0
    logging.info("Copying records from pages table to elasticsearch.")

    if days_in_past_to_sync:
        where_clause = sql.SQL(
            'WHERE pages.last_modified_time >= CURRENT_TIMESTAMP - ' 'interval \'{days} days\''
            ).format(days=sql.Literal(days_in_past_to_sync))
    else:
        where_clause = sql.SQL('')

    start_time = time.time()

    with db_connection.cursor(name='populate_es_pages', cursor_factory=RealDictCursor) as cursor:
        query = sql.SQL(
            '''SELECT page_id, page_name, COALESCE(lifelong_amount_spent, 0) AS
            lifelong_amount_spent, extract(epoch from pages.last_modified_time) AS
            last_modified_time FROM pages LEFT JOIN (SELECT page_id, max(amount_spent) AS
            lifelong_amount_spent FROM ad_library_report_pages JOIN ad_library_reports
            USING(ad_library_report_id) WHERE kind = 'lifelong' AND geography = 'US'
            GROUP BY page_id) AS page_id_max_lifelong_spend USING(page_id) {where_clause}'''
            ).format(where_clause=where_clause)
        cursor.execute(query)
        logging.info('move_pages_to_es qyery: %s', cursor.query.decode())

        rows = cursor.fetchmany(PAGES_TABLE_FETCH_BATCH_SIZE)
        while rows:
            es_records = []

            for row in rows:
                record = {}
                record['id'] = row['page_id']
                record['page_name'] = row['page_name']
                # This field is used as a rank_feature, which requires a values greater than 0
                record['lifelong_amount_spent'] = max(row['lifelong_amount_spent'], 0.1)
                es_records.append(record)

            insert_rows_into_es(es_client, rows=es_records, action='index', index=pages_index_name)
            total_records_inserted += len(es_records)
            logging.debug("Inserted %s page records.", total_records_inserted)

            rows = cursor.fetchmany(PAGES_TABLE_FETCH_BATCH_SIZE)

    logging.info("Copied %s page records in %d seconds.", total_records_inserted,
                 int(time.time() - start_time))

def move_ads_to_es(db_connection_params, es_client, ad_creatives_index_name, days_in_past_to_sync):
    '''Transfer page data from Postgres to Elasticsearch'''
    total_records_inserted = 0
    logging.info("Copying records from ad creatives table to elasticsearch.")
    direct_copy_field_names = [
        'text_sim_hash', 'text_sha256_hash', 'image_downloaded_url', 'image_bucket_path',
        'image_sim_hash', 'image_sha256_hash', 'ad_creative_body_language', 'funding_entity',
        'page_id', 'page_name', 'last_modified_time', 'ad_delivery_start_time',
        'ad_delivery_stop_time']

    if days_in_past_to_sync:
        where_clause = sql.SQL(
            'WHERE ads.last_modified_time >= CURRENT_TIMESTAMP - interval \'{days} days\' '
            'OR ad_creatives.last_modified_time >= CURRENT_TIMESTAMP - interval \'{days} days\''
            ).format(days=sql.Literal(days_in_past_to_sync))
    else:
        where_clause = sql.SQL('')

    start_time = time.time()

    db_connection = db_functions.get_database_connection(db_connection_params)
    with db_connection.cursor(name='populate_es_ad_creatives',
                              cursor_factory=RealDictCursor) as cursor:
        query = sql.SQL(
            """SELECT ad_creatives.*, ads.funding_entity,
               EXTRACT(epoch from ad_creatives.last_modified_time) "last_modified_time",
               EXTRACT(epoch from ads.ad_delivery_start_time) "ad_delivery_start_time",
               EXTRACT(epoch from ads.ad_delivery_stop_time) "ad_delivery_stop_time", ads.page_id,
               pages.page_name FROM ad_creatives JOIN ads USING(archive_id)
               JOIN pages USING(page_id) {where_clause}"""
        ).format(where_clause=where_clause)
        cursor.execute(query)
        logging.info('move_ads_to_es qyery: %s', cursor.query.decode())
        rows = cursor.fetchmany(AD_CREATIVES_TABLE_FETCH_BATCH_SIZE)

        while rows:
            es_records = []

            for row in rows:
                record = {}
                record['id'] = row['ad_creative_id']
                record['archive_id'] = row['archive_id']
                record['body'] = row['ad_creative_body']
                record['link_url'] = row['ad_creative_link_url']
                record['link_caption'] = row['ad_creative_link_caption']
                record['link_title'] = row['ad_creative_link_title']
                record['link_description'] = row['ad_creative_link_description']
                record['body_language'] = row['ad_creative_body_language']
                for key in direct_copy_field_names:
                    record[key] = row[key]
                es_records.append(record)

            insert_rows_into_es(es_client, rows=es_records, action='index',
                                index=ad_creatives_index_name)
            total_records_inserted += len(es_records)
            logging.debug("Inserted %s ad creatives records.", total_records_inserted)

            rows = cursor.fetchmany(AD_CREATIVES_TABLE_FETCH_BATCH_SIZE)

    logging.info("Copied %s ad creatives records in %d seconds.", total_records_inserted,
                 int(time.time() - start_time))

def main(argv):
    config = config_utils.get_config(argv[0])
    db_connection_params = config_utils.get_database_connection_params_from_config(config)
    es_cloud_id = config['ELASTIC_SEARCH'].get('CLOUD_ID', fallback=None)
    es_cluster_name = config['ELASTIC_SEARCH'].get('CLUSTER_NAME', fallback=None)
    if not any([es_cloud_id, es_cluster_name]):
        logging.fatal('Must provide elastic search cluster name or cloud ID')
    pages_index_name = config['ELASTIC_SEARCH']['PAGES_INDEX_NAME']
    ad_creatives_index_name = config['ELASTIC_SEARCH']['AD_CREATIVES_INDEX_NAME']
    api_id = config['ELASTIC_SEARCH']['API_ID']
    api_key = config['ELASTIC_SEARCH']['API_KEY']
    days_in_past_to_sync = config.getint('ELASTIC_SEARCH', 'DAYS_IN_PAST_TO_SYNC', fallback=0)
    if es_cloud_id:
        elasticsearch_client = elasticsearch.Elasticsearch(
            cloud_id=es_cloud_id, api_key=(api_id, api_key))
        logging.info('Populating elastic search cloud ID %s, indexes: %s, days_in_past_to_sync: %s',
                     es_cloud_id, [pages_index_name, ad_creatives_index_name], days_in_past_to_sync)
    else:
        elasticsearch_client = elasticsearch.Elasticsearch(
            es_cluster_name, api_key=(api_id, api_key))
        logging.info('Populating elastic search cluster %s, indexes: %s, days_in_past_to_sync: %s',
                     es_cluster_name, [pages_index_name, ad_creatives_index_name], days_in_past_to_sync)
    move_pages_to_es(db_connection_params, elasticsearch_client, pages_index_name, days_in_past_to_sync)
    move_ads_to_es(db_connection_params, elasticsearch_client, ad_creatives_index_name,
                   days_in_past_to_sync)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <config file>' % sys.argv[0])
    config_utils.configure_logger("populate_es.log")
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    main(sys.argv[1:])
