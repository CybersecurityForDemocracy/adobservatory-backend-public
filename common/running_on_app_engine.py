import logging
import os

def running_on_app_engine():
    logging.info(
        'env vars GAE_ENV: %s, GAE_INSTANCE: %s, GAE_SERVICE: %s, GAE_DEPLOYMENT_ID: %s, '
        'GAE_APPLICATION: %s, GAE_VERSION: %s, GOOGLE_CLOUD_PROJECT: %s',
        os.getenv('GAE_ENV'), os.getenv('GAE_INSTANCE'), os.getenv('GAE_SERVICE'),
        os.getenv('GAE_DEPLOYMENT_ID'), os.getenv('GAE_APPLICATION'), os.getenv('GAE_VERSION'),
        os.getenv('GOOGLE_CLOUD_PROJECT'))
    return os.getenv('GAE_ENV', '').startswith('standard') or os.getenv('GAE_INSTANCE', '')
