"""Module to hold common config logic."""
import configparser
import logging

import db_functions


def get_database_connection_params_from_config(config):
    """Get database connection params from configparser object.

    Args:
        config: configparser.ConfigParser from which to extract config params.
    Returns:
        db_functions.DatabaseConnectionParams to get a database connection.
    """
    return db_functions.DatabaseConnectionParams(
        host=config['POSTGRES']['HOST'],
        database_name=config['POSTGRES']['DBNAME'],
        username=config['POSTGRES']['USER'],
        password=config['POSTGRES']['PASSWORD'],
        port=config['POSTGRES']['PORT'],
        default_schema=config['POSTGRES']['DEFAULT_SCHEMA'],
        sslrootcert=config.get('POSTGRES', 'SERVER_CA', fallback=None),
        sslcert=config.get('POSTGRES', 'CLIENT_CERT', fallback=None),
        sslkey=config.get('POSTGRES', 'CLIENT_KEY', fallback=None),
        )


def get_database_connection_from_config(config):
    """Get pyscopg2 database connection from the provided ConfigParser.

    Args:
        config: configparser.ConfigParser initialized from desired file.
    Returns:
        psycopg2.connection ready to be used.
    """
    connection_params = get_database_connection_params_from_config(config)
    return db_functions.get_database_connection(connection_params)


def get_facebook_access_token(config):
    return config['FACEBOOK']['TOKEN']


def get_config(config_path):
    """Get configparser object initialized from config path.

    Args:
        config_path: str file path to config.
    Returns:
        configparser.ConfigParser initialized from config_path.
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def configure_logger(log_filename):
    """Configure root logger to write to log_filename and STDOUT.

    Args:
      log_filename: str, filename to be used for log file.
    """
    record_format = (
        '[%(levelname)s\t%(asctime)s] %(process)d %(thread)d {%(filename)s:%(lineno)d} '
        '%(message)s')
    logging.basicConfig(
        handlers=[logging.FileHandler(log_filename),
                  logging.StreamHandler()],
        format=record_format,
        level=logging.INFO)
