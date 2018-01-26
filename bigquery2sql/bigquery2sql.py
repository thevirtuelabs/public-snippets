"""
Import Bigquery table to mysql table. Importing rules will be defined in the separate
yaml file, which will be given as an argument to the script.
"""
import argparse
import os
import sys
from datetime import datetime

import yaml

from bigquery_helper import BQ
from config_helper import config, logger
from mysql_helper import SQL

__version__ = '0.1.0'

def parse_console_params():
    """
    Command line parsing
    """
    parser = argparse.ArgumentParser(description="Bigquery to MySQL importer")
    parser.add_argument('--file', help='Input yaml filename with import rules', required=True)
    options = parser.parse_args()
    return options


def perform_action(options):
    """
    The script action - load from BQ and store to MySQL
    """
    # load ETL rules
    rules = yaml.load(open(os.path.join(sys.path[0], options.file)))

    # get results from BQ
    rows = BQ.execute_query(rules.get('bq_select'))
    column_names = BQ.get_column_names(rows)

    SQL.insert_rows(rules.get('sql_table'), column_names, rows, rules.get('mode'))


def main():

    # here we go
    script_start_time = datetime.now()
    logger.info("START %s, version %s", os.path.basename(__file__), __version__)

    # command line parsing
    options = parse_console_params()
    logger.info("Options = %s", options)

    # do things and stuff
    perform_action(options)

    # pack and leave
    script_end_time = datetime.now()
    logger.info("END %s. Execution time = %s", os.path.basename(__file__), script_end_time - script_start_time)


if __name__ == "__main__":
    main()
