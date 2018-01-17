"""
Import CSV file into Bigquery table. The table must already exist, containing
correct columns corresponding with the csv file.
"""
import argparse
import csv
import os
import re
import sys
from datetime import datetime

import sqlalchemy.exc
from google.cloud import bigquery
from sqlalchemy import MetaData, and_, create_engine, text

from config_helper import config, logger


__version__ = '0.1.0'


def parse_console_params():
    """
    Command line parsing
    """
    parser = argparse.ArgumentParser(description="CSV to Bigquery importer")
    parser.add_argument('--file', help='Input csv filename', required=True)
    parser.add_argument('--timefield', help="Index of the csv field containing the datetime", required=True, type=int)
    parser.add_argument('--starttime', help="Starting time in format YYYY-MM-DD hh:mm:ss", default='min')
    parser.add_argument('--endtime', help="Ending time in format YYYY-MM-DD hh:mm:ss", default='max')
    parser.add_argument('--table', help='Bigquery table where csv data will be stored', required=True)
    options = parser.parse_args()
    return options


def store_to_bq(options):
    """
    Load the csv file, and import it to BQ table according to the input options
    """
    # set start/end time
    dt_format = '%Y-%m-%d %H:%M:%S'
    if options.starttime == 'min':
        START = datetime.min
    else:
        START = datetime.strptime(options.starttime, dt_format)
    if options.endtime == 'max':
        END = datetime.max
    else:
        END = datetime.strptime(options.endtime, dt_format)

    # Bigquery initialization
    # table is in the format "tnc-web:Operations.ymhits", let's split it by ":" and "."
    parsed_table = re.split(r":|\.", options.table)
    bq_client = bigquery.Client.from_service_account_json(os.path.join(sys.path[0], 'bigquery-secret.json'))
    bq_dataset = bq_client.dataset(parsed_table[1])
    bq_table_ref = bq_dataset.table(parsed_table[2])
    bq_table = bq_client.get_table(bq_table_ref)

    with open(os.path.join(sys.path[0], options.file)) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV)
        stored_rows_counter = 0
        for row in readCSV:
            try:
                TS = datetime.strptime(row[options.timefield], dt_format)
                in_range = (TS >= START and TS <= END)
                if not in_range:
                    continue

                errors = bq_client.create_rows(bq_table, [row])
                if errors:
                    logger.warning("BQ create_rows error (uuid: %s): %s", row[4], str(errors))
                else:
                    stored_rows_counter += 1

            except Exception as e:
                    logger.warning(str(e))

            # if stored_rows_counter > 10:
            #     break

    logger.info("Stored %d rows", stored_rows_counter)


def main():
    # here we go
    script_start_time = datetime.now()
    logger.info("START csv2bigquery.py version %s", __version__)

    # command line parsing
    options = parse_console_params()
    logger.info("Options = %s", options)

    # do the action
    store_to_bq(options)

    script_end_time = datetime.now()
    logger.info("END csv2bigquery.py. Execution time = %s", script_end_time - script_start_time)

if __name__ == "__main__":
    main()
