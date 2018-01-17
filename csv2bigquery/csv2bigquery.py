"""
Import CSV file into Bigquery table. The table must already exist, containing
correct columns corresponding with the csv file.
"""
import argparse
import csv
import os
import sys
from datetime import datetime

from config_helper import logger
from bigquery_helper import BQManager

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


def import_csv(options):
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
    BQ = BQManager(options.table)

    with open(os.path.join(sys.path[0], options.file)) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV)
        row_buffer = []
        for row in readCSV:
            try:
                TS = datetime.strptime(row[options.timefield], dt_format)
                in_range = (TS >= START and TS <= END)
                if not in_range:
                    continue

                row_buffer.append(row)
                if len(row_buffer) == 5000:
                    BQ.store_rows(row_buffer)
                    row_buffer = []

            except Exception as e:
                logger.warning(str(e))

        BQ.store_rows(row_buffer)

    logger.info("Stored %d rows", BQ.number_of_stored_rows())


def main():
    # here we go
    script_start_time = datetime.now()
    logger.info("START csv2bigquery.py version %s", __version__)

    # command line parsing
    options = parse_console_params()
    logger.info("Options = %s", options)

    # do the action
    import_csv(options)

    script_end_time = datetime.now()
    logger.info("END csv2bigquery.py. Execution time = %s", script_end_time - script_start_time)

if __name__ == "__main__":
    main()
