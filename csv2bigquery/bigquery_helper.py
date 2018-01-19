"""
Manager for Bigquery operations
"""
import os
import re
import sys

from google.cloud import bigquery
from config_helper import logger

class BQManager:
    """
    Manages Bigquery table manipulation
    """
    def __init__(self, table_name):
        parsed_table = re.split(r":|\.", table_name)
        self.client = bigquery.Client.from_service_account_json(os.path.join(sys.path[0], 'bigquery-secret.json'))
        bq_dataset = self.client.dataset(parsed_table[1])
        bq_table_ref = bq_dataset.table(parsed_table[2])
        self.table = self.client.get_table(bq_table_ref)

        self.insert_counter = 0


    def store_rows(self, rows):
        """
        Store rows to BQ table specified in constructor
        """
        if not rows:
            return
        errors = self.client.create_rows(self.table, rows)
        if errors:
            logger.warning("BQ create_rows error: %s", str(errors))
        else:
            self.insert_counter += len(rows)
            logger.info("Stored %d rows (%d in total)", len(rows), self.number_of_stored_rows())


    def number_of_stored_rows(self):
        return self.insert_counter

