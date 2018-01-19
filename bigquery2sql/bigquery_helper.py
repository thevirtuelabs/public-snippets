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
    def __init__(self):
        self.client = bigquery.Client.from_service_account_json(os.path.join(sys.path[0], 'bigquery-secret.json'))


    def execute_query(self, sql_query):
        """
        Execute the SQL query and return the results
        """
        query_job = self.client.query(sql_query)
        results = query_job.result()
        return results


    def get_column_names(self, results):
        """
        Get columns names from schema
        """
        return [x.name for x in results.schema]


BQ = BQManager()
