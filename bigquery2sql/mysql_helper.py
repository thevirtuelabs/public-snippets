"""
Class for working with MySQL
"""
import warnings
from datetime import datetime

from sqlalchemy import exc as sa_exc
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from config_helper import config, logger


class SQLManager:

    def __init__(self):
        """
        Constructor
        """
        self.engine = create_engine(config.get('mysql_connect'))
        self.transaction_size = 1000

        # we want to filter 'Duplicate entry' warning for INSERT IGNORE
        warnings.filterwarnings('ignore', '.*1062.*')


    def insert_rows(self, table_name, column_names, rows, mode='insert'):
        """
        Insert rows (from BQ) to the MySQL table, transactions are used.
        """
        # first create the query according to the operational mode
        sql_insert = self._create_sql_insert(table_name, column_names, mode)
        if sql_insert is None:
            logger.error("Unknown mode (%s) specified, don't know what to do", mode)
            return
        logger.info("SQL query: %s", sql_insert)

        in_transaction = False
        try:
            conn = self.engine.connect()
            for index, row in enumerate(rows):
                try:
                    if not in_transaction:
                        trans = conn.begin()
                        in_transaction = True

                    dict_row = {col: row[col] for col in column_names}
                    conn.execute(sql_insert, dict_row)

                    if in_transaction and index % self.transaction_size == (self.transaction_size - 1):
                        trans.commit()
                        logger.info("Commited at %d", index + 1)
                        in_transaction = False

                except Exception as e:
                    logger.warning(str(e))

            if in_transaction:
                logger.info("Final commit at %d", index + 1)
                trans.commit()

            conn.close()

        except Exception as e:
            logger.warning(str(e))


    def _create_sql_insert(self, table_name, column_names, mode):
        """
        Construct the sql query for inserting rows
        """
        if mode == 'insert':
            sql = text("INSERT INTO {}({}) VALUES({})".format(
                table_name,
                ",".join(column_names),
                ",".join([':' + x for x in column_names])))

        elif mode == 'insert_ignore':
            sql = text("INSERT IGNORE INTO {}({}) VALUES({})".format(
                table_name,
                ",".join(column_names),
                ",".join([':' + x for x in column_names])))

        elif mode == 'replace':
            sql = text("REPLACE INTO {}({}) VALUES({})".format(
                table_name,
                ",".join(column_names),
                ",".join([':' + x for x in column_names])))

        else:
            sql = None

        return sql


SQL = SQLManager()
