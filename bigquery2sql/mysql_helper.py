"""
Class for storing articles to database(s)
"""
from datetime import datetime

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


    def insert_rows(self, table_name, column_names, rows, mode='insert'):
        """
        """
        sql_insert = text("INSERT INTO {}({}) VALUES({})".format(
            table_name,
            ",".join(column_names),
            ",".join([':' + x for x in column_names])))
        logger.info("SQL query: %s", sql_insert)

        in_transaction = False
        try:
            conn = self.engine.connect()
            for index, row in enumerate(rows):
                try:
                    if not in_transaction:
                        trans = conn.begin()
                        in_transaction = True

                    conn.execute(sql_insert, {col: row[col] for col in column_names})

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


SQL = SQLManager()
