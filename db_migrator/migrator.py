import pandas as pd
from sqlalchemy import create_engine
import argparse
import yaml
import sqlalchemy
from datetime import datetime

#script starting time
startTime = datetime.now()

# Connection to Database to get/dump the table
with open("config.yaml", 'r') as ymlfile:
    config = yaml.load(ymlfile)

engine = create_engine(config['db_connection_get'])
engine2 = create_engine(config['db_connection_dump'])
connection = engine.connect()

# Parse command line args select for different files
parser = argparse.ArgumentParser()
parser.add_argument("--getdb", help='Type table name you would like to use from the database')
parser.add_argument("--dumpdb", help='Name your new table for the second database' )
args = parser.parse_args()

# use pandas dataframe to copy table from Database1

get_database = str(args.getdb)
replace_database = str(args.dumpdb)
df_matched_results = pd.read_sql_table(get_database, con=engine)


print(df_matched_results)
length = len(df_matched_results)

# Dump the Dataframe to the new Database
df_matched_results.to_sql(name= replace_database, con=engine2, if_exists='replace', index=False,
                          dtype={"name": sqlalchemy.types.NVARCHAR(length=255), "source": sqlalchemy.types.NVARCHAR(length=255),
                                 "source_site_id": sqlalchemy.types.NVARCHAR(length=255), "source_site_name": sqlalchemy.types.NVARCHAR(length=255),
                                 "source_site_name": sqlalchemy.types.NVARCHAR(length=255), "tag_id": sqlalchemy.types.NVARCHAR(length=255),
                                 "tag_name": sqlalchemy.types.NVARCHAR(length=255), "size": sqlalchemy.types.NVARCHAR(length=255),
                                 "appname": sqlalchemy.types.NVARCHAR(length=255)})

# Python script running time
print ("Python script executed for " + str(datetime.now() - startTime))