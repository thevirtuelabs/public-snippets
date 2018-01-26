# bigquery2sql script

This script will import data from a bigquery table (defined via SELECT statement) into
a MySQL table. The sql table needs to have columns with the exact names as what SELECT
from Bigquery will return.

## Usage

__python3 bigquery2csv -h__ - help with instructions

__python3 bigquery2csv --file=path/to/rules.yaml__ - run the import with instructions defined in the rules.yaml file.

## Format of the rules.yaml file

```
# SQL command for getting data from Bigquery table
bq_select: >
  SELECT order_id,article_id,timestamp_client,display_duration
  FROM `tnc-web.CoreProduct.user_engagement_signals` LIMIT 10

# name of the output MySQL table, which is already created with correct columns
sql_table: user_engagement_signals

# insertion mode: insert | insert_ignore | replace
mode: replace
```
