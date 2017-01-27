## AWS ETL Tools
[![Build Status](https://travis-ci.org/shopkeep/aws_etl_tools.svg?branch=master)](https://travis-ci.org/shopkeep/aws_etl_tools)

AWS ETL Tools is a set of helpers to get your data easily and repeatably into Redshift.

If you're using Redshift to enable querying across many different data schemas from varying data sources, and you're using python3 to get it there, AWS ETL Tools helps to reduce the code needed to write those jobs. It'll also give you some standardization and auditability around managing files in s3 and COPYing data directly to Redshift. With that in mind, this library won't help you manage the dependencies of complex workflows or schedule jobs.

### Example Usage

AWS ETL Tools uses **[boto3](https://github.com/boto/boto3)**, so you can set up your AWS credentials using **[these instructions](https://github.com/boto/boto3#quick-start)**.

```python
from aws_etl_tools.redshift_ingest import RedshiftTable, from_in_memory
from aws_etl_tools.redshift_database import RedshiftDatabase

data_for_upsert = [
    (1, 'a row', 'of data'),
    (2, 'another', 'wonderful row')
]

database = RedshiftDatabase(
    {
        'username': 'admin',
        'password': 'PASSWORD',
        'database_name': 'my_database',
        'host': 'amazon.redshift.db.com',
        'port': 5439
    }
)

redshift_destination = RedshiftTable(
    database=database,
    target_table='public.my_table',
    upsert_uniqueness_key=('id',)
)

from_in_memory(data_for_upsert, redshift_destination)
```

## Getting Started

This library is relatively opinionated about how to send data to Redshift, but not necessarily about where it's coming from. The main concepts in AWS ETL Tools are `destinations`, `sources`, and `ingestors`.

### Destinations

Destinations wrap the context of where the data is going. Currently, there is only one that makes sense for this library, and it's `RedshiftTable`.

A destination is made up of:

1. the target Redshift database instance
2. the target table
3. a uniqueness identifier

Let's go through each of these.

##### database

The Redshift instance is a database class that wraps the credentials necessary to execute commands. It takes a dictionary of credentials:

```python
from aws_etl_tools.redshift_database import RedshiftDatabase
creds = {
    'database_name': 'my_db'
    'username': 'admin'
    'password': 'PASSWORD'
    'host': 'amazon.redshift.db.com'
    'port': 5439
}
my_db = RedshiftDatabase(creds)
```

If you're going to be loading the same database a lot, we recommend subclassing this object and adding your own logic. For example, if you have an application that can run against a staging and a production environment, it might be a good idea to add the concept of an `environment` to your database abstraction.

Databases also have an `ingestion_class`, which dictate how they consume data. See **[Ingestors](#Ingestors)** below.

##### target_table

The name of the table should explicitly include the schema as well: e.g. `public.user_events`

##### upsert_uniqueness_key

The uniqueness identifier is a tuple of the columns in your table that would be a composite primary key: e.g. `('id',)`. The base upserting logic will use these values in your data to upsert (last-write-wins) to the target table.

### Sources

Sources are functions that encapsulate logic around moving data to a `destination` from a variety of, well, sources, including:

* Postgres databases
* S3 (via an S3 path or a manifest)
* Pandas dataframes
* local files
* in memory data

Sources are explicit about what information is needed to send data to its destination. For example, `from_local_file` requires a path to a csv file, while `from_postgres_query` requires a `PostgresDatabase` and a query to execute. All sources require a `destination`.


### Ingestors

Ingestors are classes that determine a database's ingest behavior. AWS ETL Tools' `RedshiftDatabase` class uses the `BasicUpsert` ingestor out of the box, which does pretty much what you'd expect. We also provide the `AuditedUpsert` ingestor, which uses `before_ingest` and `after_ingest` hooks provided by `BasicUpsert` to add some logging on top of the basic functionality.

Using these same hooks, you could add any behavior you want standardized across your ingest. Want send an SNS notification each time you ingest data or need to add another step to deduplicate malformed data? This is where you'd do that. See the **[examples](examples/)** for more.

## Configuration

For a lot of the higher level functionality of this library (everything except ingesting single files from S3), you'll need to set an *S3_BASE_PATH* in the config, so AWS ETL Tools can handle shuttling the data through S3 on its way to Redshift. The easiest way to do this is to set an environment variable on the instance/container or to do it yourself just before you import the library:
```python
import os
os.environ['AWS_ETL_TOOLS_S3_BASE_PATH']='s3://ye-bucket/this/is/where/we/work'
import aws_etl_tools
```
You could also set it directly on the config object like this:
```python
from aws_etl_tools import config
config.S3_BASE_PATH = 's3://ye-bucket/this/is/where/we/work'
```

## Advanced Usage

For more advice and examples on functionality like subclassing destinations and ingestors, check out the **[examples](examples/)**.
