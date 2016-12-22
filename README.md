##Overview
This library is about helping you get your data easily and repeatably into Redshift, and it'll give you a little bit of sugar on top (e.g. convenience around managing files in s3, COPYing and LOADing directly to Redshift). `aws_etl_tools` won't help you manage the dependencies of complex workflows, and it won't help you schedule jobs on instances or in containers. It WILL help you reduce the code needed to write those jobs, and it'll give you some standardization and auditability. This is especially useful if you are using Redshift to enable querying across lots of different data schemas from lots of different places, and if you are running batch jobs using python3 to get it there.

##Perspective
This library is relatively opinionated about how to send data to Redshift, but not necessarily about where it's coming from. These opinions are what allow for the benefits of standardization. There are sources, and there are destinations.
###Destinations: RedshiftTable
This is is an object wrapping the context of where the data is going. Currently, there is only one that makes sense for this library, and it's `RedshiftTable`. It's (1) the target Redshift database instance, (2) the name of the table, and (3) a uniqueness identifier. Let's go through each of these.
####database
The Redshift instance is a database class that wraps the credentials necessary to execute commands. You can instantiate one like this:
```python
from aws_etl_tools.redshift_database import RedshiftDatabase
creds = {'database_name': 'my_db'
         'username': 'admin'
         'password': 'PASSWORD'
         'host': 'amazon.redshift.db.com'
         'port': 5439}
my_db = RedshiftDatabase(creds)
```
If you're going to be loading the same database a lot, we recommend subclassing this object and adding your own logic. For example, if you have an application that can run against a staging and a production environment, it might be a good idea to add the concept of an `environment` to your database abstraction.
####target_table
The name of the table should explicitly include the schema as well: e.g. `public.user_events`
####upsert_uniqueness_key
The uniqueness identifier is a tuple of the columns in your table that would be a composite primary key if Redshift allowed you to have one. The base upserting logic will use these values in your data to upsert (last-write-wins) to the target table.

###Sources
WIP

##Configuration
For a lot of the higher level functionality of this library (e.g. all the cool source -> destination stuff), you'll need to set an S3_BASE_PATH in the config, so `aws_etl_tools` can handle shuttling the data through S3 on its way to Redshift. The easiest way to do this is to set an environment variable on the instance / container or to do it yourself just before you import the library:
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
