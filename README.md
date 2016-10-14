More to come, but the most important thing to know about using this library is that you need to tell it where to put things in s3. You _must_ set an environment variable for an s3_base_path before you import aws_etl_tools:

```python
import os
os.environ['AWS_ETL_TOOLS_S3_BASE_PATH']='s3://ye-bucket/this/isnt/optional'
import aws_etl_tools
```
Or better yet, set the environment variable as part of the context of whatever is using this as a dependency.
