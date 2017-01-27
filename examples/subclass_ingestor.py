import json

from aws_etl_tools.redshift_ingest.ingestors import AuditedUpsert
from boto3 import SNS


class AdditionalDedupeIngestor(AuditedUpsert):
    '''An example of a custom ingestor that deduplicates data that
    may not be unique within a single batch.
    '''

    def _insert_statement(self):
        return """
            INSERT INTO {target_table}
            SELECT DISTINCT unique_id
                , column_1
                , column_2
             FROM {staging_table}
        """.format(
           target_table=self.target_table,
           staging_table=self.staging_table
        )


class SNSNotifyingIngestor(AuditedUpsert):
    '''An example of a custom ingestor that sends an SNS Notification
    with each ingest
    '''

    def after_ingest(self):
        super.after_ingest()

        notification = {
            'uuid': self.uuid,
            'target_table': self.target_table,
            'ingest_results': self.ingest_results
        }
        SNS.notify(json.dumps(notification))
