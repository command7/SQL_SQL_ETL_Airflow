from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import logging


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 sql_template,
                 s3_location,
                 table_name,
                 json_path=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_template = sql_template
        self.s3_location = s3_location
        self.table_name = table_name
        self.json_path = json_path
        self.staging_songs_insert = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto'
        """
        self.staging_events_insert = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}
        timeformat 'auto'
        """

    def execute(self, context):
        # self.log.info("Creating staging tables if not present.")
        redshift_hook = PostgresHook(self.conn_id)
        # redshift_hook.run(sql=self.sql_template)
        # self.log.info("Staging tables created.")

        aws_hook = AwsHook('aws_credentials')
        credentials = aws_hook.get_credentials()
        logging.info(f"Staging {self.table_name} table")
        logging.info(f"Access key: {credentials.access_key}")
        logging.info(f"Secret key: {credentials.secret_key}")
        if self.table_name =='staging_events':
            redshift_hook.run(sql=self.staging_events_insert.format(self.table_name,
                                                                    self.s3_location,
                                                                    credentials.access_key,
                                                                    credentials.secret_key,
                                                                    self.json_path))
        else:
            redshift_hook.run(sql=self.staging_songs_insert.format(self.table_name,
                                                               self.s3_location,
                                                               credentials.access_key,
                                                               credentials.secret_key))
        logging.info(f"{self.table_name} table successfully staged")
