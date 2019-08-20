from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins.helpers.sql_queries import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook


staging_events_insert = """
"""


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 sql_template,
                 songs_s3_location,
                 events_s3_location,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_template = sql_template
        self.songs_location = songs_s3_location
        self.events_location = events_s3_location
        self.staging_songs_insert = """
        COPY staging_songs
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        """
        self.staging_events_insert = """
        COPY staging_events
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        """

    def execute(self, context):
        self.log.info("Creating staging tables if not present.")
        redshift_hook = PostgresHook('redshift_connection')
        redshift_hook.run(sql=self.sql_template)
        self.log.info("Staging tables created.")

        aws_hook = AwsHook('aws_credentials')
        credentials = aws_hook.get_credentials()
        self.logging.info("Staging songs table")
        redshift_hook.run(sql=self.staging_songs_insert.format(self.songs_location,
                                                               credentials.access_key,
                                                               credentials.secret_key))
        self.logging.info("Songs table successfully staged")

        self.logging.info("Staging events table")
        redshift_hook.run(sql=self.staging_songs_insert.format(self.events_location,
                                                               credentials.access_key,
                                                               credentials.secret_key))
        self.logging.info("Events table successfully staged")







