from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 sql_template,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_template = sql_template

    def execute(self, context):
        self.log.info("Creating staging tables if not present.")
        redshift_hook = PostgresHook('redshift_connection')
        redshift_hook.run(sql=self.sql_template)
        self.log.info("Staging tables exist.")





