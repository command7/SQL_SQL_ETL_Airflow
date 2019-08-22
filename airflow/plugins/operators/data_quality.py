from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table_names,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table_names = table_names

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for table_name in self.table_names:
            record_counts = redshift_hook.run(sql=f"SELECT COUNT(*) FROM {table_name}")
            logging.info(f"Query Result: {record_counts}")