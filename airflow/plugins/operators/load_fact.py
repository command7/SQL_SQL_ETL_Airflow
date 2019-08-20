from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins.helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table_name="Not Specified",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table_name = table_name

    def execute(self, context):
        self.log.info(f'Extracting data into facts table {}'.format(self.table_name))
        redshift_hook = PostgresHook(self.conn_id)
        redshift_hook.run(sql=SqlQueries.songplay_table_insert)
        self.logging.info(f"Successfully loaded data into {}".format(self.table_name))