from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table_name,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table_name = table_name

    def execute(self, context):
        self.logging.info(f'Extracting data from staging tables to {self.table_name}')
        redshift_hook = PostgresHook(self.conn_id)
        if self.table_name == "users":
            redshift_hook.run(sql=SqlQueries.user_table_insert.format(self.table_name))
            self.logging.info(f"Successfully loaded data into {self.table_name} table")
        elif self.table_name == 'songs':
            redshift_hook.run(sql=SqlQueries.song_table_insert.format(self.table_name))
            self.logging.info(f"Successfully loaded data into {self.table_name} table")
        elif self.table_name == 'time':
            redshift_hook.run(sql=SqlQueries.time_table_insert.format(self.table_name))
            self.logging.info(f"Successfully loaded data into {self.table_name} table")
        elif self.table_name == 'users':
            redshift_hook.run(sql=SqlQueries.user_table_insert.format(self.table_name))
            self.logging.info(f"Successfully loaded data into {self.table_name} table")
        elif self.table_name == 'artists':
            redshift_hook.run(sql=SqlQueries.artist_table_insert.format(self.table_name))
            self.logging.info(f"Successfully loaded data into {self.table_name} table")
        else:
            self.logging.error("Wrong table name")
            raise ValueError
