from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins.helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 dest_table_name,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dest_table_name = dest_table_name

    def execute(self, context):
        self.log.info(f'Extracting data from staging tables to {}'.format(self.dest_table_name))
        redshift_hook = PostgresHook('redshift_connection')
        if self.dest_table_name == "users":
            redshift_hook.run(sql=SqlQueries.user_table_insert)
            self.logging.info(f"Successfully loaded data into {} table".format(self.dest_table_name))
        elif self.dest_table_name == 'songs':
            redshift_hook.run(sql=SqlQueries.song_table_insert)
            self.logging.info(f"Successfully loaded data into {} table".format(self.dest_table_name))
        elif self.dest_table_name == 'time':
            redshift_hook.run(sql=SqlQueries.time_table_insert)
            self.logging.info(f"Successfully loaded data into {} table".format(self.dest_table_name))
        elif self.dest_table_name == 'users':
            redshift_hook.run(sql=SqlQueries.user_table_insert)
            self.logging.info(f"Successfully loaded data into {} table".format(self.dest_table_name))
        elif self.dest_table_name == 'artists':
            redshift_hook.run(sql=SqlQueries.artist_table_insert)
            self.logging.info(f"Successfully loaded data into {} table".format(self.dest_table_name))
        else:
            self.logging.error("Wrong table name")
            raise ValueError