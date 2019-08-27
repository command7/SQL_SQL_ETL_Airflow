from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries
import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table_name,
                 truncate_insert=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table_name = table_name
        self.truncate_insert = truncate_insert
        self.valid_tables = ["users",
                             "songplays",
                             "artists",
                             "songs",
                             "time"]

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        if self.table_name not in self.valid_tables:
            logging.error("No such table exists")
            raise ValueError
        if self.truncate_insert:
            logging.info(f"Clearing {self.table_name} table")
            redshift_hook.run(SqlQueries.truncate_table.format(self.table_nmae))
            logging.info(f"{self.table_name} successfully cleared")
        logging.info(f'Extracting data from staging tables to {self.table_name}')
        if self.table_name == "users":
            redshift_hook.run(sql=SqlQueries.user_table_insert.format(self.table_name))
            logging.info(f"Successfully loaded data into {self.table_name} table")
        elif self.table_name == 'songs':
            redshift_hook.run(sql=SqlQueries.song_table_insert.format(self.table_name))
            logging.info(f"Successfully loaded data into {self.table_name} table")
        elif self.table_name == 'time':
            redshift_hook.run(sql=SqlQueries.time_table_insert.format(self.table_name))
            logging.info(f"Successfully loaded data into {self.table_name} table")
        elif self.table_name == 'users':
            redshift_hook.run(sql=SqlQueries.user_table_insert.format(self.table_name))
            logging.info(f"Successfully loaded data into {self.table_name} table")
        elif self.table_name == 'artists':
            redshift_hook.run(sql=SqlQueries.artist_table_insert.format(self.table_name))
            logging.info(f"Successfully loaded data into {self.table_name} table")
