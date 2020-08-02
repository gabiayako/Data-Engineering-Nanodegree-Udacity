from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_query="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_query = sql_query
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if not append_only:
            self.log.info("Deleting all rows from {}".format(
                self.destination_table))
            redshift.run("DELETE FROM {}".format(self.destination_table))

        self.log.info("Creating dimension table")
        if append_only:
            redshift.run("""
                INSERT INTO {table} AS {sql_query}"""
                         .format(
                             table=self.destination_table,
                             sql_query=self.sql_query
                         ))
        else:
            redshift.run("""
                CREATE TABLE {table} AS {sql_query}"""
                         .format(
                             table=self.destination_table,
                             sql_query=self.sql_query
                         ))
