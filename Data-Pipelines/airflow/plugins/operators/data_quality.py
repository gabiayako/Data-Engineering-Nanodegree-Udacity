from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        tables = ["songplays", "users", "songs", "artists", "time"]
        for table in tables:
            records = redshift_hook.get_records(
                "SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    "Data quality check failed. {} returned no results".format(table))
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    "Data quality check failed. {} contained 0 rows".format(table))
            logging.info("Data quality on table {table} check passed with {num_records} records".format(
                table=table, num_record=records[0][0]))
