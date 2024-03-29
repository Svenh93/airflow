from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 reshift_conn_id = '',
                 table_name = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.reshift_conn_id = reshift_conn_id,
        self.table_names = table_names

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table_name in table_names:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table_name}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table_name} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.table_name} contained 0 rows")
            logging.info(f"Data quality on table {self.table_name} check passed with {records[0][0]} records")