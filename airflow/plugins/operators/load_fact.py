from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 reshift_conn_id = '',
                 table_name_trunc = '',
                 table_name = '',
                 truncate = '',
                 sql_statement = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = SqlQueries.songplay_table_insert
        self.table_name_trunc = table_name_trunc
        self.table_name = table_name
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        facts_sql = self.sql_statement.format(
            truncate = self.truncate,
            table_name_trunc = self.table_name_trunc,
            table_name = self.table_name
        )
        self.log.info('Start loading facts')
        redshift.run(facts_sql)
        self.log.info('Finished loading facts')
