from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""

This LoadFactOperator operators loads data from Stages files into Fact table

Output : Data will be inserted from stages tables to songplays table

"""

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 table="",
                 sql="",
                 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id= redshift_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        self.log.info('LoadFactOperator')
        self.log.info('Loading data into FAct tables...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = f"INSERT INTO {self.table} {self.sql}"
        redshift.run(formatted_sql)
