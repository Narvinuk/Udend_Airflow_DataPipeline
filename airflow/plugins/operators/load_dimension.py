
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
This Load Dimension Operator loads data from
Stage tables to Dim tables on Redshift DB
Output: Data will populated into Songs,Users,artists and time Dim tables
"""

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 table="",
                 sql="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id= redshift_conn_id
        self.table=table
        self.sql=sql
        self.truncate=truncate

    def execute(self, context):
        self.log.info("Loading data into Dimension tables...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f"Truncatind data from {self.table} Dimension tables...")
            redshift.run("TRUNCATE TABLE {}". format(self.table))
        formatted_sql = f"INSERT INTO {self.table} {self.sql}"
        redshift.run(formatted_sql)
