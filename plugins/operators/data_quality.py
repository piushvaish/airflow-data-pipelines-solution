from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    A class to run checks on the data itself

    ...

    Attributes
    ----------
    redshift_conn_id : str
        Redshift credentials
    table : list
        List of tables
    Methods
    -------
    execute(context):
        run checks on the data itself
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                  redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] == 0:
                self.log.error(f"{table} returned no results")
                raise ValueError(f"{table} returned no results. Data quality check failed.")
            self.log.info(f"Data quality check on {self.tables} check passed")
