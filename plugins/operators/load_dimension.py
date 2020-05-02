from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    A class to utilize the provided SQL helper class to run data transformations

    ...

    Attributes
    ----------
    redshift_conn_id : str
        Redshift credentials
    table : str
        Name of the table
    table_exit : Boolean
        Truncate table if exists
    query : SQL
        Data transformation
    Methods
    -------
    execute(context):
        Utilize the provided SQL helper class to run data transformations
    """


    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_exit=True,
                 query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id = redshift_conn_id
        self.table_exit=table_exit
        self.query=query
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.table_exit:
            self.log.warn(f'Table {self.table} exits')
            redshift.run(f"DELETE FROM {self.table}")
        self.log.info(f'Running query {self.query}')
        redshift.run(f"Insert into {self.table} {self.query}")
        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")
