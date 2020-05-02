from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    A class to utilize the provided SQL helper class to run data transformations

    ...

    Attributes
    ----------
    redshift_conn_id : str
        Redshift credentials
    table : str
        Name of the table
    append_only : Boolean
        Table exits or not
    query : SQL
        Data transformation
    Methods
    -------
    execute(context):
        Utilize the provided SQL helper class to run data transformations
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append_only=True,
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id = redshift_conn_id
        self.append_only=append_only
        self.query=query
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            self.log.warn(f'Table {self.table} exit.')
            redshift.run(f"DELETE FROM {self.table}")
        self.log.info(f'Running query {self.query}')
        redshift.run(f"Insert into {self.table} {self.query}")
        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")
        
