from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
class StageToRedshiftOperator(BaseOperator):
    """
    A class to load JSON formatted files from S3 to Amazon Redshift.

    ...

    Attributes
    ----------
    redshift_conn_id : str
        Redshift credentials
    aws_credentials_id : str
        AWS credentials
    table : str
        Name of the table
    s3_bucket : str
        Name of the bucket
    s3_key : str
        Name of the key
    json : JSON
        Name of the JSON file
    table_exit : Boolean
        Table exits or not
    ignore_headers : int
        Number of rows to ignore
    
    Methods
    -------
    execute(context):
        Loads JSON formatted files from S3 to Amazon Redshift
    """
    ui_color = '#358140'
    template_fields=("s3_key",)  
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="auto",
                 table_exit=True,
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.table_exit=table_exit
        self.ignore_headers = ignore_headers
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.table_exit:
            self.log.warn(f'Table {self.table} exit.')
            self.log.info("Clearing data from staging table")
            redshift.run(f"DELETE FROM {self.table}") 
            
        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key.format(**context))        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        redshift.run(formatted_sql)
        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")