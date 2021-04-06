from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 aws_credentials_id='',
                 query_and_result_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.query_and_result_checks=query_and_result_checks

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        test_errors = []
        
        for check in self.query_and_result_checks:
            select_stmt = check.get('select_stmt')
            expected_result = check.get('result')
            table = check.get('table')
            
            records = redshift.get_records(select_stmt)[0]
            self.log.info('Find {} values from {} table. Expected: {}'.format(records[0], table, expected_result))
            if expected_result != records[0]:
                test_errors.append({
                    'table': table,
                    'results': records[0],
                    'expected_result': expected_result,
                    'select_stmt': select_stmt
                })
             
            if(len(test_errors) > 0):
                self.log.error(test_errors)
                raise ValueError('Data quality check failed')
            self.log.info('Data quality check ended successfully!')
                