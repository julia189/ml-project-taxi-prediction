import sys
import datetime
import boto3
import time
import pandas as pd
from string import Template
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


athena_client = boto3.client("athena", region_name='eu-west-1')
output_path = "s3://athena-query-results-default-thinktank/"
db = "default"
env = args.get("env", "dev")
bucket = args.get("bucket", f"test-database-jh") 
prefix = args.get("prefix", "data/ingestion_data/trips")
# start_date = args.get("start_date", (datetime.now() - pd.Timedelta(value=1, unit="day")).strftime("%Y-%m-%d"))
# end_date = datetime.now().strftime("%Y-%m-%d")

def athena_query(client, query_string, database_name, output_path, max_execution_sec=30) -> pd.DataFrame:
    """Run a query against an Athena database and return the results.

    Start the (asynchronous) execution of the provided query and check every second if it succeeded.
    If so, load the results into a data frame and return it, otherwise return an empty data frame.
    Try for a maximum of max_execution_sec seconds.

    Args:
        client: boto3 athena client
        query_string: SQL query to run
        database_name: database to query
        output_path: s3 path to bucket into which results are written
        max_execution_sec: maximum query execution time in seconds

    Returns:
        pandas DataFrame containing the query results
    """
    response = client.start_query_execution(QueryString=query_string,
                                            QueryExecutionContext={'Database': database_name},
                                            ResultConfiguration={'OutputLocation': output_path})
    execution_id = response['QueryExecutionId']

    for _ in range(max_execution_sec):
        response = client.get_query_execution(QueryExecutionId=execution_id)
        state = response.get('QueryExecution', {}).get('Status', {}).get('State')
        if state == 'SUCCEEDED':
            return pd.read_csv(response['QueryExecution']['ResultConfiguration']['OutputLocation'])
        elif state == 'FAILED':
            print(f'Query execution failed. Athena Response:\n{response}')
            break
        time.sleep(1)
    client.stop_query_execution(QueryExecutionId=execution_id)
    print(f'''Query execution stopped after {max_execution_sec} sec. Either increase max_execution_sec or run
              a faster query. Tip: add a WHERE statement on a partitioned column (column names starting with p_)''')
    return pd.DataFrame()

try:
    
    sql_query_partitions = "SELECT distinct(taxi_id) FROM raw_data"
    sql_main_query = "SELECT * FROM raw_data WHERE taxi_id = 'current_taxi_id'"
    logger.info("Executing first query")
    partitions_df = athena_query(client=athena_client,
                                 query_string=sql_query_partitions,
                                 database_name=db,
                                 output_path=output_path,
                                 max_execution_sec=60)
    logger.info("Executing second query")
    for partition_ in list(partitions_df):
        current_df = athena_query(client=athena_client,
                        query_string=Template(sql_main_query).substitute(current_taxi_id=partition_),
                        database_name=db,
                        output_path=output_path,
                        max_execution_sec=30)
    if not current_df.empty:
        logger.info(f"Writing data to s3://{bucket}/{prefix}")
        current_df.astype(str).to_parquet(
            f's3://{bucket}/{prefix}',
            partition_cols=["taxi_id"]
        )
        logger.info(f"Number of rows: {current_df.shape[0]}")

except Exception as e:
    logger.error("Failed when executing query with error:" , e)

