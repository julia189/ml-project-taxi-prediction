import sys
import datetime
import boto3
import time
import logging
from string import Template
from data_ingestion import athena_query
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


client = boto3.client("athena", region_name='eu-west-1')
output_path = "s3://athena-query-results-default-thinktank/"
db = "default"
env = args.get("env", "dev")
bucket = args.get("bucket", f"test-database-jh") 
prefix = args.get("prefix", "data/ingestion_data/trips")
# start_date = args.get("start_date", (datetime.now() - pd.Timedelta(value=1, unit="day")).strftime("%Y-%m-%d"))
# end_date = datetime.now().strftime("%Y-%m-%d")


try:
    sql_query_partitions = "SELECT distinct(taxi_id) FROM raw_data"
    sql_main_query = "SELECT * FROM raw_data WHERE taxi_id = 'current_taxi_id'"
    partitions_df = athena_query(client=client,
                                 query_string=sql_query_partitions,
                                 database_name=db,
                                 output_path=output_path,
                                 max_execution_sec=60)
    for partition_ in list(partitions_df):
        current_df = athena_query(client=client,
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

