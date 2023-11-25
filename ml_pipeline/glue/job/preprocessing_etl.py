import sys, os
import boto3
import time
import pandas as pd
import awswrangler as wr
from string import Template
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
optional_params = ["target_bucket", "prefix", "env"]
given_params = []
for param_ in optional_params:
    if '--{}'.format(param_) in sys.argv:
        given_params.append(param_)
args = getResolvedOptions(sys.argv, ["JOB_NAME"] + given_params)
job.init(args["JOB_NAME"], args)

file_path = "s3://think-tank-casestudy/raw_data/"
env = args.get("env", "dev")
source_bucket =  "think-tank-casestudy"
source_prefix = "raw_data"
target_bucket = args.get("target_bucket", f"test-database-jh") 
prefix = args.get("prefix", "data/ingestion_data/trips")

try:
    s3_client = boto3.client("s3")
    result = s3_client.list_objects(Bucket=source_bucket, Prefix=source_prefix)
    file_paths_list = [f"s3://{source_bucket}/" + content_['Key'] for content_ in result['Contents'] if  content_['Key'].endswith(".parquet")]
    for file_ in file_paths_list:   
        current_df = wr.s3.read_parquet(path=file_)
    
    if not current_df.empty:
                logger.info(f"Writing data to s3://{target_bucket}/{prefix}")
                current_df.astype(str).to_parquet(
                    f's3://{target_bucket}/{prefix}',
                    partition_cols=["taxi_id"]
            )
                logger.info(f"Number of rows: {current_df.shape[0]}")

except Exception as e:
    logger.error("Failed when executing query with error:" , e)

job.commit()
