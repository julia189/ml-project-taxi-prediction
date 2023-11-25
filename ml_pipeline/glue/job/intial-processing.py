import boto3 
import awswrangler as wr
import logging
from datetime import datetime

session = boto3.Session(profile_name='Terraform_Playground')
s3_client = session.client("s3")
source_bucket = "think-tank-casestudy"
source_prefix = "raw_data"
target_bucket = "test-database-jh"
target_prefix = "data/ingestion_data/trips"

result = s3_client.list_objects(Bucket=source_bucket, Prefix=source_prefix)
file_paths_list = [f"s3://{source_bucket}/" + content_['Key'] for content_ in result['Contents'] if  content_['Key'].endswith(".parquet")]
for file_ in file_paths_list:
    current_df = wr.s3.read_parquet(path=file_, boto3_session=session)
    current_df['DATE'] = current_df["TIMESTAMP"].apply(
                lambda value_unix: datetime.fromtimestamp(value_unix).date())
    current_df["TRIP_ID"]  = current_df["TRIP_ID"].astype(object)
    current_df["TAXI_ID"]  = current_df["TAXI_ID"].astype(object)
    current_df["CALL_TYPE"]  = current_df["CALL_TYPE"].astpye(object)
    current_df["ORIGIN_CALL"]  = current_df["ORIGIN_CALL"].astype(object)
    current_df["ORIGIN_STAND"]  = current_df["ORIGIN_STAND"].astype(object)
    

    if not current_df.empty:
        logging.info(f"Writing data to s3://{target_bucket}/{target_prefix}")
        wr.s3.to_parquet(df=current_df,
                         path=f's3://{target_bucket}/{target_prefix}',
                         dataset=True,
                         boto3_session=session,
                         partition_cols=["DATE"])
    break