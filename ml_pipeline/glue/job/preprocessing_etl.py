import datetime
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


client = boto3.client("athena", region_name='eu-west-1')
output_path = "s3://aws-athena-query-results-177474864456-eu-west-1"
db = "default"
env = args.get("env", "dev")
bucket = args.get("bucket", f"")
prefix = args.get("prefix", "s")
start_date = args.get("start_date", (datetime.now() - pd.Timedelta(value=1, unit="day")).strftime("%Y-%m-%d"))
end_date = datetime.now().strftime("%Y-%m-%d")


try:
    sql_query = Template(sql_query).substitute(
        start_date=start_date,
        end_date=end_date
    )
    print(sql_query)

    df = athena_query(client=client,
                      query_string=sql_query,
                      database_name=db,
                      output_path=output_path,
                      max_execution_sec=30)
    if not df.empty:
        print(f"Writing data to s3://{bucket}/{prefix}")
        start = time.time()
        df.astype(str).to_parquet(
            f's3://{bucket}/{prefix}',
            partition_cols=["snapshot_year", "snapshot_month", "snapshot_day"]
        )

except Exception:
    logging.exception("read_missing_parts_sql_query")

print(f"Number of rows: {df.shape[0]}")
print(f"Data table queried at {datetime.now()}")
