import boto3
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark Read Parquet").getOrCreate()

s3 = boto3.resource("s3")

my_bucket = s3.Bucket("sfc-main-datasets")
work_dir = r"/Users/joseph.mcmilllan/Documents/projects/s4c/slide_deck/model_metrics"

for file in my_bucket.objects.filter(Prefix="domain=data_engineering/dataset=model_metrics/model_name=care_home_with_nursing_historical_jobs_prediction/model_version=1.0.2/"):
    if file.key.endswith(".parquet"):
        local_file_name = os.path.join(work_dir, file.key.split("/")[-1])

        print(f"Downloading{file.key} to {local_file_name}")
        my_bucket.download_file(file.key,local_file_name)
        print(f"Finished {local_file_name}")


df = spark.read.parquet(work_dir)
df.show()