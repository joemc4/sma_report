import boto3
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark Read Parquet of r2 from the new model").getOrCreate()
work_dir = r"/Users/joseph.mcmilllan/Documents/projects/s4c/slide_deck/r2_new_values"

s3 = boto3.resource("s3")

my_bucket = s3.Bucket("sfc-care-home-features-v2-datasets")

for file in my_bucket.objects.filter(Prefix="domain=data_engineering/dataset=model_metrics/model_name=./model_version=2.0.0/"):
    if file.key.endswith(".parquet"):
        local_file_name = os.path.join(work_dir, file.key.split("/")[-1])

        print(f"Downloading{file.key} to {local_file_name}")
        my_bucket.download_file(file.key,local_file_name)
        print(f"Finished {local_file_name}")

df = spark.read.parquet(work_dir)
df.show()