import boto3
import os
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.functions import mean, stddev, min, max, percentile_approx
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PySpark Read Parquet").getOrCreate()
work_dir = r"/Users/joseph.mcmilllan/Documents/projects/s4c/slide_deck/SMA_report/data/"

# s3 = boto3.resource("s3")
#
# my_bucket = s3.Bucket("sfc-update-locations-feature-engin-datasets")
#
# for file in my_bucket.objects.filter(
#         Prefix="domain=data_engineering/dataset=job_estimates/version=1.0.3/run_year=2023/run_month=03/run_day=07/"):
#     if file.key.endswith(".parquet"):
#         local_file_name = os.path.join(work_dir, file.key.split("/")[-1])
#
#         #print(f"Downloading{file.key} to {local_file_name}")
#         my_bucket.download_file(file.key, local_file_name)
#         #print(f"Finished {local_file_name}")

df = spark.read.parquet(work_dir)
df.show()

bnd_df = df.filter(df.locationid == "1-126362857")
bnd_df = bnd_df.orderBy(col("snapshot_date").asc())

quantiles = bnd_df.approxQuantile("estimate_job_count", [0.05, 0.95], 0.0)
print(quantiles)
# Filter out the bottom and top 2.5% of estimate_jobs
bnd_df_filter = bnd_df.filter((bnd_df.estimate_job_count > quantiles[0]) & (bnd_df.estimate_job_count < quantiles[1]))

# unique_jc = bnd_df.select("job_count").distinct().rdd.flatMap(lambda x: x).collect()
# # print each unique value
# for val in unique_jc:
#     print(val)

num_rows = bnd_df.count()

# print the total number of rows
print("Total number of rows in bnd_df: ", num_rows)

# show the new dataframe
bnd_df.show()

x_axis = bnd_df.select("snapshot_date").rdd.flatMap(lambda x: x).collect()
y_axis_EJC = bnd_df.select("estimate_job_count").rdd.flatMap(lambda x: x).collect()
y_axis_NOB = bnd_df.select("number_of_beds").rdd.flatMap(lambda x: x).collect()

plt.plot(x_axis, y_axis_EJC, label="EJC")
plt.plot(x_axis, y_axis_NOB, label="NOB")
plt.xlabel("snapshot_date")
plt.ylabel("jobs")
plt.title("snapshot_date x jobs + beds")
plt.legend()
plt.tight_layout()
plt.show()


desc_stats = bnd_df.select(
    mean("estimate_job_count"),
    stddev("estimate_job_count"),
    min("estimate_job_count"),
    max("estimate_job_count"),
    percentile_approx("estimate_job_count", [0.5]).alias("median"))

desc_stats.show()

# Define the window size for the rolling window
window_size = 35

# Define the window based on the estimate_job_count column and the specified window size
rolling_window = Window.orderBy('snapshot_date').rowsBetween(-(window_size - 1), 0)

# Create a new column with the SMA smoothed version of estimate_job_count
bnd_df = bnd_df.withColumn('estimate_job_count_sma', avg(col('estimate_job_count')).over(rolling_window))

bnd_df.show()

y_axis_SMA = bnd_df.select("estimate_job_count_sma").rdd.flatMap(lambda x: x).collect()

plt.plot(x_axis, y_axis_EJC, label="EJC")
plt.plot(x_axis, y_axis_NOB, label="NOB")
plt.plot(x_axis, y_axis_SMA, label="SMA")
plt.xlabel("snapshot_date")
plt.ylabel("jobs")
plt.title("snapshot_date x jobs + beds + SMA")
plt.legend()
plt.tight_layout()
plt.show()

# Convert Spark DataFrame to Pandas DataFrame
pandas_df = bnd_df.select('estimate_job_count', 'snapshot_date', 'estimate_job_count_sma' ).toPandas()

# Create scatterplot
pandas_df.plot.scatter(x='snapshot_date', y='estimate_job_count')
plt.xlabel("snapshot_date")
plt.ylabel("Jobs per Point")
plt.title("snapshot_date x jobs scatter")
plt.legend()
plt.tight_layout()
plt.show()

pandas_df.plot.scatter(x='snapshot_date', y='estimate_job_count_sma')
plt.xlabel("snapshot_date")
plt.ylabel("Jobs per Point - SMA")
plt.title("snapshot_date x jobs scatter - SMA")
plt.legend()
plt.tight_layout()
plt.show()

bnd_df_filter = bnd_df_filter.withColumn('estimate_job_count_sma', avg(col('estimate_job_count')).over(rolling_window))

x_axis_fil = bnd_df_filter.select("snapshot_date").rdd.flatMap(lambda x: x).collect()
y_axis_EJC_fil = bnd_df_filter.select("estimate_job_count").rdd.flatMap(lambda x: x).collect()
y_axis_NOB_fil = bnd_df_filter.select("number_of_beds").rdd.flatMap(lambda x: x).collect()
y_axis_SMA_fil = bnd_df_filter.select("estimate_job_count_sma").rdd.flatMap(lambda x: x).collect()


plt.plot(x_axis, y_axis_EJC, label="EJC")
plt.plot(x_axis, y_axis_NOB, label="NOB")
plt.plot(x_axis, y_axis_SMA, label="SMA")
plt.plot(x_axis_fil, y_axis_SMA_fil, label="SMAFIL")
plt.xlabel("snapshot_date")
plt.ylabel("jobs")
plt.title("snapshot_date x jobs+beds+SMA+SMAFIL")
plt.legend()
plt.tight_layout()
plt.show()


# Convert Spark DataFrame to Pandas DataFrame
pandas_df_filter = bnd_df_filter.select('estimate_job_count', 'snapshot_date', 'estimate_job_count_sma' ).toPandas()

# Create scatterplot
pandas_df_filter.plot.scatter(x='snapshot_date', y='estimate_job_count')
plt.xlabel("snapshot_date")
plt.ylabel("Jobs per Point")
plt.title("snapshot_date x jobs scatter - (filter)")
plt.tight_layout()
plt.show()

pandas_df_filter.plot.scatter(x='snapshot_date', y='estimate_job_count_sma')
plt.xlabel("snapshot_date")
plt.ylabel("Jobs per Point - SMA")
plt.title("snapshot_date x jobs scatter - SMA (filter)")
plt.tight_layout()
plt.show()

plt.plot(x_axis_fil, y_axis_EJC_fil, label="EJCFIL")
plt.plot(x_axis_fil, y_axis_NOB_fil, label="NOBFIL")
plt.plot(x_axis_fil, y_axis_SMA_fil, label="SMAFIL")
plt.xlabel("snapshot_date")
plt.ylabel("jobs")
plt.title("snapshot_date x jobs+beds+SMA+SMAFIL")
plt.legend()
plt.tight_layout()
plt.show()