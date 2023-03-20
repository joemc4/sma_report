from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col

spark = SparkSession.builder.appName("PySpark- put legacy and new r2 value sin to the same table").getOrCreate()

work_dir_new = r"/Users/joseph.mcmilllan/Documents/projects/s4c/slide_deck/r2_new_values"

work_dir_legacy = r"/Users/joseph.mcmilllan/Documents/projects/s4c/slide_deck/r2_legacy_values"

df_new = spark.read.parquet(work_dir_new)
df_legacy = spark.read.parquet(work_dir_legacy)

df_new = df_new.withColumn("model_version", lit("new"))
df_legacy = df_legacy.withColumn("model_version", lit("legacy"))

r2_merged_df = df_new.union(df_legacy)
r2_merged_df = r2_merged_df.orderBy("generated_metric_date")
r2_merged_df = r2_merged_df.drop("job_name")

r2_merged_df = r2_merged_df.withColumn("order", r2_merged_df.job_run_id.alias("order"))

r2_merged_df = r2_merged_df.drop("job_run_id")

# unique_jc = r2_merged_df.select("order").distinct().rdd.flatMap(lambda x: x).collect()
# # print each unique value
# for val in unique_jc:
#     print(val)

r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_7632c709cce3369429a9e05306f70623ecae1fcb5f14e1c58359572659fa8d08", "12").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_5116e2c038acad41551b39f18a3d4ee5d6bdbf6a04f70dbc9da87a712d64e57f", "6").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_9eb7b9758906ed816277b2afdca87ee874ec7465aba89ae55ed97bd6c9714725", "3").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_1122c4210b7370a5ca25edc50827fab3b36bd7d28ba9898cef35c1929d23fafe", "8").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_73678cc722c062b1ed37378250628c9daa39e9771adc696687d69c3db7cac2a5", "9").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_c0141199b533796b6f98afe3b2bda509fba6a26f1f8b0c75a752f3c5d9543051", "5").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_393724e3298504c1c2a38734c383c70c92d6c45ee600d8d99c30706ae6830371", "11").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_d93166b4d128c5c9a56ee01eddfbc0e83018e298e7c8abe0ce54105b47efa329", "4").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_d8a175f97daa4d0b818073e4e93fa5b46292e6d3da484516755355d0ec5f3033", "7").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_0379e11cddd46c23cd67c914eb97b2dd1dcc36c3809b757a15fbf1d5b7fc4351", "1").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_7b524a876bb3605a29985a6c8d4225c83715fd9372fe1ff9b3071bafa36a5711", "10").otherwise(r2_merged_df.order))
r2_merged_df = r2_merged_df.withColumn("order", when(r2_merged_df.order == "jr_3f274fb74cc4d07015bbd3caf5bd68257338a08ff0844d9710c7dbe536b0cf7e", "2").otherwise(r2_merged_df.order))

r2_merged_df = r2_merged_df.filter(col("percentage_data") > 10)

r2_merged_df.show()

x_axis = r2_merged_df.select("order").rdd.flatMap(lambda x: x).collect()

y_axis = r2_merged_df.select("r2").rdd.flatMap(lambda x: x).collect()

# Create a line graph with points
plt.plot(x_axis, y_axis, "-o")
plt.xlabel("Previously Generate Model Metrics")
plt.ylabel("R2")
plt.title("R2 since 15/01/2023")
plt.show()

# bar graph
plt.bar(x_axis, y_axis)
plt.xlabel("Previously Generate Model Metrics")
plt.ylabel("R2")
plt.title("R2 since 15/01/2023")
plt.show()