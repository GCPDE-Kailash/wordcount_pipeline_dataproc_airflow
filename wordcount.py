from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, length

spark = SparkSession.builder.appName("WordCount").getOrCreate()

input_path = "gs://airflow-wordcount-bucket/input/readme.md"
output_path = "gs://airflow-wordcount-bucket/output/fixed_wordcount_result"

# Read and clean text
lines = spark.read.text(input_path)

words = lines.select(
    explode(
        split(lower(col("value")), "\\W+")
    ).alias("word")
)

# Filter out empty/short words
filtered = words.filter((col("word") != "") & (length(col("word")) > 1))

word_counts = filtered.groupBy("word").count().orderBy("count", ascending=False)

word_counts.write.mode("overwrite").csv(output_path)

spark.stop()
