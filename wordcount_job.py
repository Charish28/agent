from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col

spark = SparkSession.builder.appName("ELT-dataproc").getOrCreate()

# Load CSV
df = spark.read.option("header", True).csv(
    "gs://agentic-elt-dataproc/input/*.csv"
)

# Assume column name = text
text_df = df.select(lower(col("text")).alias("text"))

# Split into words
words = text_df.select(
    explode(split(col("text"), "\\s+")).alias("word")
)

# Word Count
word_count = words.groupBy("word").count()

# Save to BigQuery (LOAD phase)
word_count.write \
    .format("bigquery") \
    .option("table", "sss-gov.Agentic_pipeline.wordcount") \
    .mode("overwrite") \
    .save()

spark.stop()