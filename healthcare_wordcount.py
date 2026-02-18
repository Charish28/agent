from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, regexp_replace

spark = SparkSession.builder.appName("Healthcare-ELT-Dataproc").getOrCreate()

df = spark.read.option("header", True).csv(
    "gs://agentic-elt-dataproc/input/Healthcare_DataSet.csv"
)

text_df = df.select(lower(col("tweets")).alias("text"))

clean_df = text_df.select(
    regexp_replace(col("text"), "[^a-zA-Z\\s]", "").alias("text")
)

words = clean_df.select(
    explode(split(col("text"), "\\s+")).alias("word")
)

words = words.filter(col("word") != "")

word_count = words.groupBy("word").count()

word_count.write \
    .format("bigquery") \
    .option("table", "sss-gov.Agentic_pipeline.healthcare_wordcount") \
    .mode("overwrite") \
    .save()

spark.stop()