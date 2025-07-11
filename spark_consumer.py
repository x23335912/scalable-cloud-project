
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("KinesisTextProcessor") \
    .getOrCreate()

schema = StructType().add("text", StringType())

df = spark.readStream \
    .format("kinesis") \
    .option("streamName", "text-stream") \
    .option("region", "us-east-1") \
    .option("initialPosition", "LATEST") \
    .load()

json_df = df.selectExpr("CAST(data AS STRING)") \
            .select(from_json(col("data"), schema).alias("parsed")) \
            .select("parsed.text")

# Simple word count
words = json_df.selectExpr("explode(split(text, ' ')) as word")
word_count = words.groupBy("word").count()

query = word_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
