from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DateType

import os

spark = SparkSession.builder.appName("test").getOrCreate()
path = '/usr/local/airflow/data/20240419134448.json'

schema = StructType([
    StructField("_id", StringType(), True),
    StructField("content", StringType(), True),
    StructField("author", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("authorSlug", StringType(), True),
    StructField("length", IntegerType(), True),
    StructField("dateAdded", StringType(), True),
    StructField("dateModified", StringType(), True)
])

# Read JSON file as text
json_text = spark.read.text(path)

# Explode JSON array and parse JSON objects using schema
exploded_df = json_text.select(explode(from_json("value", ArrayType(schema))).alias("data"))

# Show DataFrame schema
exploded_df.printSchema()

# Show DataFrame content
exploded_df.show(truncate=False)



df = spark.read.json(path)

# Show DataFrame schema
df.printSchema()

# Show DataFrame content
df.show(truncate=False)