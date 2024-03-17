from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Parquet to CSV Conversion").getOrCreate()

# Read the Parquet file
parquet_df = spark.read.parquet("/user/maria_dev/dip")

# Write the DataFrame to a CSV file
parquet_df.write \
    .option("header", "true").csv("/user/maria_dev/dip_csv")

# Stop the SparkSession
spark.stop()
