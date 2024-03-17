import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Parquet to CSV Conversion")
  .getOrCreate()

// Read the Parquet file
val parquetDF = spark.read.parquet("/user/maria_dev/dip")

// Write the DataFrame to a CSV file
parquetDF.write
  .option("header", "true") // Write header in the CSV file
  .csv("/user/maria_dev/dip_csv")

spark.stop()
