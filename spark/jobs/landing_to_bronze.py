import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

def create_spark_session():
    load_dotenv("/home/jovyan/.env")

    spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("landing_to_bronze")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_DOCKER"))
    .config("fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    .config("fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
    )

    return spark

def read_from_landing(spark, dataset_name):
    print(f"Reading schema from landing: {dataset_name}")

    sample_df = (
    spark.read
    .option("header", "true")
    .option("recursiveFileLookup", "true")
    .format("csv")
    .load(f"s3a://landingzone/{dataset_name}/")
    )
    sample_schema = sample_df.schema

    stream_df = (
    spark
    .readStream
    .schema(sample_schema)
    .option("header", "true")
    .option("recursiveFileLookup", "true")
    .format("csv")
    .load(f"s3a://landingzone/{dataset_name}")
    )

    return stream_df

def write_to_bronze(stream_df, dataset_name):
    print(f"Writing to bronze: {dataset_name}")

    bronze_df = (
    stream_df
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("source_file_name", input_file_name())
    )

    query = (
    bronze_df.writeStream
    .outputMode("append")
    .format("delta")
    .option("path", f"s3a://bronze/{dataset_name}")
    .option("checkpointLocation", f"s3a://bronze/_checkpoints/{dataset_name}")
    .trigger(availableNow=True)
    .start()
    )

    query.awaitTermination()
    print(f"Bronze write complete: {dataset_name}")

if __name__ == "__main__":
    dataset_name = sys.argv[1]
    spark = create_spark_session()
    stream_df = read_from_landing(spark, dataset_name)
    write_to_bronze(stream_df, dataset_name)