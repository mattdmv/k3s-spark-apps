import os
from dotenv import load_dotenv
from pyspark import SparkConf, SparkContext


load_dotenv()

MINIO_S3_HOST = os.environ["MINIO_S3_HOST"]
MINIO_S3_PORT = os.environ["MINIO_S3_PORT"]
NESSIE_HOST = os.environ["NESSIE_HOST"]
NESSIE_PORT = os.environ["NESSIE_PORT"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_REGION"]

def get_spark_config() -> SparkConf:
    return (
        SparkConf()
        .set("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "software.amazon.awssdk:bundle:2.20.131,"
            "software.amazon.awssdk:url-connection-client:2.20.131,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
            )
        .set("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") # Use Iceberg with Spark
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/nessie")
        .set("spark.sql.catalog.nessie.s3.endpoint", f"http://{MINIO_S3_HOST}:{MINIO_S3_PORT}")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog.nessie.uri", f"http://{NESSIE_HOST}:{NESSIE_PORT}/api/v1")
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.cache-enabled", "false")
    )

def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"{MINIO_S3_HOST}:{MINIO_S3_PORT}")
    spark_context._jsc.hadoopConfiguration().set("aws.region", AWS_REGION)
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")