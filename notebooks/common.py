from pyspark.sql import SparkSession

BASE = "s3a://lake"

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "io.delta:delta-spark_2.12:3.2.0",
    "graphframes:graphframes:0.8.3-spark3.5-s_2.12",
])


def get_spark(app="fastfood", packages=True, extra_packages=None):
    """SparkSession connectée au cluster standalone, avec accès MinIO (S3A).

    packages=False : session légère (Partie 1), sans téléchargement Maven.
    """
    b = (SparkSession.builder.appName(app)
         .master("spark://spark-master:7077")
         .config("spark.driver.host", "jupyter")
         .config("spark.driver.bindAddress", "0.0.0.0")
         .config("spark.cores.max", "4")
         .config("spark.executor.memory", "1g")
         .config("spark.sql.shuffle.partitions", "6"))
    if packages:
        b = (b.config("spark.jars.packages", PACKAGES)
             .config("spark.jars.ivy", "/tmp/ivy")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
             .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
             .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    return b.getOrCreate()
