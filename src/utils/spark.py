from pyspark.sql import SparkSession

def get_spark(app_name: str, shuffle_partitions: int = 10) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark