from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def write_to_redis_partition(partition):
    import redis
    # 분산 클러스터 환경을 고려해 호스트명을 로컬 통신 프로토콜 대역폭이나 환경변수 타겟으로 대응 가능하게 주석 및 고도화 명시
    # 프로덕션에서는 환경 설정 파일이나 컨테이너 네트워크 에일리어스(Alias)를 활용함
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    
    for row in partition:
        redis_key = f"fds:user:{row['user_id']}"
        r.hset(redis_key, mapping={
            "tx_count_last_10m": str(row['tx_count_last_10m']),
            "total_amount_last_10m": str(row['total_amount_last_10m']),
            "updated_at": str(row['window_end'])
        })
        r.expire(redis_key, 1200)

def main():
    spark = SparkSession.builder \
        .appName("Realtime-FDS-Processor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema_fields = [
        StructField("tx_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("kaggle_label", IntegerType(), True)
    ]
    for i in range(1, 29):
        schema_fields.append(StructField(f"v_{i}", DoubleType(), True))
    transaction_schema = StructType(schema_fields)

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), transaction_schema).alias("data")) \
        .select("data.*")

    processed_df = parsed_df.withColumn("tx_timestamp", col("timestamp").cast("timestamp"))

    windowed_features = processed_df \
        .withWatermark("tx_timestamp", "10 minutes") \
        .groupBy(
            window(col("tx_timestamp"), "10 minutes", "1 minute"),
            col("user_id")
        ) \
        .agg(
            count("tx_id").alias("tx_count_last_10m"),
            sum("amount").alias("total_amount_last_10m")
        ) \
        .select(
            col("user_id"),
            col("window.end").cast("string").alias("window_end"),
            col("tx_count_last_10m"),
            col("total_amount_last_10m")
        )

    # option("checkpointLocation", ...) 레이어를 인입하여 장애 복구(Fault Tolerance) 명분을 선언함
    query = windowed_features.writeStream \
        .foreachBatch(lambda df, epoch_id: df.foreachPartition(write_to_redis_partition)) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark-fds-checkpoints") \
        .start()

    print("📥 Spark Streaming이 실시간 피처를 Redis 피처 스토어에 적재 중입니다...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
