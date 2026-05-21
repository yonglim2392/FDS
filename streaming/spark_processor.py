from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def write_to_redis_partition(partition):
    """
    [생산성 튜닝] 파티션 단위로 Redis 커넥션을 단 한 번만 생성하여 
    네트워크 오버헤드를 극단적으로 줄이며 피처를 적재함.
    """
    import redis
    
    # Redis 컨테이너에 연결 (WSL2 로컬 호스트 포트 포워딩 활용)
    r = redis.Redis(host='localhost', port=6379, db=0)
    
    for row in partition:
        # Key 포맷: fds:user:{user_id}
        redis_key = f"fds:user:{row['user_id']}"
        
        # 피처 데이터를 Hash 구조로 저장
        r.hset(redis_key, mapping={
            "tx_count_last_10m": str(row['tx_count_last_10m']),
            "total_amount_last_10m": str(row['total_amount_last_10m']),
            "updated_at": str(row['window_end'])
        })
        
        # [데이터 관리] 10분이 지난 피처는 FDS 추론에 무의미하므로 
        # Redis 메모리 관리를 위해 20분(1200초) 후 자동 삭제(TTL) 설정
        r.expire(redis_key, 1200)

def main():
    spark = SparkSession.builder \
        .appName("Realtime-FDS-Processor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 스키마 정의
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

    # Kafka 읽기
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

    # 윈도우 집계
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

    # [수정 구간] 콘솔이 아닌 Redis로 스트리밍 데이터를 Sink 함
    # outputMode는 실시간 데이터가 업데이트/추가될 때마다 반영하도록 "update" 설정
    query = windowed_features.writeStream \
        .foreachBatch(lambda df, epoch_id: df.foreachPartition(write_to_redis_partition)) \
        .outputMode("update") \
        .start()

    print("📥 Spark Streaming이 실시간 피처를 Redis 피처 스토어에 적재 중입니다...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
