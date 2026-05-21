from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    # 1. Spark 세션 초기화 (Kafka 연동용 패키지 내장 지정)
    spark = SparkSession.builder \
        .appName("Realtime-FDS-Processor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. 하이브리드 데이터 스키마 정의 (Generator가 쏘는 JSON 구조와 일치해야 함)
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
    # V1 ~ V28 피처 동적 추가
    for i in range(1, 29):
        schema_fields.append(StructField(f"v_{i}", DoubleType(), True))
        
    transaction_schema = StructType(schema_fields)

    # 3. Kafka Topic으로부터 스트리밍 데이터 읽기
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load()

    # Kafka의 value(Binary)를 String으로 변환 후 JSON 파싱
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), transaction_schema).alias("data")) \
        .select("data.*")

    # 타임스탬프 문자열을 Timestamp 타입으로 변환
    processed_df = parsed_df.withColumn("tx_timestamp", col("timestamp").cast("timestamp"))

    # 4. 실시간 윈도우 집계 (Feature Engineering)
    # 워터마크를 10분으로 설정하여 10분 이상 늦게 도착한 데이터는 폐기함
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
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("tx_count_last_10m"),
            col("total_amount_last_10m")
        )

    # 5. 실시간 연산 결과 콘솔 출력 (검증용 Sink)
    query = windowed_features.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()