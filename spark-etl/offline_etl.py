"""
离线数仓 ETL 脚本 (PySpark)

数据流：
  Kafka (user-behavior topic)
    → ODS (原始 JSON，按天分区)
    → DWD (解析/清洗，Parquet 格式)
    → DWS (每日视频聚合: PV/UV)
    → ADS (每日热门 Top10)

用法：
  spark-submit \
    --master spark://spark-master:7077 \
    offline_etl.py \
    --date 2026-05-09
"""

import argparse
import random
import time as time_module
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, from_json, from_unixtime, to_date
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, TimestampType


# Kafka 消息 JSON schema
EVENT_SCHEMA = StructType() \
    .add("user_id", IntegerType()) \
    .add("video_id", StringType()) \
    .add("action", StringType()) \
    .add("timestamp", LongType())  # 单位：秒


def create_spark_session():
    """创建启用 Hive 支持的 SparkSession"""
    return SparkSession.builder \
        .appName("Offline Data Warehouse ETL") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .enableHiveSupport() \
        .getOrCreate()


def extract_from_kafka(spark, date_str):
    """
    生成模拟用户行为数据（生产环境替换为 Kafka 消费者）。
    """
    print(f"[INFO] 生成演示数据日期: {date_str}")

    actions = ["click", "click", "click", "click", "click",
               "like", "like", "comment", "share"]
    users = list(range(1, 51))   # 50 个用户
    videos = [f"v{i:03d}" for i in range(1, 101)]  # 100 个视频

    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    base_ts = int(time_module.mktime(target_date.timetuple()))
    random.seed(hash(date_str))  # 同一天生成相同数据

    rows = []
    for _ in range(2000):
        ts = base_ts + random.randint(0, 86399)
        user = random.choice(users)
        video = random.choice(videos)
        action = random.choice(actions)
        rows.append(
            f'{{"user_id":{user},"video_id":"{video}","action":"{action}","timestamp":{ts}}}'
        )

    raw_rdd = spark.sparkContext.parallelize(rows)
    raw_df = raw_rdd.map(lambda r: Row(raw_json=r)).toDF()
    return raw_df


def ods_to_dwd(spark, date_str):
    """ODS → DWD：解析 JSON、清洗、写入明细表"""
    print(f"[INFO] ODS → DWD: {date_str}")

    ods_df = spark.sql(f"""
        SELECT raw_json
        FROM ods_user_behavior
        WHERE event_date = '{date_str}'
    """)

    parsed = ods_df \
        .select(from_json(col("raw_json"), EVENT_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType())) \
        .withColumn("event_date", to_date(from_unixtime(col("timestamp"))))

    # 过滤无效数据
    cleaned = parsed.filter(col("user_id").isNotNull()) \
        .filter(col("action").isin("click", "like", "comment", "share"))

    # 用 Spark SQL INSERT OVERWRITE 写入 Hive 表
    cleaned.createOrReplaceTempView("tmp_dwd")
    spark.sql(f"""
        INSERT OVERWRITE TABLE dwd_event_detail PARTITION (event_date='{date_str}')
        SELECT user_id, video_id, action, timestamp AS event_ts, event_time
        FROM tmp_dwd
    """)
    spark.sql("DROP VIEW IF EXISTS tmp_dwd")

    print(f"[INFO] DWD 写入完成")


def dwd_to_dws(spark, date_str):
    """DWD → DWS：按 video_id 聚合 PV/UV"""
    print(f"[INFO] DWD → DWS: {date_str}")

    spark.sql(f"""
        INSERT OVERWRITE TABLE dws_video_daily PARTITION (event_date='{date_str}')
        SELECT
            video_id,
            SUM(CASE WHEN action='click' THEN 1 ELSE 0 END) AS pv,
            COUNT(DISTINCT user_id) AS uv,
            SUM(CASE WHEN action='like' THEN 1 ELSE 0 END) AS like_count,
            SUM(CASE WHEN action='comment' THEN 1 ELSE 0 END) AS comment_count,
            SUM(CASE WHEN action='share' THEN 1 ELSE 0 END) AS share_count
        FROM dwd_event_detail
        WHERE event_date = '{date_str}'
        GROUP BY video_id
    """)
    print(f"[INFO] DWS 写入完成")


def dws_to_ads(spark, date_str):
    """DWS → ADS：每日热门 Top10"""
    print(f"[INFO] DWS → ADS: {date_str}")

    spark.sql(f"""
        INSERT OVERWRITE TABLE ads_hot_videos PARTITION (event_date='{date_str}')
        SELECT video_id, pv, row_number() OVER (ORDER BY pv DESC) AS rank
        FROM dws_video_daily
        WHERE event_date = '{date_str}'
        LIMIT 10
    """)
    print(f"[INFO] ADS 写入完成")

    # 打印结果
    print("\n=== 每日热门 Top10 ===")
    spark.sql(f"SELECT * FROM ads_hot_videos WHERE event_date='{date_str}' ORDER BY rank").show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="ETL 处理日期 yyyy-MM-dd")
    args = parser.parse_args()
    date_str = args.date

    spark = create_spark_session()
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

    # Step 1: 从 Kafka 抽取 → ODS
    raw_df = extract_from_kafka(spark, date_str)
    raw_df.createOrReplaceTempView("tmp_raw")
    spark.sql(f"""
        INSERT INTO ods_user_behavior PARTITION (event_date='{date_str}')
        SELECT raw_json FROM tmp_raw
    """)
    spark.sql("DROP VIEW IF EXISTS tmp_raw")
    print(f"[INFO] ODS 写入完成, 共 2000 条")

    # Step 2: ODS → DWD
    ods_to_dwd(spark, date_str)

    # Step 3: DWD → DWS
    dwd_to_dws(spark, date_str)

    # Step 4: DWS → ADS (Top10)
    dws_to_ads(spark, date_str)

    print("[INFO] 离线 ETL 全部完成!")


if __name__ == "__main__":
    main()
