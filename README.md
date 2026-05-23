# 实时用户行为分析平台 + 离线数仓

## 架构

```
实时 Pipeline:
  Producer → Kafka → Flink (PV/UV + TopN) → ClickHouse → Grafana

离线 Pipeline:
  Producer → Kafka → Spark (ETL) → Hive (ODS → DWD → DWS → ADS)  → Airflow (调度)
```

## 技术栈

| 组件 | 用途 |
|------|------|
| **Kafka** | 消息队列，接收用户行为数据 |
| **Flink** | 实时计算 PV/UV 和热门视频 TopN |
| **ClickHouse** | 实时 OLAP 存储，秒级查询 |
| **Grafana** | 实时可视化大屏 |
| **Spark** | 离线 ETL 批处理 |
| **Hive** | 离线数仓分层存储 |
| **HDFS** | 分布式文件系统 |
| **Airflow** | 离线任务调度 |
| **Docker** | 容器化部署 |

## 快速启动

### 实时 Pipeline

```bash
cd docker
docker compose up -d
```

### 提交 Flink 任务

```bash
# 复制 jar 到容器
docker cp flink-job/target/flink-job-1.0.jar jobmanager:/opt/flink/job.jar

# 提交 PV/UV 任务
docker exec jobmanager flink run /opt/flink/job.jar --job PvJob

# 提交 TopN 任务
docker exec jobmanager flink run /opt/flink/job.jar --job TopNJob
```

### 启动数据模拟

```bash
cd producer
pip install kafka-python
python producer.py
```

### 离线 Pipeline

```bash
cd docker
docker compose -f docker-compose-offline.yml up -d
```

访问 Airflow (http://localhost:8082, admin/admin) 手动触发 `offline_etl_dag` 即可运行离线 ETL。

## 数仓分层

| 层 | 表名 | 说明 |
|----|------|------|
| ODS | `ods_user_behavior` | 原始 JSON 数据 |
| DWD | `dwd_event_detail` | 解析清洗后的明细数据 |
| DWS | `dws_video_daily` | 每日视频 PV/UV 汇总 |
| ADS | `ads_hot_videos` | 每日热门 Top10 |

## Flink 实时任务

- **PvJob**: 统计每个视频的 PV 和 UV，10 秒窗口，写入 ClickHouse
- **TopNJob**: 两层窗口计算全局热门视频 Top5，写入 ClickHouse

## 端口映射

| 服务 | 端口 |
|------|------|
| Kafka | 9092 / 29092 |
| Flink Web UI | 8081 |
| ClickHouse | 8123 |
| Grafana | 3000 |
| HDFS NameNode | 9870 |
| HiveServer2 | 10000 |
| Spark Master | 8083 / 7077 |
| Airflow | 8082 |
