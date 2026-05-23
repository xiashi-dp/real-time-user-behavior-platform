-- ============================================
-- 离线数仓 Hive 初始化 DDL
-- ODS (原始数据) → DWD (明细数据) → DWS (汇总数据)
-- ============================================

-- ========== ODS 层：原始数据 ==========
DROP TABLE IF EXISTS ods_user_behavior;
CREATE TABLE ods_user_behavior (
    raw_json STRING
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET;

-- ========== DWD 层：清洗后明细数据 ==========
DROP TABLE IF EXISTS dwd_event_detail;
CREATE TABLE dwd_event_detail (
    user_id     INT,
    video_id    STRING,
    action      STRING,
    event_ts    BIGINT,
    event_time  TIMESTAMP
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET;

-- ========== DWS 层：每日视频汇总 ==========
DROP TABLE IF EXISTS dws_video_daily;
CREATE TABLE dws_video_daily (
    video_id     STRING,
    pv           BIGINT,
    uv           BIGINT,
    like_count   BIGINT,
    comment_count BIGINT,
    share_count  BIGINT
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET;

-- ========== ADS 层：热门视频 TopN（可选） ==========
DROP TABLE IF EXISTS ads_hot_videos;
CREATE TABLE ads_hot_videos (
    video_id   STRING,
    pv         BIGINT,
    rank       INT
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET;
