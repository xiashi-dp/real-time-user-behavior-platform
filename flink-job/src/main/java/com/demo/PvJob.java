package com.demo;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;

public class PvJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("user-behavior")
                .setGroupId("pv-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 2. 解析 JSON → 过滤 click → Watermark → 10s窗口 → PV+UV → ClickHouse
        stream
                .map(json -> {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(json);
                    String videoId = node.get("video_id").asText();
                    String action = node.get("action").asText();
                    long ts = node.get("timestamp").asLong() * 1000;
                    int userId = node.get("user_id").asInt();
                    return Tuple4.of(videoId, action, ts, userId);
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG, Types.INT))

                // 3. 只保留 click
                .filter(tp -> tp.f1.equals("click"))

                // 4. Watermark（允许 5 秒乱序）
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple4<String, String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Tuple4<String, String, Long, Integer>>)
                                                (element, recordTimestamp) -> element.f2)
                )

                // 5. 按 video_id 分组 → 10秒窗口 → PV + UV
                .keyBy(tp -> tp.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple4<String, String, Long, Integer>, Tuple4<String, Long, Long, Timestamp>, String, TimeWindow>() {
                    @Override
                    public void apply(String key,
                                      TimeWindow window,
                                      Iterable<Tuple4<String, String, Long, Integer>> input,
                                      Collector<Tuple4<String, Long, Long, Timestamp>> out) {
                        long pv = 0;
                        HashSet<Integer> uvSet = new HashSet<>();
                        for (Tuple4<String, String, Long, Integer> tuple : input) {
                            pv++;
                            uvSet.add(tuple.f3);
                        }
                        out.collect(Tuple4.of(key, pv, (long) uvSet.size(), new Timestamp(window.getEnd())));
                    }
                })

                // 6. 写入 ClickHouse
                .addSink(new ClickHouseSink())
                .name("ClickHouse Sink");

        env.execute("PV Job → ClickHouse");
    }

    // 自定义 Sink：写入 ClickHouse
    public static class ClickHouseSink extends RichSinkFunction<Tuple4<String, Long, Long, Timestamp>> {

        private Connection conn;
        private PreparedStatement pstmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            String url = "jdbc:clickhouse://clickhouse:8123/flink_db";
            Properties props = new Properties();
            props.setProperty("user", "default");
            props.setProperty("password", "123456");
            ClickHouseDataSource dataSource = new ClickHouseDataSource(url, props);
            conn = dataSource.getConnection();

            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS video_pv (" +
                "  video_id   String," +
                "  pv         UInt64," +
                "  uv         UInt64," +
                "  window_end DateTime" +
                ") ENGINE = MergeTree() ORDER BY window_end"
            );

            pstmt = conn.prepareStatement(
                "INSERT INTO video_pv VALUES (?, ?, ?, ?)"
            );
        }

        @Override
        public void invoke(Tuple4<String, Long, Long, Timestamp> value, Context context) throws Exception {
            pstmt.setString(1, value.f0);   // video_id
            pstmt.setLong(2, value.f1);     // pv
            pstmt.setLong(3, value.f2);     // uv
            pstmt.setTimestamp(4, value.f3); // window_end
            pstmt.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            if (pstmt != null) pstmt.close();
            if (conn != null) conn.close();
        }
    }
}
