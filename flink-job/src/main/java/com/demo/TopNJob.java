package com.demo;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

public class TopNJob {

    private static final int TOP_N = 5;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("user-behavior")
                .setGroupId("topn-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 2. 解析 → 过滤 click → Watermark → 10s窗口 → 按视频统计PV
        DataStream<Tuple3<String, Long, Timestamp>> perVideoPv = stream
                .map(json -> {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(json);
                    String videoId = node.get("video_id").asText();
                    String action = node.get("action").asText();
                    long ts = node.get("timestamp").asLong() * 1000;
                    return Tuple3.of(videoId, action, ts);
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))

                .filter(tp -> tp.f1.equals("click"))

                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Tuple3<String, String, Long>>)
                                                (element, recordTimestamp) -> element.f2)
                )

                .keyBy(tp -> tp.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple3<String, String, Long>, Tuple3<String, Long, Timestamp>, String, TimeWindow>() {
                    @Override
                    public void apply(String key,
                                      TimeWindow window,
                                      Iterable<Tuple3<String, String, Long>> input,
                                      Collector<Tuple3<String, Long, Timestamp>> out) {
                        long count = 0;
                        for (Tuple3<String, String, Long> ignored : input) {
                            count++;
                        }
                        out.collect(Tuple3.of(key, count, new Timestamp(window.getEnd())));
                    }
                });

        // 3. 按窗口结束时间分组 → 1s窗口 → 排序取 Top5
        perVideoPv
                .keyBy(tp -> tp.f2) // keyBy window_end
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(new WindowFunction<Tuple3<String, Long, Timestamp>, Tuple4<Timestamp, Integer, String, Long>, Timestamp, TimeWindow>() {
                    @Override
                    public void apply(Timestamp windowEndKey,
                                      TimeWindow window,
                                      Iterable<Tuple3<String, Long, Timestamp>> input,
                                      Collector<Tuple4<Timestamp, Integer, String, Long>> out) {
                        // 收集所有视频的PV
                        List<Tuple2<String, Long>> list = new ArrayList<>();
                        for (Tuple3<String, Long, Timestamp> tp : input) {
                            list.add(Tuple2.of(tp.f0, tp.f1));
                        }

                        // 按PV降序排序
                        list.sort((a, b) -> Long.compare(b.f1, a.f1));

                        // 取Top5
                        for (int i = 0; i < Math.min(TOP_N, list.size()); i++) {
                            Tuple2<String, Long> item = list.get(i);
                            // window_end, rank, video_id, pv
                            out.collect(Tuple4.of(windowEndKey, i + 1, item.f0, item.f1));
                        }
                    }
                })

                // 4. 写入 ClickHouse
                .addSink(new TopNClickHouseSink())
                .name("TopN ClickHouse Sink");

        env.execute("TopN Job → ClickHouse");
    }

    // 自定义 Sink：写入热门视频 TopN
    public static class TopNClickHouseSink extends RichSinkFunction<Tuple4<Timestamp, Integer, String, Long>> {

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
                "CREATE TABLE IF NOT EXISTS hot_videos_topn (" +
                "  window_end DateTime," +
                "  rank        UInt32," +
                "  video_id    String," +
                "  pv          UInt64" +
                ") ENGINE = MergeTree() ORDER BY (window_end, rank)"
            );

            pstmt = conn.prepareStatement(
                "INSERT INTO hot_videos_topn VALUES (?, ?, ?, ?)"
            );
        }

        @Override
        public void invoke(Tuple4<Timestamp, Integer, String, Long> value, Context context) throws Exception {
            pstmt.setTimestamp(1, value.f0);
            pstmt.setInt(2, value.f1);
            pstmt.setString(3, value.f2);
            pstmt.setLong(4, value.f3);
            pstmt.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            if (pstmt != null) pstmt.close();
            if (conn != null) conn.close();
        }
    }
}
