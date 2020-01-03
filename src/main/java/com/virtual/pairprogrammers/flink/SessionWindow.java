package com.virtual.pairprogrammers.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SessionWindow
{
    public static void main(String ...args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Tuple2<Long, String>> sum = data.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String value) throws Exception {
                String[] words = value.split(",");
                return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Long, String> longStringTuple2) {
                return longStringTuple2.f0;
            }
        })
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> reduce(Tuple2<Long, String> current, Tuple2<Long, String> pre_result) throws Exception {
                        return new Tuple2<Long, String>(current.f0, current.f1);
                    }
                });

        sum.writeAsText("tumblingwindowout");


        env.execute("Session Window");
    }
}
