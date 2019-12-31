package com.virtual.pairprogrammers.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;

public class ReduceOperation
{
    public static void main(String ... args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("/home/path");

        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new MapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
            @Override
            public Tuple5<String, String, String, Integer, Integer> map(String value) throws Exception {
                String[] words = value.split(",");
                return new Tuple5<String, String, String, Integer, Integer>(words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
            }
        });

        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped.keyBy(0).reduce(new ReduceFunction<Tuple5<String, String, String, Integer, Integer>>() {
            @Override
            public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current, Tuple5<String, String, String, Integer, Integer> pre_result) throws Exception {
                return new Tuple5<String, String, String, Integer, Integer> (current.f0, current.f1, current.f2,  current.f3 + pre_result.f3, current.f4 + pre_result.f4);
            }
        });

        DataStream<Tuple2<String, Double>> profitPerMonth = reduced.map(new MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> input) throws Exception {
                return new Tuple2<String, Double>(input.f0, new Double((input.f3 * 1.0) / input.f4));
            }
        });

        profitPerMonth.print();

        env.execute("Avg Profit Per Month");
    }
}
