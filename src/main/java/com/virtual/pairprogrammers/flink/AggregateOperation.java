package com.virtual.pairprogrammers.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregateOperation
{
    public static void main(String ... args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("DataSetAgg");

        DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(new MapFunction<String, Tuple4<String, String, String, Integer>>() {
            @Override
            public Tuple4<String, String, String, Integer> map(String value) throws Exception {
                String[] words = value.split(",");
                return new Tuple4<String, String, String, Integer>(words[1], words[2], words[3], Integer.parseInt(words[4]));
            }
        });

        mapped.keyBy(0).sum(3).writeAsText("out1");
        mapped.keyBy(0).min(3).writeAsText("out2");
        mapped.keyBy(0).minBy(3).writeAsText("out3");
        mapped.keyBy(0).max(3).writeAsText("out4");
        mapped.keyBy(0).maxBy(3).writeAsText("out5");

        env.execute("Aggregation");
    }
}
