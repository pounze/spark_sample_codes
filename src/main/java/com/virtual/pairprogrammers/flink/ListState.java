package com.virtual.pairprogrammers.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.security.auth.login.Configuration;

public class ListState
{
    public static void main(String ...args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Long> sum = data.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
            }
        })
                .keyBy(0)
                .flatMap(new FlinkState.StatefulMap());

        sum.print("State2");
        env.execute("State1");
    }

    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Tuple2<String, Long>>
    {
        private transient ValueState<Long> count;
        private transient org.apache.flink.api.common.state.ListState<Long> numbers;

        @Override
        public void flatMap(Tuple2<Long, String> input, Collector<Tuple2<String, Long>> out) throws Exception {
            Long currValue = Long.parseLong(input.f1);
            Long currCount = count.value();

            currCount += 1;
            currCount = currCount + Long.parseLong(input.f1);

            count.update(currCount);
            numbers.add(currValue);

            if(currCount >= 10)
            {
                Long sum = 0L;
                String numberStr = "";
                for(Long number: numbers.get())
                {
                    numberStr = numberStr + " "+ number;
                    sum = sum + number;
                }

                out.collect(new Tuple2<String, Long>(numberStr, sum));

                count.clear();
                numbers.clear();
            }
        }

        public void open(Configuration conf)
        {
            ListStateDescriptor<Long> listDesc = new ListStateDescriptor<Long>("numbers",  Long.class);

            numbers = getRuntimeContext().getListState(listDesc);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>("count", Long.class);

            count = getRuntimeContext().getState(descriptor2);
        }
    }
}
