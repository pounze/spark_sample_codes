package com.virtual.pairprogrammers.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitOperation
{
    public static void main(String ...args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("DataSetAgg");

        SplitStream<Integer> evenOddStream = data.map(new MapFunction<String, Integer>(){
            public Integer map(String value)
            {
                return Integer.parseInt(value);
            }
        }).split(new OutputSelector<Integer>(){
            public Iterable<String> select(Integer value)
            {
                List<String> out = new ArrayList<String>();

                if(value % 2 == 0)
                {
                    out.add("even");
                }
                else
                {
                    out.add("odd");
                }

                return out;
            }
        });

        DataStream<Integer> evenData = evenOddStream.select("even");

        DataStream<Integer> oddData = evenOddStream.select("odd");

        evenData.writeAsText("evenData");
        oddData.writeAsText("oddData");

        env.execute("ODD EVEN");
    }
}
