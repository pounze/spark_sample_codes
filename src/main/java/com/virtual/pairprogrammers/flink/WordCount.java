package com.virtual.pairprogrammers.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class WordCount
{
    public static void main(String ... args) throws Exception
    {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        // read the text file from given input path
        DataSet<String> text = env.readTextFile("wordcount_input.txt");

        DataSet<String> filtered = text.filter(data -> data.startsWith("I"));

        // split up the lines in paris (2-tuples) containing (word,1)

        DataSet<Tuple2<String,Integer>> counts = filtered.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                    throws Exception {
                for(String word: value.toLowerCase().split(" ")){
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }).groupBy(0).sum(1);

//        DataSet<Tuple2<String,Integer>> tokenized = words.map(value -> Tuple2.of(value, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        // group by the tuple field "0" and sum up tuple field "1"

        counts.print();

        counts.writeAsCsv("wordCount", "\n"," ");

        // execute program

        env.execute("WordCount example");
    }
}
