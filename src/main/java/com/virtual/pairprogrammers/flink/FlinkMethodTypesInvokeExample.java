package com.virtual.pairprogrammers.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static sun.misc.Version.print;

public class FlinkMethodTypesInvokeExample
{
    public static void main(String ... args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use the explicit ".returns(...)"
        env.fromElements(1, 2, 3)
                .map(i -> Tuple2.of(i, i))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();

        // use a class instead

        env.fromElements(1, 2, 3)
                .map(new MyTuple2Mapper())
                .print();



        // use an anonymous class instead
//        env.fromElements(1, 2, 3)
//            .map(new MapFunction<Integer, Tuple2<Integer, Integer>>{
//            @Override
//            public Tuple2<Integer, Integer> map(Integer i) {
//                return Tuple2.of(i, i);
//            }
//        }).print();

        // or in this example use a tuple subclass instead
        env.fromElements(1, 2, 3)
                .map(i -> new DoubleTuple(i, i))
                .print();

        env.execute("WordCount example");
    }

    public static class MyTuple2Mapper implements MapFunction<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Integer i) {
            return Tuple2.of(i, i);
        }
    }

    public static class DoubleTuple extends Tuple2<Integer, Integer> {
        public DoubleTuple(int f0, int f1) {
            this.f0 = f0;
            this.f1 = f1;
        }
    }
}
