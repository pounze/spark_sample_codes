package com.virtual.pairprogrammers.flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class JoinExample
{
    public static void main(String ... args) throws Exception
    {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile("JoinFile").map(value -> {
            String[] words = value.split(",");
            return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
        }).returns(Types.TUPLE(Types.INT, Types.STRING));

        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile("JoinSecondFile").map(value -> {
            String[] words = value.split(",");
            return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
        }).returns(Types.TUPLE(Types.INT, Types.STRING));

        DataSet<Tuple3<Integer, String, String>> joinSet = personSet.join(locationSet, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) throws Exception {
                return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);
            }
        });

        personSet.print();

        locationSet.print();

        System.out.println("##################################################");

        joinSet.print();

        System.out.println("##################################################");

        DataSet<Tuple3<Integer, String, String>> leftjoinSet = personSet.leftOuterJoin(locationSet).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) throws Exception {
                if(location == null)
                {
                    return new Tuple3<Integer, String, String>(person.f0, person.f1, "NULL");
                }

                return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);
            }
        });

        leftjoinSet.print();

        System.out.println("##################################################");

        DataSet<Tuple3<Integer, String, String>> rightjoinSet = personSet.rightOuterJoin(locationSet).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) throws Exception {
                if(person == null)
                {
                    return new Tuple3<Integer, String, String>(location.f0, "NULL" , location.f1);
                }

                return new Tuple3<Integer, String, String>(location.f0, person.f1, location.f1);
            }
        });

        rightjoinSet.print();

        DataSet<Tuple3<Integer, String, String>> fullOuterjoinSet = personSet.fullOuterJoin(locationSet).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) throws Exception {
                if(location == null)
                {
                    return new Tuple3<Integer, String, String>(person.f0, person.f1, "NULL");
                }
                else if(person == null)
                {
                    return new Tuple3<Integer, String, String>(location.f0, "NULL" , location.f1);
                }

                return new Tuple3<Integer, String, String>(location.f0, person.f1, location.f1);
            }
        });

        fullOuterjoinSet.print();
    }
}
