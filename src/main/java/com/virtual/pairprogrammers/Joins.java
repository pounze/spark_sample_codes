package com.virtual.pairprogrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Joins {

    @SuppressWarnings("resource")
    public static void init()
    {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitRaw = new ArrayList<>();

        visitRaw.add(new Tuple2<>(4, 18));
        visitRaw.add(new Tuple2<>(6, 4));
        visitRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();

        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));
        usersRaw.add(new Tuple2<>(7, "Sudeep"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);

        joinedRdd.foreach(value -> System.out.println("Inner Join: "+value));

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftjoinRdd = visits.leftOuterJoin(users);

        leftjoinRdd.foreach(value -> System.out.println("Left outer join: "+value));

        leftjoinRdd.foreach(value -> System.out.println(value._2._2.orElse("blank").toUpperCase()));

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightjoinRdd = visits.rightOuterJoin(users);

        rightjoinRdd.foreach(value -> System.out.println("Right outer join: "+value));

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> unions = visits.cartesian(users);

        unions.foreach(value -> System.out.println("unions outer join: "+value));

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullouterjoin = visits.fullOuterJoin(users);

        fullouterjoin.foreach(value -> System.out.println("full outer join: "+value));

        sc.close();
    }
}
