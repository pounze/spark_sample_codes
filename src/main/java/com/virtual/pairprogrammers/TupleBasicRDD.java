package com.virtual.pairprogrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TupleBasicRDD
{
    public static void init()
    {
        List<Integer> inputData = new ArrayList<>();

        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);

        JavaRDD<Double> sqrtRdd =  myRdd.map((value) -> Math.sqrt(value));

        sqrtRdd.foreach((value) -> System.out.println(value));

        System.out.println(result);

        // how many elements in sqrtRdd

        System.out.println(sqrtRdd.count());

        // using just map and reduce

        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
        Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);

        System.out.println(count);

        // This is no longer RDD, its now normal Java List

        sqrtRdd.collect().forEach((value) -> System.out.println(value));

        JavaRDD<Tuple2<Integer, Double>> sqrtRdd1 =  myRdd.map((value) -> new Tuple2<Integer, Double>(value, Math.sqrt(value)));

        Tuple2<Integer, Double>myValue = new Tuple2<>(2, 3.0);

        sc.close();
    }
}
