package com.virtual.pairprogrammers;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MapFilter
{
    @SuppressWarnings("resource")
    public static void init()
    {
        List<String> inputData = new ArrayList<>();

        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 4 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalLogMessages = sc.parallelize(inputData).cache();

        JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair(rawValue -> {
            String[] columns =  rawValue.split(":");
            String level = columns[0];

            return new Tuple2<String, Long>(level, 1L);
        });

        JavaPairRDD<String, Long> sumRdd = pairRdd.reduceByKey((value1, value2) -> value1 + value2);


        sumRdd.foreach(tuple -> System.out.println(tuple._1+" has "+tuple._2+" instances"));

        // groupbykey version

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .groupByKey()
                .foreach(tuple -> System.out.println(tuple._1+" has "+ Iterables.size(tuple._2)+" instances"));

        JavaRDD<String> sentences = sc.parallelize(inputData).persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator()).filter(value -> value.length() > 4);

        words.foreach(value -> System.out.println(value));

        sc.close();
    }
}
