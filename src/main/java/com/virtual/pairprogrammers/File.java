package com.virtual.pairprogrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class File
{
    public static void init()
    {
        // System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        initialRdd
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .foreach(value -> System.out.println(value));
    }
}
