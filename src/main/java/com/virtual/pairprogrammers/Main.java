package com.virtual.pairprogrammers;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.*;

public class Main
{
    @SuppressWarnings("resource")
    public static void main(String[] args) throws InterruptedException
    {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(3000));

        JavaReceiverInputDStream<String> inputData = ssc.socketTextStream("localhost", 8989);

        JavaDStream<Object> results = inputData.map(item -> item);

        results.print();

        ssc.start();

        ssc.awaitTermination();


    }
}
