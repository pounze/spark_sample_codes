package com.virtual.pairprogrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkAccessHost {
    public static void init()
    {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Scanner scanner = new Scanner(System.in);

        scanner.nextLine();

        sc.close();
    }
}
