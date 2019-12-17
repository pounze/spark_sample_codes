package com.virtual.pairprogrammers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;

public class DataFrames
{
    public static void init()
    {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession spark = SparkSession.builder().appName("testingSQL").master("local[*]")
                .config("spark.sql.warehouse.dir","D:\\spark_sample\\")
                .getOrCreate();


        Dataset<Row> dataset = spark.read().option("header", true).csv("D:\\spark_sample\\SparkSample\\pima-indians-diabetes.csv");

        dataset = dataset.select(col("Number_pregnant"), col("Glucose_concentration"), col("Blood_pressure"));

        dataset = dataset.groupBy(col("Number_pregnant"), col("Glucose_concentration"), col("Blood_pressure")).pivot("Blood_pressure").count();

        dataset.orderBy("Number_pregnant");

        //dataset = dataset.groupBy("Number_pregnant").agg(max(col("Glucose_concentration").cast(DataTypes.IntegerType)));

//        dataset = dataset.groupBy("Number_pregnant").pivot("Glucose_concentration").agg(round(avg(col("Blood_pressure")), 2).alias("average")
//            round(stddev(col("Glucose_concentration")), 2).alias("stddev")
//        );

        dataset.show(100);

        spark.close();
    }
}
