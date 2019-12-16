package com.virtual.pairprogrammers;

import com.sun.rowset.internal.Row;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkSQL
{
    public static void init()
    {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession spark = SparkSession.builder().appName("testingSQL").master("local[*]")
                .config("spark.sql.warehouse.dir","D:\\spark_sample\\")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header",true).csv("D:\\spark_sample\\pima-indians-diabetes.csv");

        dataset.show();

        long numberRows = dataset.count();

        System.out.println("total rows: "+numberRows);

        Row firstRow = dataset.first();

        String Blood_pressure = (String) firstRow.get(2);

        System.out.println(Blood_pressure);

        String Number_pregnant = firstRow.getAs("Number_pregnant");

        System.out.println(Number_pregnant);

        Dataset<Row> modernArtResult = dataset.filter("Pedigree = 0.627 AND Blood_pressure >= 72");

        modernArtResult.show();

        dataset.createOrReplaceTempView("my_student_table");

        Dataset<Row> results = spark.sql("SELECT count(Number_pregnant) as total_count, first(Group), first(Pedigree), first(Blood_pressure), first(Number_pregnant), first(Age) as age FROM my_student_table WHERE Blood_pressure >= 72 GROUP BY Group ORDER BY age DESC");

        results.show();

        spark.close();
    }
}
