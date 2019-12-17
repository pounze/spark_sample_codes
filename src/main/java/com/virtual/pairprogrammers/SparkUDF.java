package com.virtual.pairprogrammers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class SparkUDF
{
    public static void init()
    {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession spark = SparkSession.builder().appName("testingSQL").master("local[*]")
                .config("spark.sql.warehouse.dir","D:\\spark_sample\\")
                .getOrCreate();

        spark.conf().set("spark.sql.shuffle.partitions", "12");


        spark.udf().register("hasPassed", (String grade, String subject) -> {

            if(subject.equals("Biology"))
            {
                return false;
            }

            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);

        Dataset<Row> dataset = spark.read().option("header", true).csv("D:\\spark_sample\\SparkSample\\pima-indians-diabetes.csv");

        dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));

        dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));

        dataset.explain();

        spark.close();
    }
}
