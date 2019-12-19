package com.virtual.pairprogrammers;

import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;

public class SparkMongoSql
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cryptovpay.historical_pricefeed")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/cryptovpay.historical_pricefeed_spark")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Load data and infer schema, disregard toDF() name as it returns Dataset
        Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
        implicitDS.printSchema();
        implicitDS.show();

        // Load data with explicit schema
        Dataset<Character> explicitDS = MongoSpark.load(jsc).toDS(Character.class);
        explicitDS.printSchema();
        explicitDS.show();

        // Create the temp view and execute the query
        explicitDS.createOrReplaceTempView("price_feed");
        Dataset<Row> centenarians = spark.sql("SELECT * FROM price_feed WHERE average >= 100");
        centenarians.show();

        /* Note: "overwrite" drops the collection before writing,
         * use "append" to add to the collection */
        MongoSpark.write(centenarians).option("collection", "spark_price")
                .mode("overwrite").save();


        // Load the data from the "hundredClub" collection
       // MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("collection", "hundredClub"), Character.class).show();

        jsc.close();
    }
}
