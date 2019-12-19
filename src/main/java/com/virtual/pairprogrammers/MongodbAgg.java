package com.virtual.pairprogrammers;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import static java.util.Collections.singletonList;

public class MongodbAgg
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

        // Load and analyze data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        /*Start Example: Use aggregation to filter a RDD***************/
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { average : { $gt : 0 } } }")));
        /*End Example**************************************************/

        // Analyze data from MongoDB
        System.out.println(aggregatedRdd.count());
        System.out.println(aggregatedRdd.first().toJson());

        jsc.close();
    }
}
