package com.virtual.pairprogrammers;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import com.mongodb.spark.config.WriteConfig;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;


import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.Map;

public class SparkMongodb
{
    public static void main(String[] args)
    {
        /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cryptovpay.historical_pricefeed")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/cryptovpay.historical_pricefeed_spark")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        /*Start Example: Read data from MongoDB************************/
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        /*End Example**************************************************/

        // Analyze data from MongoDB
        System.out.println(rdd.count());
        System.out.println(rdd.first().toJson());

        // Create a custom WriteConfig with collection name
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "spark");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

        // parallelize the RDD and create object

        JavaRDD<Document> sparkDocuments = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                (new Function<Integer, Document>() {
                    public Document call(final Integer i) throws Exception {
                        return Document.parse("{spark: " + i + "}");
                    }
                });

        // save the document
        /*Start Example: Save data from RDD to MongoDB*****************/
        MongoSpark.save(sparkDocuments, writeConfig);

        jsc.close();
    }
}
