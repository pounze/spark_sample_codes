package com.virtual.pairprogrammers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DStreamVersionsKafka
{
    public static void main(String[] args) throws InterruptedException
    {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        SparkConf conf = new SparkConf().setAppName("viewingFigures").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(2));

        Collection topics = Arrays.asList("viewrecords","sql-insert");

        Map<String, Object> kafkaParams = new HashMap<>();

        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-group");
        kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("enable.auto.commit", false);

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
        KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        //JavaDStream<String> results = stream.map(item -> item.value());

        JavaPairDStream<Long, String> results = stream.mapToPair(item -> new Tuple2<>(item.value(), 5L))
                .reduceByKeyAndWindow((x,y) -> x+y, Durations.minutes(2), Durations.minutes(1))
                .mapToPair(item -> item.swap())
                .transformToPair(rdd -> rdd.sortByKey(false));

        results.print(50);

        ssc.start();
        ssc.awaitTermination();
    }
}
