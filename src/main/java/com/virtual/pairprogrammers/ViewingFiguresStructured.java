package com.virtual.pairprogrammers;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import scala.concurrent.duration.Duration;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ViewingFiguresStructured
{
    public static void main(String[] args) throws StreamingQueryException
    {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ALL);
        SparkConf conf = new SparkConf().setAppName("viewingFigures").setMaster("local[*]");

        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("structuredViewingReport")
                .getOrCreate();

        session.conf().set("spark.sql.shuffle.partitions", "10");

        Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers","localhost:9092")
                .option("subscribe","viewrecords")
                //.option("kafka.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .load();

        // start some dataframe operations
        df.createOrReplaceTempView("viewing_figures");

        // key, value, timestamp
        Dataset<Row> results = session.sql("SELECT window, cast(value as string) as course_name,sum(1) as  seconds_watched from viewing_figures group by window(timestamp, '2 minutes'), course_name order by seconds_watched DESC");

        StreamingQuery query = results
            .writeStream()
            .format("console")
            //.outputMode(OutputMode.Append())
//            .outputMode(OutputMode.Update())
            .outputMode(OutputMode.Complete())
            .option("truncate", false)
            .option("numRows", 50)
            //.trigger(Trigger.ProcessingTime(Duration))
            .start();

        query.awaitTermination();
    }
}
