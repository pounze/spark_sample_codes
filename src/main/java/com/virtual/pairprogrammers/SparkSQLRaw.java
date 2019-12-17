package com.virtual.pairprogrammers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkSQLRaw
{
    public static void init()
    {
        /*
         * Spark SQL uses 2 algorithms for grouping
         * 1) Sort Aggregate
         * 2) Hash Aggregate
         * */

        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession spark = SparkSession.builder().appName("testingSQL").master("local[*]")
                .config("spark.sql.warehouse.dir","D:\\spark_sample\\")
                .getOrCreate();

        spark.conf().set("spark.sql.shuffle.partitions", "12");


        List<Row> inMemory = new ArrayList<Row>();

        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2016-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL", "2016-4-21 19:23:20"));

        StructField[] fields = new StructField[]{
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);

        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql("SELECT level, date_format(datetime, 'y') as year from logging_table");

        results.show();

        results.explain();

        System.out.println(results.toJSON());

        spark.close();
    }
}
