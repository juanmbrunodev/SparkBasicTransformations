package com.jmb;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BasicTransformations {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicTransformations.class);
    private static final String SPARK_FILES_FORMAT = "csv";
    public static final String PATH_RESOURCES = "src/main/resources/spark-data/generic-food.csv";

    public static void main(String[] args) throws Exception {

        LOGGER.info("Application starting up");
        BasicTransformations app = new BasicTransformations();
        app.init();
        LOGGER.info("Application gracefully exiting...");
    }

    private void init() throws Exception {
        //Create the Spark Session
        SparkSession session = SparkSession.builder()
                .appName("BasicTransformations")
                .master("local").getOrCreate();

        //Ingest data from CSV file into a DataFrame
        Dataset<Row> df = session.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        //Show first 5 records of the Raw ingested dataset
        //df.show(5);

        //Also print its schema
        //df.printSchema();

    }
}
