package com.codekata;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleApp {

    private final static Logger logger = LoggerFactory.getLogger(SimpleApp.class);

    public static void main(String[] args) {
        testSparkRead();
    }

    private static void testSparkRead() {
        SparkSession spark = SparkSessionFactory.getSparkSession();
        String logFile = "./anicom/src/main/java/com/anicom/SimpleApp.java";
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

        logger.info("Lines with a: " + numAs + ", lines with b: " + numBs);
        spark.stop();
    }

}
