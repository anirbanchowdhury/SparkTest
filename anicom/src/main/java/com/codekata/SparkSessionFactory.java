package com.codekata;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkSessionFactory {
    private final static Logger logger = LoggerFactory.getLogger(SparkSessionFactory.class);
    private static SparkSession sparkSession;
    private static JavaSparkContext javaSparkContext;


    private static SparkSessionFactory instance = new SparkSessionFactory();

    private  SparkSessionFactory (){
        sparkSession =
            SparkSession.builder()
                    //.config("spark.master", "local[8]") // Comment out this line if you are submitting to the cluster
                   // .config("spark.default.parallelism",3)
                    .getOrCreate();

         javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
         System.out.println(javaSparkContext.getConf().getAll());
    }


    public static SparkSession getSparkSession() {
        logger.info("Getting SparkSession");
        return sparkSession;
    }

    public static JavaSparkContext getJavaSparkContext() {
        logger.info("Getting SparkSession");
        return javaSparkContext;
    }

    public void stopSparkSession(SparkSession sparkSessions) {
        sparkSessions.stop();
    }
}
