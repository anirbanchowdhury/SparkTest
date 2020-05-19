package com.codekata;

import com.gs.collections.impl.list.mutable.FastList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.Objects;


public class SimpleApp {

    private final static Logger logger = LoggerFactory.getLogger(SimpleApp.class);

    public static void main(String[] args) {

        //testSparkRead();
        testReadFileAll();
    }

    public static void testReadFileAll() {
        SparkSession sparkSession = SparkSessionFactory.getSparkSession();

        JavaSparkContext javaSparkContext = SparkSessionFactory.getJavaSparkContext();
        String file = "/Users/aniamritapc/IdeaProjects/SparkTest/anicom/src/test/java/com/codekata/resources/sample.csv";
/*        StructType schema = new StructType()
                .add("account", "string")
                .add("date", "string")
                .add("ccy", "string")
                .add("amount", "double");


        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .schema(schema)
                .option("header", "true")
                .option("delimiter", "}")
                .load(file);

        dataset.printSchema();
        dataset.show();


*/

        //Transformation


        JavaRDD<String> javaRDDWithHeader = javaSparkContext.textFile(file);

        JavaRDD<Balance> javaRDDWithoutHeader = javaRDDWithHeader
                .filter((Function<String, Boolean>) v1 -> !v1.equals(javaRDDWithHeader.first()))
                .map((Function<String, Balance>) line -> {
                    String[] fields = line.split("}");
                    return new Balance(fields[0], fields[1], fields[2].trim(), fields[3]);
                });

        JavaPairRDD<DummyString, BigDecimal> pairRDD = javaRDDWithoutHeader
                //  .mapToPair((PairFunction<Balance, String, Integer>) balance -> new Tuple2<>(balance.getAccount() + balance.getCcy(), 1))
                .mapToPair(new MyPairFunction())
                //.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2)
                .reduceByKey(new ReductionFunction())
                //.partitionBy( new HashPartitioner(4))
                ;

        System.out.println("Number of partitions = " +pairRDD.getNumPartitions());
        System.out.println("Partitions before = "+pairRDD.glom().collect());
        JavaPairRDD<DummyString, BigDecimal> newPairRDD = pairRDD.repartition(3);
        System.out.println("Number of partitions = " +newPairRDD.getNumPartitions());
        System.out.println("Partitions after  = "+newPairRDD.glom().collect());
    }

static    class MyPairFunction implements PairFunction<Balance, DummyString, BigDecimal> {
        @Override
        public Tuple2<DummyString, BigDecimal> call(Balance balance) {
            return new Tuple2<>(new DummyString(balance.getAccount(), balance.getCcy()), balance.getAmount());
        }
    }

  static  class DummyString implements Serializable {
        String account;
        String ccy;

        public DummyString(String account, String ccy) {
            this.account = account;
            this.ccy = ccy;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DummyString that = (DummyString) o;
            return Objects.equals(account, that.account) &&
                    Objects.equals(ccy, that.ccy);
        }

        @Override
        public int hashCode() {
            //System.out.println("DummyString="+this);
            // System.out.println("Hash="+Objects.hash(account,ccy));
            return Objects.hash(account, ccy);
        }


        @Override
        public String toString() {
            return "DummyString{" +
                    "\naccount='" + account + '\'' +
                    ", ccy='" + ccy + '\'' +

                    '}';
        }
    }

   static class ReductionFunction implements Function2<BigDecimal, BigDecimal, BigDecimal> {

        @Override
        public BigDecimal call(BigDecimal v1, BigDecimal v2) {
            return v1.add(v2);
        }
    }


    private static void testSparkRead() {
        SparkSession spark = SparkSessionFactory.getSparkSession();
        //without the absolute path submitting to spark master didnt work as the context was set to $SPARK_HOME/sbin
        String logFile = "/Users/aniamritapc/IdeaProjects/SparkTest/anicom/src/test/java/com/codekata/resources/sample.csv";
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

        logger.info("Lines with a: " + numAs + ", lines with b: " + numBs);
        spark.stop();
    }

}
