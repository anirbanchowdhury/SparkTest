package com.codekata.tests;

import com.codekata.SparkSessionFactory;
import com.gs.collections.impl.list.mutable.FastList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.*;
import static org.apache.spark.sql.functions.count;

import static org.apache.spark.sql.functions.sum;


/*
*
* https://spark.apache.org/docs/latest/rdd-programming-guide.html
* */
public class TestSpark {

    private static SparkSession sparkSession;
    private static JavaSparkContext javaSparkContext;

    private final static Logger logger = LoggerFactory.getLogger(TestSpark.class);


    @BeforeClass
    public static void getSparkSession() {
        sparkSession = SparkSessionFactory.getSparkSession();
        javaSparkContext = SparkSessionFactory.getJavaSparkContext();
    }

    @AfterClass
    public static void killSession() {
        sparkSession.stop();
    }


    @Test
    public void readSparkFile() {
        /*
            Spark’s primary abstraction is a distributed collection of items called a Dataset.
            Datasets can be created from Hadoop InputFormats (such as HDFS files) or by transforming other Datasets
         */
        Dataset<String> logData = sparkSession.read().textFile("src/test/java/com/codekata/resources/test.csv");
        logData.show();
        long numAs = logData
                .filter((FilterFunction<String>) s -> s.contains("A"))
                .count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("U")).count();
        System.out.println("Lines with A: " + numAs + ", lines with U: " + numBs);
        Assert.assertEquals(2,numAs);
    }

    @Test
    public void testReduce() {
        JavaRDD<String> distFile = javaSparkContext.textFile("src/test/java/com/codekata/resources/test.csv");
        System.out.println(distFile
                .map(String::length)
                .reduce((a, b) -> a + b));
    }

    @Test
    public void testReadFile() {

        Dataset<Row> df = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", "}")
                .load("/Users/aniamritapc/IdeaProjects/Test/anicom/src/test/java/com/codekata/resources/test.csv");


        ArrayList<String> inputColsList = new ArrayList<>(asList(df.columns()));
        System.out.println(inputColsList);
        System.out.println(df.count());
        df.printSchema();
        df.show();
    }


    @Test
    public void testReadFileDataSet() {
        StructType schema = new StructType()
                .add("account", "string")
                .add("date", "string")
                .add("ccy", "string")
                .add("amount", "double");

        String file = "src/test/java/com/codekata/resources/sample.csv";
        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .schema(schema)
                .option("header", "true")
                .option("delimiter", "}")
                .load(file);

        dataset.printSchema();
        dataset.show();

        Dataset finalDS = dataset.groupBy("account", "ccy").
                agg(count("account"), sum("amount").alias("sum_amount"))
                .orderBy("account", "ccy");


        finalDS.show();

        Object[] items = (Object[])(finalDS.filter(functions.col("sum_amount") //this explicit cast is reqd.
                .equalTo(300.36))
                .collect());

        String accountNo =  (String)((GenericRowWithSchema)items[0]).get(0);
        Assert.assertEquals("A2",accountNo);

    }

    @Test
    public void testSparkSQL() {
        StructType schema = new StructType()
                .add("account", "string")
                .add("date", "string")
                .add("ccy", "string")
                .add("amount", "double");

        String file = "src/test/java/com/codekata/resources/sample.csv";
        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .schema(schema)
                .option("header", "true")
                .option("delimiter", "}")
                .load(file);

        dataset.printSchema();
        dataset.show();
        Assert.assertEquals(((Row [])dataset.head(4))[3].getDouble(3),400.12,0); // this explicit cast is also reqd.

        dataset.createOrReplaceTempView("balances");
        Dataset<Row> filteredDataset =  sparkSession.sql(
                "SELECT account,ccy, COUNT(*),sum(amount) as sum_amount"
                        + " FROM balances GROUP BY account,ccy"
                        + " having sum(amount) = 500.24 "
                        + " order by account,ccy  ");


        Assert.assertEquals("A1",filteredDataset.first().getString(0));



    }

        @Test
    public void testReadFileAll() {

        StructType schema = new StructType()
                .add("account", "string")
                .add("date", "string")
                .add("ccy", "string")
                .add("amount", "double");

        String file = "src/test/java/com/codekata/resources/sample.csv";
        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .schema(schema)
                .option("header", "true")
                .option("delimiter", "}")
                .load(file);

        dataset.printSchema();
        dataset.show();

        /*Instant start = Instant.now();
          Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toNanos();
        //  System.out.println("TimeElapsed = "+timeElapsed);
        */
        //  System.out.println("timeElap[sed = "+ Duration.between(finish,Instant.now()).toNanos());

        //Transformation
        JavaRDD<Balance> javaRDD = javaSparkContext.textFile(file)
                .map((Function<String, Balance>) line -> {
                    String[] fields = line.split("}");
                    return new Balance(fields[0], fields[1], fields[2].trim(), fields[3]);

                });

        JavaPairRDD<String, Integer> pairRDD = javaRDD
                .mapToPair((PairFunction<Balance, String, Integer>) balance -> new Tuple2<>(balance.account + balance.ccy, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);


        Assert.assertEquals(FastList.newListWith(new Tuple2("A1USD", 2)), pairRDD.take(1));

    }

    @Test
    public void testFunctionalTransformations(){
        List<List<Integer>> map = javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5))
                .map(i->Arrays.asList(i,1))
                .collect();
        System.out.println(map);

        List<Integer> flatmap = javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5))
                .flatMap(i->Arrays.asList(i,1).iterator())
                .collect();
        System.out.println(flatmap);

        JavaRDD<String> rddX = javaSparkContext.parallelize(
                Arrays.asList("spark rdd example", "sample example"),
                2);

        // map operation will return List of Array in following case
        JavaRDD<String[]> rddY = rddX.map(e -> e.split(" "));
        List<String[]> listUsingMap = rddY.collect();

        listUsingMap.forEach(System.out::println);

        // flatMap operation will return list of String in following case
        JavaRDD<String> rddY2 = rddX.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        List<String> listUsingFlatMap = rddY2.collect();

        System.out.println(listUsingFlatMap);

    }
}

class Balance implements Serializable {
    String account, date, ccy, amount;

    public Balance(String account, String date, String ccy, String amount) {
        this.account = account;
        this.date = date;
        this.ccy = ccy;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Balance{" +
                "account='" + account + '\'' +
                ", date='" + date + '\'' +
                ", ccy='" + ccy + '\'' +
                ", amount='" + amount + '\'' +
                '}';
    }
}