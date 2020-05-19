package com.codekata.tests;

import com.codekata.Balance;
import com.codekata.SparkSessionFactory;
import com.gs.collections.impl.list.mutable.FastList;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.*;
import static org.apache.spark.sql.functions.count;

import static org.apache.spark.sql.functions.sum;


/*
 *
 * https://spark.apache.org/docs/latest/rdd-programming-guide.html
 *
 * */

@Ignore
public class TestSpark implements Serializable {

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
        Assert.assertEquals(2, numAs);
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
                .load("src/test/java/com/codekata/resources/test.csv");

        ArrayList<String> inputColsList = new ArrayList<>(asList(df.columns()));
        df.printSchema();
        df.show();
        Assert.assertEquals(2, df.count());
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

        Object[] items = (Object[]) (finalDS.filter(functions.col("sum_amount") //this explicit cast is reqd.
                .equalTo(300.36))
                .collect());

        String accountNo = (String) ((GenericRowWithSchema) items[0]).get(0);
        Assert.assertEquals("A2", accountNo);

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
        Assert.assertEquals(((Row[]) dataset.head(4))[3].getDouble(3), 400.12, 0); // this explicit cast is also reqd.

        dataset.createOrReplaceTempView("balances");
        Dataset<Row> filteredDataset = sparkSession.sql(
                "SELECT account,ccy, COUNT(*),sum(amount) as sum_amount"
                        + " FROM balances GROUP BY account,ccy"
                        + " having sum(amount) = 500.24 "
                        + " order by account,ccy  ");


        Assert.assertEquals("A1", filteredDataset.first().getString(0));


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


        //Transformation


        JavaRDD<String> javaRDDWithHeader = javaSparkContext.textFile(file);
        String headerRow = javaRDDWithHeader.first();

        JavaRDD<Balance> javaRDDWithoutHeader = javaRDDWithHeader
                .filter((Function<String, Boolean>) v1 -> !v1.equals(headerRow))
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
        System.out.println(pairRDD.glom().collect());
        JavaPairRDD<DummyString, BigDecimal> newPairRDD = pairRDD.repartition(3);
        System.out.println("Number of partitions = " +newPairRDD.getNumPartitions());
        System.out.println(newPairRDD.glom().collect());
    }

    class MyPairFunction implements PairFunction<Balance, DummyString, BigDecimal> {

        @Override
        public Tuple2<DummyString, BigDecimal> call(Balance balance) {
            return new Tuple2<>(new DummyString(balance.getAccount(), balance.getCcy()), balance.getAmount());
        }
    }

    class DummyString implements Serializable {
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

    class ReductionFunction implements Function2<BigDecimal, BigDecimal, BigDecimal> {

        @Override
        public BigDecimal call(BigDecimal v1, BigDecimal v2) {
            return v1.add(v2);
        }
    }


    @Test
    public void testFunctionalTransformations() {
        List<List<Integer>> map = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5))
                .map(i -> Arrays.asList(i, 1))
                .collect();
        System.out.println(map);

        List<Integer> flatmap = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5))
                .flatMap(i -> Arrays.asList(i, 1).iterator())
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

