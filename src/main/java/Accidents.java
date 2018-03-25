import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

public class Accidents {

    public static void main(String[] args) {

        final SparkConf conf = new SparkConf().setAppName("Blue4IT Accidents Spark Example");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> accidentsFile = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/dataset/*.csv");

        JavaRDD<YearMonthCasualties> accidentsPerYearMonth = accidentsFile.map((Function<String, YearMonthCasualties>) line -> {
            final String[] csv = line.split(",");
            if ("Number_of_Casualties".equalsIgnoreCase(csv[8])) {
                return new YearMonthCasualties(YearMonth.now(), 0);
            }
            final int numberOfCasualties = Integer.parseInt(csv[8]);
            final YearMonth yearMonth = YearMonth.parse(csv[9], DateTimeFormatter.ofPattern("dd/MM/yyyy"));
            return new YearMonthCasualties(yearMonth, numberOfCasualties);
        });

        final JavaPairRDD<YearMonth, Integer> accidentsPerYearMonthPair = accidentsPerYearMonth.mapToPair(
                (PairFunction<YearMonthCasualties, YearMonth, Integer>) yearMonthCasualties ->
                        new Tuple2<>(yearMonthCasualties.getYearMonth(), yearMonthCasualties.getNumberOfCasualties()));


        final JavaPairRDD<YearMonth, Integer> yearMonthIntegerJavaPairRDD
                = accidentsPerYearMonthPair.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

        yearMonthIntegerJavaPairRDD.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/spark-output/");

        sc.close();

    }
}
