import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class SparkTransformExample {


    public void sparkTransformExample() {

        JavaRDD<Integer> sparkRDD = SparkContextFactory.getcontext().parallelize(Arrays.asList(1, 2, 3, 4, 5));

        //Map - To map each and every item to new item
        JavaRDD<Integer> spartMultiplyByTwo = sparkRDD.map(s -> s * 2);
        System.out.println(spartMultiplyByTwo.collect());

        //apply map and reduce. Map each element to make it double and then sum it up
        int sum = sparkRDD.map(f -> f * 2).reduce((a, b) -> a + b);
        System.out.println(sum);

        //apply filter on RDD
        JavaRDD<Integer> onlyEven = sparkRDD.filter(f -> f % 2 == 0);
        System.out.println(onlyEven.collect());

        //find only odd items
        JavaRDD<Integer> onlyOdd = sparkRDD.filter(f -> f % 2 != 0);

        //union of two RDD
        JavaRDD<Integer> resultRDD = onlyEven.union(onlyOdd);
        System.out.println(resultRDD.collect());

        //Example of Intersection
        JavaRDD<Integer> mixRDD = SparkContextFactory.getcontext().parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        JavaRDD<Integer> intersectionRDD = resultRDD.intersection(mixRDD);
        System.out.println(intersectionRDD.collect());

        //Example of re-partition. This repartition the existing RDD
        JavaRDD<Integer> myRDD = SparkContextFactory.getcontext().parallelize(Arrays.asList(1, 2, 3, 4, 5));
        System.out.println("Number of partition - " + myRDD.partitions());



    }
}
