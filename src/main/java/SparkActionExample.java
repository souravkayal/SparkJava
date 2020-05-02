import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SparkActionExample {

    List<Integer> lists = new ArrayList<Integer>();

    public SparkActionExample(){
        lists.add(1);
        lists.add(2);
        lists.add(3);
        lists.add(4);
        lists.add(5);
    }

    public void simpleArrayAction(){
        JavaRDD<Integer> javaRDD = SparkContextFactory.getcontext().parallelize(lists);

        //Print the RDD
        System.out.println(javaRDD.collect());

        //Take top n from rdd
        System.out.println(javaRDD.take(2));

        //count number of items in rdd
        System.out.println(javaRDD.count());

        //Return first element of RDD
        System.out.println(javaRDD.first());

        //Take sample from RDD
        System.out.println(javaRDD.takeSample(true, 3, 1));


        //Count by Value. Returns count of item in key value pair
        //Example , 1->1 . Number 1 appears in 1 time in RDD
        System.out.println(javaRDD.countByValue());

        //Reduce in Java RDD
        int sum = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println(sum);

    }
}
