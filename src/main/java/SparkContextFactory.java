
/*
    Simple spark context class to run spark job in single node local cluster.

 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkContextFactory {

    static SparkConf sparkConf = new SparkConf()
            .setMaster("local")
            .setAppName("SparkJava");

    static JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

    static SQLContext sqlContext = new SQLContext(javaSparkContext);


    private SparkContextFactory(){}

    public static JavaSparkContext getcontext(){
        return javaSparkContext;
    }

    public static SparkSession getSession(){
        return sqlContext.sparkSession();
    };

}
