import org.apache.spark.sql.Dataset;

public class SparkDatasetFactory {

    public static Dataset getDataSetFromMySQL(String tableName){

       return SparkContextFactory.getSqlContext()
                .read()
                .format("jdbc")
                .option("url" , "jdbc:mysql://127.0.0.1:3306/jpawork?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
                .option("user", "root")
                .option("password", "root")
                .option("dbtable", tableName ) //You can pass query as well
                .load();

    }

}
