import org.apache.spark.sql.Dataset;

//Read data from mysql using jdbc connector
public class SparkJDBCMySQL {

        public void readMySQLData() throws ClassNotFoundException {
            Dataset workTable = SparkDatasetFactory.getDataSetFromMySQL("work");
            workTable.show();

            //print schema
            workTable.printSchema();

            //Count number of rows in table
            workTable.count();

        }
}
