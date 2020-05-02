import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class SparkDataFrame {

    public void sparkDataRead() throws AnalysisException {

        Dataset<Row> peopleData = SparkContextFactory.getSession().read().json("G:\\Learning\\SparkJava\\src\\main\\data\\people.json");

        //Show data in dataSet
        peopleData.show();

        //Show schema of DataSet
        peopleData.printSchema();

        //take only one column and print
        peopleData.select("id").show();

        //filter on DataSet
        peopleData.filter(col("id").gt(1)).show();

        //Create temporary table/view using DataFrame. This is local temp view and visible on spark context level
        peopleData.createOrReplaceTempView("PEOPLE");

        //SQL query to read from temp view
        Dataset<Row> singleRecord = SparkContextFactory.getSession().sql("select * from PEOPLE limit 1");
        singleRecord.show();

    }
}
