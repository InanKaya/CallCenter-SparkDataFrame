import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.1-bin");

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Call-Center-Spark-SQL")
                .getOrCreate();


        Dataset<Row> callCenterRaw = sparkSession.read().option("header",true).csv("C:\\Users\\inan\\Desktop\\callcenter.csv");


        Dataset<Row> selectData = callCenterRaw.select(new Column("Creation_Date"),
                new Column("Status"),
                new Column("Completion_Date"),
                new Column("Service_Request_Number"),
                new Column("Type_of_Service_Request"),
                new Column("Street_Address"));


        Dataset<Row> dataByRequestNumber = selectData.groupBy(new Column("Service_Request_Number")).count();

        Dataset<Row> dataByFiveRequest = dataByRequestNumber.filter(new Column("count").equalTo("5"));

        Dataset<Row> dataBySpecialNumber = selectData.filter(new Column("Service_Request_Number").equalTo("14-01783521"));

        dataBySpecialNumber.show();

    }
}
