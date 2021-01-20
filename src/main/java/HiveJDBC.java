import com.opencsv.CSVWriter;
import org.apache.hive.jdbc.HiveDriver;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;


public class HiveJDBC {
    private static String driverClass = "org.apache.hive.jdbc.HiveDriver";

    private int fetchSize = 10;

    private String tableName;

    private String hostName;

    public HiveJDBC(int fetchSize, String tableName, String hostName) {
        this.fetchSize = fetchSize;
        this.tableName = tableName;
        this.hostName = hostName;
    }

    public void run() throws InterruptedException {

        final Thread readerThread = new Thread("Reader") {
            public void run() {
                try {
                    try {
                        Class.forName(driverClass);
                    } catch (ClassNotFoundException exception) {

                        exception.printStackTrace();
                        System.exit(1);
                    }

                    HiveDriver hiveDriver = new HiveDriver();
                    Properties properties = new Properties();
                    properties.setProperty("user", "");
                    properties.setProperty("password", "");

                    Connection connection = hiveDriver.connect(hostName, properties);
                    // Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.21.5:10000/;hive.execution.engine=tez;tez.queue.name=alt;hive.exec.parallel=true;hive.vectorized.execution.enabled=true;hive.vectorized.execution.reduce.enabled=true;", "", "");
                    // Connection connection = DriverManager.getConnection(hostName, "", "");
                    // Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.80.227:10000", "", "");

                    Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    //Statement statement = connection.createStatement();

                    statement.setFetchSize(fetchSize);

                    String sql = "SELECT * FROM " + tableName;

                    System.out.println("Running: " + sql);
                    ResultSet result = statement.executeQuery(sql);

                    // result.setFetchSize(100);

                    CSVWriter csvWriter = new CSVWriter(new FileWriter("output.csv"));
                    csvWriter.writeAll(result, false, true);

                    int count = 0;

                    while (result.next()) {
                        count++;
                    }
                    System.out.println(count);
                    result.close();
                    statement.close();
                    connection.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                    return;
                }
            }
        };

        long startTime = System.currentTimeMillis();
        readerThread.start();
        readerThread.join();
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime / 1000.0);
    }
}
