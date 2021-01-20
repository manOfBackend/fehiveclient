import com.opencsv.CSVWriter;
import org.apache.hive.jdbc.HiveDriver;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;


public class HiveJDBC {

    private int fetchSize = 10;

    private String tableName;

    private String hostName;

    public HiveJDBC(int fetchSize, String tableName, String hostName) {
        this.fetchSize = fetchSize;
        this.tableName = tableName;
        this.hostName = hostName;
    }

    private Connection createConnection(String hostName) throws SQLException {
        HiveDriver hiveDriver = new HiveDriver();
        Properties properties = new Properties();
        properties.setProperty("user", "");
        properties.setProperty("password", "");

        Connection connection = hiveDriver.connect(hostName, properties);
        // Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.21.5:10000/;hive.execution.engine=tez;tez.queue.name=alt;hive.exec.parallel=true;hive.vectorized.execution.enabled=true;hive.vectorized.execution.reduce.enabled=true;", "", "");
        // Connection connection = DriverManager.getConnection(hostName, "", "");
        // Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.80.227:10000", "", "");

        return connection;
    }

    private void writeToCsv(ResultSet rs) throws IOException, SQLException {
        CSVWriter csvWriter = new CSVWriter(new FileWriter("output.csv"));
        csvWriter.writeAll(rs, false, true);
    }

    private void logic(ResultSet rs) throws SQLException {

        int count = 0;

        while (rs.next()) {
            count++;
        }
        System.out.println(count);
    }

    public void run() throws InterruptedException {

        final Thread readerThread = new Thread("Reader") {
            public void run() {
                try {

                    final Connection connection = createConnection(hostName);

                    final Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    statement.setFetchSize(fetchSize);

                    String sql = "SELECT * FROM " + tableName;

                    System.out.println("Running: " + sql);

                    ResultSet result = statement.executeQuery(sql);
                    // result.setFetchSize(100);

                    writeToCsv(result);

                    // logic(result);

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
