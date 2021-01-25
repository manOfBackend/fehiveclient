import org.apache.hive.jdbc.HiveBaseResultSet;
import org.apache.hive.jdbc.HiveDriver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.IntStream;


public class HiveJDBC {

    // 쿼리당 가져오는 Row 수
    private int fetchSize = 10;

    private String tableName;

    // DB 접속 URL
    private String hostName;

    public HiveJDBC(int fetchSize, String tableName, String hostName) {
        this.fetchSize = fetchSize;
        this.tableName = tableName;
        this.hostName = hostName;
    }

    private Connection createConnection(String hostName) throws SQLException {
        final HiveDriver hiveDriver = new HiveDriver();
        final Properties properties = new Properties();
        properties.setProperty("user", "");
        properties.setProperty("password", "");

        final Connection connection = hiveDriver.connect(hostName, properties);

        return connection;
    }

    /**
     * ResultSet이 가져온 쿼리 결과를 CSV에 저장하는 함수(기본 저장 경로: output.csv)
     *
     * @param rs
     * @throws SQLException
     */
    private void writeToCsv(HiveBaseResultSet rs) throws SQLException {

        while (rs.next()) {
            final int n = rs.getMetaData().getColumnCount();
            final String[] line = IntStream.range(1, n).mapToObj(i -> {
                try {
                    return rs.getString(i);
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                    return new String[]{};
                }
            }).toArray(size -> new String[size]);

            QueueManager.addLine(line);
        }

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

                    final String sql = "SELECT * FROM " + tableName;

                    System.out.println("Running: " + sql);

                    final HiveBaseResultSet result = (HiveBaseResultSet) statement.executeQuery(sql);

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

        Thread writerThread = new Thread(Writer.getInstance());
        writerThread.start();

        readerThread.join();

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime / 1000.0);
    }
}
