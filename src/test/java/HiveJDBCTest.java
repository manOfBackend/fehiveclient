import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Text_1.2GB")
class HiveJDBCTest {

    //    private static final String host = "jdbc:hive2://192.168.21.5:10000/;hive.execution.engine=tez;tez.queue.name=alt;hive.exec.parallel=true;hive.vectorized.execution.enabled=true;hive.vectorized.execution.reduce.enabled=true;";
    private static final String host = "jdbc:hive2://192.168.0.12:10000/;";
    private static final String table = "adid_test";

    @Test
    @Order(1)
    @DisplayName("페치 1000개 테스트")
    public void fetch_1000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(1000, table, host);
        hive.run();
    }

    @Test
    @Order(2)
    @DisplayName("페치 5000개 테스트")
    public void fetch_5000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(5000, table, host);
        hive.run();
    }

    @Test
    @Order(3)
    @DisplayName("페치 10000개 테스트")
    public void fetch_10000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(10000, table, host);
        hive.run();
    }

    @Test
    @Order(4)
    @DisplayName("페치 15000개 테스트")
    public void fetch_15000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(15000, table, host);
        hive.run();
    }

    @Test
    @Order(5)
    @DisplayName("페치 20000개 테스트")
    public void fetch_20000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(20000, table, host);
        hive.run();
    }
}