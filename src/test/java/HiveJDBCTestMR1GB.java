

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

@DisplayName("MR_1GB")
public class HiveJDBCTestMR1GB {
    private static final String host = "jdbc:hive2://192.168.21.5:10000/;hive.execution.engine=mr";
//    private static final String host = "jdbc:hive2://192.168.21.5:10000";
    private static final String table = "adid_test2";

    //@Test
    @Order(2)
    @DisplayName("페치 10개 테스트")
    public void fetch_10() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(10, table, host);
        hive.run();
    }

    //@Test
    @Order(3)
    @DisplayName("페치 100개 테스트")
    public void fetch_100() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(100, table, host);
        hive.run();
    }

    @Test
    @Order(4)
    @DisplayName("페치 1000개 테스트")
    public void fetch_1000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(1000, table, host);
        hive.run();
    }

    @Test
    @Order(5)
    @DisplayName("페치 10000개 테스트")
    public void fetch_10000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(10000, table, host);
        hive.run();
    }

    @Test
    @Order(6)
    @DisplayName("페치 20000개 테스트")
    public void fetch_20000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(20000, table, host);
        hive.run();
    }

    @Test
    @Order(7)
    @DisplayName("페치 30000개 테스트")
    public void fetch_30000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(30000, table, host);
        hive.run();
    }

    @Test
    @Order(8)
    @DisplayName("페치 100000개 테스트")
    public void fetch_100000() throws InterruptedException {
        HiveJDBC hive = new HiveJDBC(100000, table, host);
        hive.run();
    }

}
