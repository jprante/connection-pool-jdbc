package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

@ExtendWith(PoolTestExtension.class)
public class PoolTest {

    private static final Logger logger = Logger.getLogger(PoolTest.class.getName());

    @BeforeAll
    static void setup() throws Exception {
        Properties properties = new Properties();
        properties.put("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        PoolConfig config = new PoolConfig(properties);
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(2);
        config.setConnectionTestQuery("SELECT 1");
        config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
        try (PoolDataSource ds = new PoolDataSource(config);
             Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS basic_pool_test");
            stmt.executeUpdate("CREATE TABLE basic_pool_test ("
                    + "id INTEGER NOT NULL IDENTITY PRIMARY KEY, "
                    + "timestamp TIMESTAMP, "
                    + "string VARCHAR(128), "
                    + "string_from_number NUMERIC "
                    + ")");
        }
    }

    @Test
    public void testIdleTimeout() throws Exception {
        Properties properties = new Properties();
        properties.put("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        PoolConfig config = new PoolConfig(properties);
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(10);
        config.setConnectionTestQuery("SELECT 1");
        config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
        System.setProperty("pool.jdbc.housekeeping.periodMs", "1000");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            System.clearProperty("pool.jdbc.housekeeping.periodMs");
            TimeUnit.SECONDS.sleep(1);
            Pool pool = ds.getPool();
            config.setIdleTimeout(3000);
            assertEquals(5, pool.getTotalConnections(), "Total connections not as expected");
            assertEquals(5, pool.getIdleConnections(), "Idle connections not as expected");
            try (Connection connection = ds.getConnection()) {
                logger.log(Level.INFO, "got connection " + connection);
                assertNotNull(connection);
                TimeUnit.MILLISECONDS.sleep(1500);
                //assertEquals(6, pool.getTotalConnections(), "Second total connections not as expected");
                //assertEquals(5, pool.getIdleConnections(), "Second idle connections not as expected");
                assertEquals(5, pool.getTotalConnections(), "Second total connections not as expected");
                assertEquals(4, pool.getIdleConnections(), "Second idle connections not as expected");
            }
            //assertEquals(6, pool.getIdleConnections(), "Idle connections not as expected");
            assertEquals(5, pool.getIdleConnections(), "Idle connections not as expected");
            TimeUnit.SECONDS.sleep(2);
            assertEquals(5, pool.getTotalConnections(), "Third total connections not as expected");
            assertEquals(5, pool.getIdleConnections(), "Third idle connections not as expected");
        }
    }

    @Test
    public void testIdleTimeout2() throws Exception {
        Properties properties = new Properties();
        properties.put("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        PoolConfig config = new PoolConfig(properties);
        config.setMaximumPoolSize(50);
        config.setConnectionTestQuery("SELECT 1");
        config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
        System.setProperty("pool.jdbc.housekeeping.periodMs", "1000");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            System.clearProperty("pool.jdbc.housekeeping.periodMs");
            TimeUnit.SECONDS.sleep(1);
            Pool pool = ds.getPool();
            config.setIdleTimeout(3000);
            assertEquals(50, pool.getTotalConnections(), "Total connections not as expected");
            assertEquals(50, pool.getIdleConnections(), "Idle connections not as expected");
            try (Connection connection = ds.getConnection()) {
                assertNotNull(connection);
                TimeUnit.MILLISECONDS.sleep(1500);
                assertEquals(50, pool.getTotalConnections(), "Second total connections not as expected");
                assertEquals(49, pool.getIdleConnections(), "Second idle connections not as expected");
            }
            assertEquals(50, pool.getIdleConnections(), "Idle connections not as expected");
            TimeUnit.SECONDS.sleep(3);
            assertEquals(50, pool.getTotalConnections(), "Third total connections not as expected");
            assertEquals(50, pool.getIdleConnections(), "Third idle connections not as expected");
        }
    }
}
