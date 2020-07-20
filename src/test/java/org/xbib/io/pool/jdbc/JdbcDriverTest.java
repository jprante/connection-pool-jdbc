package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.xbib.io.pool.jdbc.util.DriverDataSource;
import java.sql.Connection;
import java.util.Properties;

public class JdbcDriverTest {

    private PoolDataSource ds;

    @AfterEach
    public void teardown() {
        if (ds != null) {
            ds.close();
        }
    }

    @Test
    public void driverTest1() throws Exception {
        Properties properties = new Properties();
        properties.put("url", "jdbc:stub");
        properties.put("user", "bart");
        properties.put("password", "simpson");
        PoolConfig config = new PoolConfig(properties);
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDriverClassName("org.xbib.io.pool.jdbc.mock.StubDriver");
        ds = new PoolDataSource(config);
        assertTrue(ds.isWrapperFor(DriverDataSource.class));
        DriverDataSource unwrap = ds.unwrap(DriverDataSource.class);
        assertNotNull(unwrap);
        try (Connection connection = ds.getConnection()) {
            // test that getConnection() succeeds
        }
    }

    @Test
    public void driverTest2() throws Exception {
        Properties properties = new Properties();
        properties.put("url", "jdbc:invalid");
        PoolConfig config = new PoolConfig(properties);
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDriverClassName("org.xbib.io.pool.jdbc.mock.StubDriver");
        try {
            ds = new PoolDataSource(config);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("claims to not accept"));
        }
    }
}
