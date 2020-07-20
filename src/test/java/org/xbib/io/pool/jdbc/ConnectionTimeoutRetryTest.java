package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;
import org.xbib.io.pool.jdbc.mock.StubConnection;
import org.xbib.io.pool.jdbc.mock.StubDataSource;
import org.xbib.io.pool.jdbc.util.ClockSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConnectionTimeoutRetryTest {

    @Test
    public void testConnectionRetries() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2800);
        config.setValidationTimeout(2800);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
            stubDataSource.setThrowException(new SQLException("Connection refused"));
            long start = ClockSource.currentTime();
            try (Connection connection = ds.getConnection()) {
                connection.close();
                fail("Should not have been able to get a connection.");
            } catch (SQLException e) {
                long elapsed = ClockSource.elapsedMillis(start);
                long timeout = config.getConnectionTimeout();
                assertTrue(elapsed >= timeout, "Didn't wait long enough for timeout");
            }
        }
    }

    @Test
    public void testConnectionRetries2() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2800);
        config.setValidationTimeout(2800);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            final StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
            stubDataSource.setThrowException(new SQLException("Connection refused"));
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.schedule(() -> stubDataSource.setThrowException(null), 300, TimeUnit.MILLISECONDS);
            long start = ClockSource.currentTime();
            try {
                try (Connection connection = ds.getConnection()) {
                    // close immediately
                }
                long elapsed = ClockSource.elapsedMillis(start);
                assertTrue(elapsed < config.getConnectionTimeout(), "waited too long to get a connection");
            } catch (SQLException e) {
                fail("Should not have timed out: " + e.getMessage());
            } finally {
                scheduler.shutdownNow();
            }
        }
    }

    @Test
    public void testConnectionRetries3() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(2800);
        config.setValidationTimeout(2800);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            final Connection connection1 = ds.getConnection();
            final Connection connection2 = ds.getConnection();
            assertNotNull(connection1);
            assertNotNull(connection2);
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
            scheduler.schedule(() -> {
                try {
                    connection1.close();
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }, 800, TimeUnit.MILLISECONDS);

            long start = ClockSource.currentTime();
            try {
                try (Connection connection3 = ds.getConnection()) {
                    // close immediately
                }
                long elapsed = ClockSource.elapsedMillis(start);
                assertTrue((elapsed >= 700) && (elapsed < 950), "waited too long to get a connection");
            } catch (SQLException e) {
                fail("Should not have timed out.");
            } finally {
                scheduler.shutdownNow();
            }
        }
    }

    @Test
    public void testConnectionRetries5() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(1000);
        config.setValidationTimeout(1000);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            final Connection connection1 = ds.getConnection();
            long start = ClockSource.currentTime();
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
            scheduler.schedule(() -> {
                try {
                    connection1.close();
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }, 250, TimeUnit.MILLISECONDS);
            StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
            stubDataSource.setThrowException(new SQLException("Connection refused"));
            try {
                try (Connection connection2 = ds.getConnection()) {
                    // close immediately
                }
                long elapsed = ClockSource.elapsedMillis(start);
                assertTrue((elapsed >= 250) && (elapsed < config.getConnectionTimeout()), "Waited too long to get a connection");
            } catch (SQLException e) {
                fail("Should not have timed out.");
            } finally {
                scheduler.shutdownNow();
            }
        }
    }

    @Test
    public void testConnectionIdleFill() throws Exception {
        StubConnection.slowCreate = false;
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(2000);
        config.setValidationTimeout(2000);
        config.setConnectionTestQuery("VALUES 2");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config.setHousekeepingPeriodMs(400);
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            try (Connection connection1 = ds.getConnection();
                    Connection connection2 = ds.getConnection();
                    Connection connection3 = ds.getConnection();
                    Connection connection4 = ds.getConnection();
                    Connection connection5 = ds.getConnection();
                    Connection connection6 = ds.getConnection();
                    Connection connection7 = ds.getConnection()) {
                Thread.sleep(1300);
                assertSame(10, pool.getTotalConnections(), "Total connections not as expected");
                assertSame(3, pool.getIdleConnections(), "Idle connections not as expected");
            }
            assertSame(10, pool.getTotalConnections(), "Total connections not as expected");
            assertSame(10, pool.getIdleConnections(), "Idle connections not as expected");
        }
    }
}
