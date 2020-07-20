package org.xbib.io.pool.jdbc;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.io.pool.jdbc.mock.StubConnection;
import org.xbib.io.pool.jdbc.mock.StubDataSource;
import org.xbib.io.pool.jdbc.mock.StubStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@ExtendWith(PoolTestExtension.class)
public class ConnectionTest {
    
    @Test
    public void testCreate() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setConnectionInitSql("SELECT 1");
        config.setReadOnly(true);
        config.setConnectionTimeout(2500);
        config.setLeakDetectionThreshold(TimeUnit.SECONDS.toMillis(30));
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            ds.setLoginTimeout(10);
            assertSame(10, ds.getLoginTimeout());
            Pool pool = ds.getPool();
            ds.getConnection().close();
            assertSame(1, pool.getTotalConnections(), "total connections not as expected");
            assertSame(1, pool.getIdleConnections(), "idle connections not as expected");
            try (Connection connection = ds.getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT * FROM device WHERE device_id=?")) {
                assertNotNull(connection);
                assertNotNull(statement);
                assertSame(1, pool.getTotalConnections(), "total connections not as expected");
                assertSame(0, pool.getIdleConnections(), "idle connections not as expected");
                statement.setInt(1, 0);
                try (ResultSet resultSet = statement.executeQuery()) {
                    assertNotNull(resultSet);
                    assertFalse(resultSet.next());
                }
            }
            assertSame(1, pool.getTotalConnections(), "Total connections not as expected");
            assertSame(1, pool.getIdleConnections(), "idle connections not as expected");
        }
    }

    @Test
    public void testMaxLifetime() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(1500);
        config.setConnectionTestQuery("VALUES 1");
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config.setHousekeepingPeriodMs(2000L);
        config.setMaxLifetime(30000L); // must be 30s or higher
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            assertSame(1, pool.getTotalConnections(), "Total connections not as expected");
            assertSame(1, pool.getIdleConnections(), "Idle connections not as expected");
            Connection unwrap;
            Connection unwrap2;
            try (Connection connection = ds.getConnection()) {
                unwrap = connection.unwrap(Connection.class);
                assertNotNull(connection);
                assertSame(1, pool.getTotalConnections(), "Second total connections not as expected");
                assertSame(0, pool.getIdleConnections(), "Second idle connections not as expected");
            }
            assertSame(1, pool.getIdleConnections(), "Idle connections not as expected");
            try (Connection connection = ds.getConnection()) {
                unwrap2 = connection.unwrap(Connection.class);
                assertSame(unwrap, unwrap2);
                assertSame(1, pool.getTotalConnections(), "Second total connections not as expected");
                assertSame(0, pool.getIdleConnections(), "Second idle connections not as expected");
                //pool.evictConnection(connection);
            }
            Thread.sleep(31000L);
            try (Connection connection = ds.getConnection()) {
                unwrap2 = connection.unwrap(Connection.class);
                assertNotSame(unwrap, unwrap2, "Expected a different connection");
            }
            assertSame(1, pool.getTotalConnections(), "Post total connections not as expected");
            assertSame(1, pool.getIdleConnections(), "Post idle connections not as expected");
        }
    }

    @Test
    public void testMaxLifetime2() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config.setHousekeepingPeriodMs(100L);
        try (PoolDataSource ds = new PoolDataSource(config)) {
            ds.getPool().getConfig().setMaxLifetime(700);
            Pool pool = ds.getPool();
            assertSame(0, pool.getTotalConnections(), "Total connections not as expected");
            assertSame(0, pool.getIdleConnections(), "Idle connections not as expected");
            Connection unwrap;
            Connection unwrap2;
            try (Connection connection = ds.getConnection()) {
                unwrap = connection.unwrap(Connection.class);
                assertNotNull(connection);
                assertSame(1, pool.getTotalConnections(), "Second total connections not as expected");
                assertSame(0, pool.getIdleConnections(), "Second idle connections not as expected");
            }
            assertSame(1, pool.getIdleConnections(), "Idle connections not as expected");
            try (Connection connection = ds.getConnection()) {
                unwrap2 = connection.unwrap(Connection.class);
                assertSame(unwrap, unwrap2);
                assertSame(1, pool.getTotalConnections(), "Second total connections not as expected");
                assertSame( 0, pool.getIdleConnections(), "Second idle connections not as expected");
            }
            PoolTestExtension.quietlySleep(800);
            try (Connection connection = ds.getConnection()) {
                unwrap2 = connection.unwrap(Connection.class);
                assertNotSame(unwrap, unwrap2, "Expected a different connection");
            }
            assertSame( 1, pool.getTotalConnections(), "Post total connections not as expected");
            assertSame(1, pool.getIdleConnections(), "Post idle connections not as expected");
        }
    }

    @Test
    public void testDoubleClose() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config);
             Connection connection = ds.getConnection()) {
            connection.close();
            connection.abort(null);
            assertTrue(connection.isClosed(), "Connection should have closed");
            assertFalse(connection.isValid(5), "Connection should have closed");
            assertTrue(connection.toString().contains("ProxyConnection"), "Expected to contain ProxyConnection, but was " + connection);
        }
    }

    @Test
    public void testEviction() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Connection connection = ds.getConnection();
            Pool pool = ds.getPool();
            assertEquals(1, pool.getTotalConnections());
            ds.evictConnection(connection);
            assertEquals(0, pool.getTotalConnections());
        }
    }

    @Test
    public void testEviction2() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        //config.setExceptionOverrideClassName(OverrideHandler.class.getName());
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            while (pool.getTotalConnections() < 5) {
                PoolTestExtension.quietlySleep(100L);
            }
            try (Connection connection = ds.getConnection()) {
                assertNotNull(connection);
                PreparedStatement statement = connection.prepareStatement("SELECT some, thing FROM somewhere WHERE something=?");
                assertNotNull(statement);
                ResultSet resultSet = statement.executeQuery();
                assertNotNull(resultSet);
                try {
                    statement.getMaxFieldSize();
                } catch (Exception e) {
                    assertSame(SQLException.class, e.getClass());
                }
            }
            assertEquals(5, pool.getTotalConnections(), "Total connections not as expected");
            assertEquals(5, pool.getIdleConnections(), "Idle connections not as expected");
        }
    }

    @Test
    public void testEviction3() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            while (pool.getTotalConnections() < 5) {
                PoolTestExtension.quietlySleep(100L);
            }
            try (Connection connection = ds.getConnection()) {
                assertNotNull(connection);
                PreparedStatement statement = connection.prepareStatement("SELECT some, thing FROM somewhere WHERE something=?");
                assertNotNull(statement);
                ResultSet resultSet = statement.executeQuery();
                assertNotNull(resultSet);
                try {
                    statement.getMaxFieldSize();
                } catch (Exception e) {
                    assertSame(SQLException.class, e.getClass());
                }
            }
            assertEquals(5, pool.getTotalConnections(), "Total connections not as expected");
            assertEquals(5, pool.getIdleConnections(), "Idle connections not as expected");
        }
    }

    @Test
    public void testEvictAllRefill() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config.setHousekeepingPeriodMs(100L);
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            for (int i = 0; i < 5; i++) {
                final Connection conn = ds.getConnection();
                ds.evictConnection(conn);
            }
            PoolTestExtension.quietlySleep(SECONDS.toMillis(2));
            int count = 0;
            while (pool.getIdleConnections() < 5 && count++ < 20) {
                PoolTestExtension.quietlySleep(100);
            }
            assertEquals(5, pool.getIdleConnections(),
                    "after eviction, refill did not reach expected 5 connections");
        }
    }

    @Test
    public void testBackfill() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(1000);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        StubConnection.slowCreate = true;
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            PoolTestExtension.quietlySleep(1250);
            assertSame(1, pool.getTotalConnections(), "Total connections not as expected");
            assertSame(1, pool.getIdleConnections(), "Idle connections not as expected");
            try (Connection connection = ds.getConnection()) {
                assertNotNull(connection);
                assertSame(1, pool.getTotalConnections(), "Total connections not as expected");
                assertSame(0, pool.getIdleConnections(), "Idle connections not as expected");
                PreparedStatement statement = connection.prepareStatement("SELECT some, thing FROM somewhere WHERE something=?");
                assertNotNull(statement);
                ResultSet resultSet = statement.executeQuery();
                assertNotNull(resultSet);
                try {
                    statement.getMaxFieldSize();
                    fail();
                } catch (Exception e) {
                    assertSame(SQLException.class, e.getClass());
                }
                pool.logPoolState("testBackfill() before close...");
            }
            assertSame(1, pool.getTotalConnections(), "Total connections not as expected");
            pool.logPoolState("testBackfill() after close...");
            PoolTestExtension.quietlySleep(1250);
            assertSame(1, pool.getTotalConnections(), "Total connections not as expected");
            assertSame( 1, pool.getIdleConnections(), "Idle connections not as expected");
        } finally {
            StubConnection.slowCreate = false;
        }
    }

    @Test
    public void testMaximumPoolLimit() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(20000);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        final AtomicReference<Exception> ref = new AtomicReference<>();
        StubConnection.count.set(0); // reset counter
        try (final PoolDataSource ds = new PoolDataSource(config)) {
            final Pool pool = ds.getPool();
            Thread[] threads = new Thread[20];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        pool.logPoolState("Before acquire ");
                        try (Connection ignored = ds.getConnection()) {
                            pool.logPoolState("After  acquire ");
                            PoolTestExtension.quietlySleep(500);
                        }
                    } catch (Exception e) {
                        ref.set(e);
                    }
                });
            }
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            pool.logPoolState("before check ");
            assertNull(ref.get(), (ref.get() != null ? ref.get().toString() : ""));
            assertSame(4, StubConnection.count.get(), "StubConnection count not as expected");
        }
    }

    @Test
    public void testOldDriver() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        StubConnection.oldDriver = true;
        StubStatement.oldDriver = true;
        try (PoolDataSource ds = new PoolDataSource(config)) {
            PoolTestExtension.quietlySleep(500);
            try (Connection ignored = ds.getConnection()) {
                // close
            }
            PoolTestExtension.quietlySleep(500);
            try (Connection ignored = ds.getConnection()) {
                // close
            }
        } finally {
            StubConnection.oldDriver = false;
            StubStatement.oldDriver = false;
        }
    }

    @Test
    public void testInitializationFailure1() throws Exception {
        StubDataSource stubDataSource = new StubDataSource();
        stubDataSource.setThrowException(new SQLException("Connection refused"));
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSource(stubDataSource);
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection ignored = ds.getConnection()) {
                fail("Initialization should have failed");
            } catch (SQLException e) {
                // passed
            }
        }
    }

    @Test
    public void testInitializationFailure2() throws Exception {
        StubDataSource stubDataSource = new StubDataSource();
        stubDataSource.setThrowException(new SQLException("Connection refused"));
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSource(stubDataSource);
        try (PoolDataSource ds = new PoolDataSource(config);
             Connection ignored = ds.getConnection()) {
            fail("Initialization should have failed");
        } catch (SQLTransientConnectionException e) {
            // passed
        }
    }

    @Test
    public void testInvalidConnectionTestQuery() throws Exception {
        class BadConnection extends StubConnection {
            @Override
            public Statement createStatement() throws SQLException {
                throw new SQLException("Simulated exception in createStatement()");
            }
        }
        StubDataSource stubDataSource = new StubDataSource() {
            @Override
            public Connection getConnection() {
                return new BadConnection();
            }
        };
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(3));
        config.setConnectionTestQuery("VALUES 1");
        config.setInitializationFailTimeout(TimeUnit.SECONDS.toMillis(2));
        config.setDataSource(stubDataSource);
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection ignored = ds.getConnection()) {
                fail("getConnection() should have failed");
            } catch (SQLException e) {
                assertSame("Simulated exception in createStatement()", e.getNextException().getMessage());
            }
        } catch (PoolInitializationException e) {
            assertSame("Simulated exception in createStatement()", e.getCause().getMessage());
        }
        config.setInitializationFailTimeout(0);
        try (PoolDataSource ignored = new PoolDataSource(config)) {
            fail("Initialization should have failed");
        } catch (PoolInitializationException e) {
            // passed
        }
    }

    @Test
    public void testDataSourceRaisesErrorWhileInitializationTestQuery() throws Exception {
        StubDataSourceWithErrorSwitch stubDataSource = new StubDataSourceWithErrorSwitch();
        stubDataSource.setErrorOnConnection(true);
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSource(stubDataSource);
        try (PoolDataSource ds = new PoolDataSource(config);
             Connection ignored = ds.getConnection()) {
            fail("Initialization should have failed");
        } catch (SQLTransientConnectionException e) {
            // passed
        }
    }

    @Disabled
    @Test
    public void testDataSourceRaisesErrorAfterInitializationTestQuery() throws Exception {
        StubDataSourceWithErrorSwitch stubDataSource = new StubDataSourceWithErrorSwitch();
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(3));
        config.setConnectionTestQuery("VALUES 1");
        config.setInitializationFailTimeout(TimeUnit.SECONDS.toMillis(2));
        config.setDataSource(stubDataSource);
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection ignored = ds.getConnection()) {
                stubDataSource.setErrorOnConnection(true);
                fail("SQLException should occur!");
            } catch (Exception e) {
                // request will get timed-out
                assertTrue(e.getMessage().contains("request timed out"));
            }
        }
    }

    @Test
    public void testPopulationSlowAcquisition() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMaximumPoolSize(20);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config.setHousekeepingPeriodMs(1000L);
        StubConnection.slowCreate = true;
        try (PoolDataSource ds = new PoolDataSource(config)) {
            ds.getPool().getConfig().setIdleTimeout(3000);
            SECONDS.sleep(2);
            Pool pool = ds.getPool();
            assertSame(1, pool.getTotalConnections(), "Total connections not as expected");
            assertSame(1, pool.getIdleConnections(), "Idle connections not as expected");
            try (Connection connection = ds.getConnection()) {
                assertNotNull(connection);
                SECONDS.sleep(20);
                assertSame(20, pool.getTotalConnections(), "Second total connections not as expected");
                assertSame(19, pool.getIdleConnections(), "Second idle connections not as expected");
            }
            assertSame(20, pool.getIdleConnections(), "Idle connections not as expected");
            SECONDS.sleep(5);
            assertSame(20, pool.getTotalConnections(), "Third total connections not as expected");
            assertSame(20, pool.getIdleConnections(), "Third idle connections not as expected");
        } finally {
            StubConnection.slowCreate = false;
        }
    }

    @Test
    public void testMinimumIdleZero() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(1000L);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config);
             Connection ignored = ds.getConnection()) {
            // passed
        } catch (SQLTransientConnectionException sqle) {
            fail("Failed to obtain connection");
        }
    }

    private static class StubDataSourceWithErrorSwitch extends StubDataSource {

        private boolean errorOnConnection = false;

        @Override
        public Connection getConnection() {
            if (!errorOnConnection) {
                return new StubConnection();
            }
            throw new RuntimeException("bad thing happens on datasource");
        }

        public void setErrorOnConnection(boolean errorOnConnection) {
            this.errorOnConnection = errorOnConnection;
        }
    }

    /*public static class OverrideHandler implements SQLExceptionOverride {
        @java.lang.Override
        public Override adjudicate(SQLException sqlException) {
            return (sqlException.getSQLState().equals("08999")) ? Override.DO_NOT_EVICT : Override.CONTINUE_EVICT;
        }
    }*/
}
