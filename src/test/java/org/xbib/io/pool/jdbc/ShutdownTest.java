package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.io.pool.jdbc.mock.StubConnection;
import org.xbib.io.pool.jdbc.util.ClockSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@ExtendWith(PoolTestExtension.class)
public class ShutdownTest {

    @BeforeEach
    public void beforeTest() {
        StubConnection.count.set(0);
    }

    @AfterEach
    public void afterTest() {
        StubConnection.slowCreate = false;
    }

    @Test
    public void testShutdown1() throws Exception {
        assertSame(0, StubConnection.count.get(), "StubConnection count not as expected");
        StubConnection.slowCreate = true;
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            Thread[] threads = new Thread[10];
            for (int i = 0; i < 10; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        if (ds.getConnection() != null) {
                            PoolTestExtension.quietlySleep(TimeUnit.SECONDS.toMillis(1));
                        }
                    }
                    catch (SQLException e) {
                        //
                    }
                });
                threads[i].setDaemon(true);
            }
            for (int i = 0; i < 10; i++) {
                threads[i].start();
            }
            PoolTestExtension.quietlySleep(1800L);
            assertTrue(pool.getTotalConnections() > 0, "total connection count not as expected");
            ds.close();
            assertSame(0, pool.getActiveConnections(), "active connection count not as expected");
            assertSame( 0, pool.getIdleConnections(), "idle connection count not as expected");
            assertSame(0, pool.getTotalConnections(), "total connection count not as expected");
            assertTrue(ds.isClosed());
        }
    }

    @Test
    public void testShutdown2() throws Exception {
        assertSame( 0, StubConnection.count.get(), "StubConnection count not as expected");
        StubConnection.slowCreate = true;
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            PoolTestExtension.quietlySleep(1200L);
            assertTrue( pool.getTotalConnections() > 0, "total connection count not as expected");
            ds.close();
            assertSame(0, pool.getActiveConnections(), "active connection count not as expected");
            assertSame(0, pool.getIdleConnections(), "idle connection count not as expected");
            assertSame(0, pool.getTotalConnections(), "Total connection count not as expected");
            assertTrue(ds.toString().startsWith("PoolDataSource (") && ds.toString().endsWith(")"));
        }
    }

    @Test
    public void testShutdown3() throws Exception {
        assertSame(0, StubConnection.count.get(), "StubConnection count not as expected");
        StubConnection.slowCreate = false;
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            PoolTestExtension.quietlySleep(1200L);
            assertEquals(5, pool.getTotalConnections(), "total connection count not as expected");
            ds.close();
            assertSame(0, pool.getActiveConnections(), "active connection count not as expected");
            assertSame( 0, pool.getIdleConnections(), "idle connection count not as expected");
            assertSame(0, pool.getTotalConnections(), "total connection count not as expected");
        }
    }

    @Test
    public void testShutdown4() throws Exception {
        StubConnection.slowCreate = true;
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            PoolTestExtension.quietlySleep(500L);
            ds.close();
            long startTime = ClockSource.currentTime();
            while (ClockSource.elapsedMillis(startTime) < TimeUnit.SECONDS.toMillis(5) && threadCount() > 0) {
                PoolTestExtension.quietlySleep(250);
            }
            assertSame(0, ds.getPool().getTotalConnections(), "unreleased connections after shutdown");
        }
    }

    @Test
    public void testShutdown5() throws Exception {
        assertSame( 0, StubConnection.count.get(), "StubConnection count not as expected");
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            for (int i = 0; i < 5; i++) {
                ds.getConnection();
            }
            assertEquals(5, pool.getTotalConnections(), "total connection count not as expected, ");
            ds.close();
            assertSame(0, pool.getActiveConnections(), "active connection count not as expected");
            assertSame(0, pool.getIdleConnections(), "idle connection count not as expected");
            assertSame(0, pool.getTotalConnections(), "total connection count not as expected");
        }
    }

    @Test
    public void testAfterShutdown() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(5);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            ds.close();
            try {
                ds.getConnection();
            }
            catch (SQLException e) {
                assertTrue(e.getMessage().contains("has been closed"));
            }
        }
    }

    @Test
    public void testShutdownDuringInit() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(1000);
        config.setValidationTimeout(1000);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            StubConnection.slowCreate = true;
            PoolTestExtension.quietlySleep(3000L);
        }
    }

    @Test
    public void testThreadedShutdown() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(1000);
        config.setValidationTimeout(1000);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        for (int i = 0; i < 4; i++) {
            try (PoolDataSource ds = new PoolDataSource(config)) {
                Thread t = new Thread(() -> {
                    try (Connection connection = ds.getConnection()) {
                        for (int i1 = 0; i1 < 10; i1++) {
                            Connection connection2 = null;
                            try {
                                connection2 = ds.getConnection();
                                PreparedStatement stmt = connection2.prepareStatement("SOMETHING");
                                PoolTestExtension.quietlySleep(20);
                                stmt.getMaxFieldSize();
                            }
                            catch (SQLException e) {
                                try {
                                    if (connection2 != null) {
                                        connection2.close();
                                    }
                                }
                                catch (SQLException e2) {
                                    if (e2.getMessage().contains("shutdown") || e2.getMessage().contains("evicted")) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception e) {
                        fail(e.getMessage());
                    }
                    finally {
                        ds.close();
                    }
                });
                t.start();
                Thread t2 = new Thread(() -> {
                    PoolTestExtension.quietlySleep(100);
                    try {
                        ds.close();
                    }
                    catch (IllegalStateException e) {
                        fail(e.getMessage());
                    }
                });
                t2.start();
                t.join();
                t2.join();
            }
        }
    }

    private int threadCount() {
        Thread[] threads = new Thread[Thread.activeCount() * 2];
        Thread.enumerate(threads);
        int count = 0;
        for (Thread thread : threads) {
            count += (thread != null && thread.getName().startsWith("Pool")) ? 1 : 0;
        }
        return count;
    }
}
