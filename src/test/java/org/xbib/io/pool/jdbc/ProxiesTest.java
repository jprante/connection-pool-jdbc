package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.io.pool.jdbc.mock.StubConnection;
import org.xbib.io.pool.jdbc.mock.StubStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@ExtendWith(PoolTestExtension.class)
public class ProxiesTest {

    @Test
    public void testProxyCreation() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Connection conn = ds.getConnection();
            assertNotNull(conn.createStatement(ResultSet.FETCH_FORWARD, ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertNotNull(conn.createStatement(ResultSet.FETCH_FORWARD, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.HOLD_CURSORS_OVER_COMMIT));
            assertNotNull(conn.prepareCall("some sql"));
            assertNotNull(conn.prepareCall("some sql", ResultSet.FETCH_FORWARD, ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertNotNull(conn.prepareCall("some sql", ResultSet.FETCH_FORWARD, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.HOLD_CURSORS_OVER_COMMIT));
            assertNotNull(conn.prepareStatement("some sql", PreparedStatement.NO_GENERATED_KEYS));
            assertNotNull(conn.prepareStatement("some sql", new int[3]));
            assertNotNull(conn.prepareStatement("some sql", new String[3]));
            assertNotNull(conn.prepareStatement("some sql", ResultSet.FETCH_FORWARD, ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertNotNull(conn.prepareStatement("some sql", ResultSet.FETCH_FORWARD, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.HOLD_CURSORS_OVER_COMMIT));
            assertNotNull(conn.toString());
            assertTrue(conn.isWrapperFor(Connection.class));
            assertTrue(conn.isValid(10));
            assertFalse(conn.isClosed());
            assertNotNull(conn.unwrap(StubConnection.class));
            try {
                conn.unwrap(ProxiesTest.class);
                fail();
            } catch (SQLException e) {
                // pass
            }
        }
    }

    @Test
    public void testStatementProxy() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Connection conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement("some sql");
            stmt.executeQuery();
            stmt.executeQuery("some sql");
            assertFalse(stmt.isClosed());
            assertNotNull(stmt.getGeneratedKeys());
            assertNotNull(stmt.getResultSet());
            assertNotNull(stmt.getConnection());
            assertNotNull(stmt.unwrap(StubStatement.class));
            try {
                stmt.unwrap(ProxiesTest.class);
                fail();
            } catch (SQLException e) {
                // pass
            }
        }
    }

    @Test
    public void testStatementExceptions() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(1));
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Connection conn = ds.getConnection();
            StubConnection stubConnection = conn.unwrap(StubConnection.class);
            stubConnection.throwException = true;
            try {
                conn.createStatement();
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.createStatement(0, 0);
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.createStatement(0, 0, 0);
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.prepareCall("");
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.prepareCall("", 0, 0);
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.prepareCall("", 0, 0, 0);
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.prepareStatement("");
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.prepareStatement("", 0);
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.prepareStatement("", new int[0]);
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.prepareStatement("", new String[0]);
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.prepareStatement("", 0, 0);
                fail();
            } catch (SQLException e) {
                // pass
            }
            try {
                conn.prepareStatement("", 0, 0, 0);
                fail();
            } catch (SQLException e) {
                // pass
            }
        }
    }

    @Test
    public void testOtherExceptions() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection conn = ds.getConnection()) {
                StubConnection stubConnection = conn.unwrap(StubConnection.class);
                stubConnection.throwException = true;
                try {
                    conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    conn.isReadOnly();
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    conn.setReadOnly(false);
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    conn.setCatalog("");
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    conn.setAutoCommit(false);
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    conn.clearWarnings();
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    conn.isValid(0);
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    conn.isWrapperFor(getClass());
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    conn.unwrap(getClass());
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    conn.close();
                    fail();
                } catch (SQLException e) {
                    // pass
                }
                try {
                    assertFalse(conn.isValid(0));
                } catch (SQLException e) {
                    fail();
                }
            }
        }
    }
}
