package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class ConnectionStateTest {

    @Test
    public void testAutoCommit() throws Exception {
        Properties properties = new Properties();
        properties.put("user", "bar");
        properties.put("password", "secret");
        properties.put("url", "baf");
        properties.put("loginTimeout", "10");
        PoolConfig config = new PoolConfig(properties);
        config.setAutoCommit(true);
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection connection = ds.getConnection()) {
                Connection unwrap = connection.unwrap(Connection.class);
                unwrap.setAutoCommit(false);
                connection.close();
                assertFalse(unwrap.getAutoCommit());
            }
        }
    }

    @Test
    public void testTransactionIsolation() throws Exception {
        Properties properties = new Properties();
        PoolConfig config = new PoolConfig(properties);
        config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection connection = ds.getConnection()) {
                Connection unwrap = connection.unwrap(Connection.class);
                unwrap.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                connection.close();
                assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, unwrap.getTransactionIsolation());
            }
        }
    }

    @Test
    public void testIsolation() {
        PoolConfig config = new PoolConfig();
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
        config.validate();
        int transactionIsolation = PoolTestExtension.getTransactionIsolation(config.getTransactionIsolation());
        assertSame(Connection.TRANSACTION_REPEATABLE_READ, transactionIsolation);
    }

    @Test
    public void testReadOnly() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setCatalog("test");
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection connection = ds.getConnection()) {
                Connection unwrap = connection.unwrap(Connection.class);
                connection.setReadOnly(true);
                connection.close();
                assertFalse(unwrap.isReadOnly());
            }
        }
    }

    @Test
    public void testCatalog() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setCatalog("test");
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection connection = ds.getConnection()) {
                Connection unwrap = connection.unwrap(Connection.class);
                connection.setCatalog("other");
                connection.close();
                assertEquals("test", unwrap.getCatalog());
            }
        }
    }

    @Test
    public void testCommitTracking() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setAutoCommit(false);
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection connection = ds.getConnection()) {
                Statement statement = connection.createStatement();
                statement.execute("SELECT something");
                assertTrue(((ProxyConnection)connection).isCommitStateDirty());
                connection.commit();
                assertFalse(((ProxyConnection)connection).isCommitStateDirty());
                statement.execute("SELECT something", Statement.NO_GENERATED_KEYS);
                assertTrue(((ProxyConnection)connection).isCommitStateDirty());
                connection.rollback();
                assertFalse(((ProxyConnection)connection).isCommitStateDirty());
                ResultSet resultSet = statement.executeQuery("SELECT something");
                assertTrue(((ProxyConnection)connection).isCommitStateDirty());
                connection.rollback(null);
                assertFalse(((ProxyConnection)connection).isCommitStateDirty());
                resultSet.updateRow();
                assertTrue(((ProxyConnection)connection).isCommitStateDirty());
            }
        }
    }
}
