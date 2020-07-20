package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementTest {

    private PoolDataSource ds;

    @BeforeEach
    public void setup() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(2);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        ds = new PoolDataSource(config);
    }

    @AfterEach
    public void teardown() {
        ds.close();
    }

    @Test
    public void testStatementClose() throws SQLException {
        ds.getConnection().close();
        Pool pool = ds.getPool();
        assertTrue(pool.getTotalConnections() >= 1, "total connections not as expected");
        assertTrue(pool.getIdleConnections() >= 1, "idle connections not as expected");
        try (Connection connection = ds.getConnection()) {
            assertNotNull(connection);
            assertTrue(pool.getTotalConnections() >= 1, "total connections not as expected");
            assertTrue(pool.getIdleConnections() >= 0, "idle connections not as expected");
            Statement statement = connection.createStatement();
            assertNotNull(statement);
            connection.close();
            assertTrue(statement.isClosed());
        }
    }

    @Test
    public void testAutoStatementClose() throws SQLException {
        try (Connection connection = ds.getConnection()) {
            assertNotNull(connection);
            Statement statement1 = connection.createStatement();
            assertNotNull(statement1);
            Statement statement2 = connection.createStatement();
            assertNotNull(statement2);
            connection.close();
            assertTrue(statement1.isClosed());
            assertTrue(statement2.isClosed());
        }
    }

    @Test
    public void testStatementResultSetProxyClose() throws SQLException {
        try (Connection connection = ds.getConnection()) {
            assertNotNull(connection);
            Statement statement1 = connection.createStatement();
            assertNotNull(statement1);
            Statement statement2 = connection.createStatement();
            assertNotNull(statement2);
            statement1.getResultSet().getStatement().close();
            statement2.getGeneratedKeys().getStatement().close();
            assertTrue(statement1.isClosed());
            assertTrue(statement2.isClosed());
        }
    }

    @Test
    public void testDoubleStatementClose() throws SQLException {
        try (Connection connection = ds.getConnection();
             Statement statement1 = connection.createStatement()) {
            statement1.close();
            statement1.close();
        }
    }

    @Test
    public void testOutOfOrderStatementClose() throws SQLException {
        try (Connection connection = ds.getConnection();
             Statement statement1 = connection.createStatement();
             Statement statement2 = connection.createStatement()) {
            statement1.close();
            statement2.close();
        }
    }
}
