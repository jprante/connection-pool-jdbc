package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import org.junit.jupiter.api.Test;
import java.sql.Connection;

public class IsolationTest {

    @Test
    public void testIsolation() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setIsolateInternalQueries(true);
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection connection = ds.getConnection()) {
                connection.close();
                try (Connection connection2 = ds.getConnection()) {
                    connection2.close();
                    assertNotSame(connection, connection2);
                    assertSame(connection.unwrap(Connection.class), connection2.unwrap(Connection.class));
                }
            }
        }
    }

    @Test
    public void testNonIsolation() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setIsolateInternalQueries(false);
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            try (Connection connection = ds.getConnection()) {
                connection.close();
                try (Connection connection2 = ds.getConnection()) {
                    connection2.close();
                    assertSame(connection.unwrap(Connection.class), connection2.unwrap(Connection.class));
                }
            }
        }
    }
}
