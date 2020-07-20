package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.xbib.io.pool.jdbc.mock.StubConnection;
import org.xbib.io.pool.jdbc.mock.StubDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UnwrapTest {

    @Test
    public void testUnwrapConnection() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            ds.getConnection().close();
            assertSame(1, ds.getPool().getIdleConnections(), "Idle connections not as expected");
            Connection connection = ds.getConnection();
            assertNotNull(connection);
            StubConnection unwrapped = connection.unwrap(StubConnection.class);
            assertNotNull(unwrapped, "unwrapped connection is not instance of StubConnection: " + unwrapped);
        }
    }

    @Test
    public void testUnwrapDataSource() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config)) {
            StubDataSource unwrap = ds.unwrap(StubDataSource.class);
            assertNotNull(unwrap);
            assertTrue(ds.isWrapperFor(PoolDataSource.class));
            assertNotNull(ds.unwrap(PoolDataSource.class));
            assertFalse(ds.isWrapperFor(getClass()));
            try {
                ds.unwrap(getClass());
            } catch (SQLException e) {
                Logger.getAnonymousLogger().log(Level.INFO, e.getMessage());
                assertTrue(e.getMessage().contains("wrapped DataSource"));
            }
        }
    }
}
