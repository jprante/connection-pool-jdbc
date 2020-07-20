package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertSame;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import java.sql.Connection;

@ExtendWith(PoolTestExtension.class)
public class RampUpDownTest {

    @Test
    public void rampUpDownTest() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(60);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config.setIdleTimeout(1000);
        config.setHousekeepingPeriodMs(250L);
        try (PoolDataSource ds = new PoolDataSource(config)) {
            Pool pool = ds.getPool();
            assertSame(1, pool.getTotalConnections(), "total connections not as expected");
            PoolTestExtension.quietlySleep(500);
            Connection[] connections = new Connection[ds.getPool().getConfig().getMaximumPoolSize()];
            for (int i = 0; i < connections.length; i++) {
                connections[i] = ds.getConnection();
            }
            assertSame(60, pool.getTotalConnections(), "total connections not as expected");
            for (Connection connection : connections) {
                connection.close();
            }
            PoolTestExtension.quietlySleep(500);
            assertSame(60, pool.getIdleConnections(), "idle connections not as expected");
        }
    }
}
