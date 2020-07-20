package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xbib.io.pool.jdbc.util.DefaultThreadFactory;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HouseKeeperCleanupTest {

    private ScheduledThreadPoolExecutor executor;

    @BeforeEach
    public void before() throws Exception {
        ThreadFactory threadFactory = new DefaultThreadFactory("global-housekeeper", true);
        executor = new ScheduledThreadPoolExecutor(1, threadFactory,
                new ThreadPoolExecutor.DiscardPolicy());
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setRemoveOnCancelPolicy(true);
    }

    @Test
    public void testHouseKeeperCleanupWithCustomExecutor() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTimeout(2500);
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config.setScheduledExecutor(executor);
        PoolConfig config2 = new PoolConfig();
        config2.setMinimumIdle(0);
        config2.setMaximumPoolSize(10);
        config2.setInitializationFailTimeout(Long.MAX_VALUE);
        config2.setConnectionTimeout(2500);
        config2.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config2.setScheduledExecutor(executor);
        try (
                final PoolDataSource ds1 = new PoolDataSource(config);
                final PoolDataSource ds2 = new PoolDataSource(config2)) {
            assertEquals(4, executor.getQueue().size(), "scheduled tasks count not as expected");
        }
        assertEquals( 0, executor.getQueue().size(), "scheduled tasks count not as expected");
    }

    @AfterEach
    public void after() throws Exception {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
