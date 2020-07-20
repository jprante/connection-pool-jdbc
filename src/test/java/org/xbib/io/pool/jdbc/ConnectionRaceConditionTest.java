package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class ConnectionRaceConditionTest {

    private static final Logger logger = Logger.getLogger(ConnectionRaceConditionTest.class.getName());

    public static final int ITERATIONS = 10_000;

    @Test
    public void testRaceCondition() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTimeout(5000);
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        final AtomicReference<Exception> ref = new AtomicReference<>(null);
        try (final PoolDataSource ds = new PoolDataSource(config)) {
            ExecutorService threadPool = Executors.newFixedThreadPool(2);
            for (int i = 0; i < ITERATIONS; i++) {
                threadPool.submit(new Callable<Exception>() {
                    @Override
                    public Exception call() throws Exception {
                        if (ref.get() == null) {
                            Connection c2;
                            try {
                                c2 = ds.getConnection();
                                ds.evictConnection(c2);
                            } catch (Exception e) {
                                ref.set(e);
                            }
                        }
                        return null;
                    }
                });
            }
            threadPool.shutdown();
            threadPool.awaitTermination(30, TimeUnit.SECONDS);
            if (ref.get() != null) {
                logger.severe("task failed: " + ref.get());
                fail("task failed");
            }
        } catch (Exception e) {
            throw e;
        }
    }
}
