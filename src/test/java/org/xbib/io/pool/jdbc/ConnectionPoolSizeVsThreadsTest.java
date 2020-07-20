package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.xbib.io.pool.jdbc.util.ClockSource.currentTime;
import static org.xbib.io.pool.jdbc.util.ClockSource.elapsedMillis;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.io.pool.jdbc.mock.StubDataSource;
import java.sql.Connection;
import java.text.MessageFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

@ExtendWith(PoolTestExtension.class)
public class ConnectionPoolSizeVsThreadsTest {

    private static final Logger LOGGER = Logger.getLogger(ConnectionPoolSizeVsThreadsTest.class.getName());

    private static final int ITERATIONS = 50_000;

    @Test
    public void testPoolSizeAboutSameSizeAsThreadCount() throws Exception {
        final int threadCount = 50;
        final Counts counts = testPoolSize(2, 100,
                threadCount, 1, 0, 20,
                ITERATIONS, TimeUnit.SECONDS.toMillis(2));
        assertEquals(threadCount, counts.maxActive, 15);
        assertEquals(threadCount, counts.maxTotal,  5);
    }

    @Test
    public void testSlowConnectionTimeBurstyWork() throws Exception {
        final int threadCount = 50;
        final int workItems = threadCount * 100;
        final int workTimeMs = 0;
        final int connectionAcquisitionTimeMs = 250;
        final Counts counts = testPoolSize(2, 100,
                threadCount, workTimeMs, 0, connectionAcquisitionTimeMs,
                workItems, TimeUnit.SECONDS.toMillis(3));
        final long totalWorkTime = workItems * workTimeMs;
        final long connectionMax = totalWorkTime / connectionAcquisitionTimeMs;
        assertTrue(connectionMax <= counts.maxActive);
        assertEquals(connectionMax, counts.maxTotal, 2 + 2);
    }

    private Counts testPoolSize(final int minIdle,
                                final int maxPoolSize,
                                final int threadCount,
                                final long workTimeMs,
                                final long restTimeMs,
                                final long connectionAcquisitionTimeMs,
                                final int iterations,
                                final long postTestTimeMs) throws Exception {
        LOGGER.info(MessageFormat.format("Starting test (minIdle={0}, maxPoolSize={1}, threadCount={2}, workTimeMs={3}, restTimeMs={4}, connectionAcquisitionTimeMs={5}, iterations={6}, postTestTimeMs={7})",
                minIdle, maxPoolSize, threadCount, workTimeMs, restTimeMs, connectionAcquisitionTimeMs, iterations, postTestTimeMs));
        final PoolConfig config = new PoolConfig();
        config.setMinimumIdle(minIdle);
        config.setMaximumPoolSize(maxPoolSize);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTimeout(2500);
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        final AtomicReference<Exception> ref = new AtomicReference<>(null);
        try (final PoolDataSource ds = new PoolDataSource(config)) {
            final StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
            // connection acquisition takes more than 0 ms in a real system
            stubDataSource.setConnectionAcquistionTime(connectionAcquisitionTimeMs);
            final ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
            final CountDownLatch allThreadsDone = new CountDownLatch(iterations);
            for (int i = 0; i < iterations; i++) {
                threadPool.submit(() -> {
                    if (ref.get() == null) {
                        PoolTestExtension.quietlySleep(restTimeMs);
                        try (Connection c2 = ds.getConnection()) {
                            PoolTestExtension.quietlySleep(workTimeMs);
                        }
                        catch (Exception e) {
                            ref.set(e);
                        }
                    }
                    allThreadsDone.countDown();
                });
            }
            final Pool pool = ds.getPool();
            final Counts underLoad = new Counts();
            while (allThreadsDone.getCount() > 0 || pool.getTotalConnections() < minIdle) {
                PoolTestExtension.quietlySleep(50);
                underLoad.updateMaxCounts(pool);
            }
            LOGGER.info(MessageFormat.format("test over, waiting for post delay time {0} ms ", postTestTimeMs));
            PoolTestExtension.quietlySleep(connectionAcquisitionTimeMs + workTimeMs + restTimeMs);
            final Counts postLoad = new Counts();
            final long start = currentTime();
            while (elapsedMillis(start) < postTestTimeMs) {
                PoolTestExtension.quietlySleep(50);
                postLoad.updateMaxCounts(pool);
            }
            allThreadsDone.await();
            threadPool.shutdown();
            threadPool.awaitTermination(30, TimeUnit.SECONDS);
            if (ref.get() != null) {
                LOGGER.severe("task failed: " + ref.get());
                fail("task failed");
            }
            LOGGER.info("under load... " + underLoad);
            LOGGER.info("post load.... " + postLoad);
            if (postTestTimeMs > 0) {
                if (postLoad.maxActive != 0) {
                    fail("max active was greater than 0 after test was done");
                }
                final int createdAfterWorkAllFinished = postLoad.maxTotal - underLoad.maxTotal;
                assertEquals(0, createdAfterWorkAllFinished, 1,
                        "connections were created when there was no waiting consumers");
            }
            return underLoad;
        }
    }

    private static class Counts {
        int maxTotal = 0;
        int maxActive = 0;

        void updateMaxCounts(Pool pool) {
            maxTotal = Math.max(pool.getTotalConnections(), maxTotal);
            maxActive = Math.max(pool.getActiveConnections(), maxActive);
        }

        @Override
        public String toString() {
            return "counts{" + "max total=" + maxTotal + ", max active=" + maxActive + '}';
        }
    }
}
