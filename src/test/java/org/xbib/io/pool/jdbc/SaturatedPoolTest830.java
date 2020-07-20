package org.xbib.io.pool.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.io.pool.jdbc.mock.StubConnection;
import org.xbib.io.pool.jdbc.mock.StubStatement;
import org.xbib.io.pool.jdbc.util.ClockSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@ExtendWith(PoolTestExtension.class)
public class SaturatedPoolTest830 {

    private static final Logger logger = Logger.getLogger(SaturatedPoolTest830.class.getName());

    private static final int MAX_POOL_SIZE = 10;

    @Test
    public void saturatedPoolTest() throws Exception {
        PoolConfig config = new PoolConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(MAX_POOL_SIZE);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTimeout(1000);
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        config.setHousekeepingPeriodMs(5000L);
        StubConnection.slowCreate = true;
        StubStatement.setSimulatedQueryTime(1000);
        final long start = ClockSource.currentTime();
        try (PoolDataSource ds = new PoolDataSource(config)) {
            LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
            ThreadPoolExecutor threadPool = new ThreadPoolExecutor( 50, 50,
                    2, TimeUnit.SECONDS, queue, new ThreadPoolExecutor.CallerRunsPolicy());
            threadPool.allowCoreThreadTimeOut(true);
            AtomicInteger windowIndex = new AtomicInteger();
            boolean[] failureWindow = new boolean[100];
            Arrays.fill(failureWindow, true);
            for (int i = 0; i < 50; i++) {
                threadPool.execute(() -> {
                    try (Connection conn = ds.getConnection();
                         Statement stmt = conn.createStatement()) {
                        stmt.execute("SELECT bogus FROM imaginary");
                    }
                    catch (SQLException e) {
                        logger.info(e.getMessage());
                    }
                });
            }
            long sleep = 80;
            outer:   while (true) {
                PoolTestExtension.quietlySleep(sleep);
                if (ClockSource.elapsedMillis(start) > TimeUnit.SECONDS.toMillis(12) && sleep < 100) {
                    sleep = 100;
                    logger.warning("switching to 100ms sleep");
                }
                else if (ClockSource.elapsedMillis(start) > TimeUnit.SECONDS.toMillis(6) && sleep < 90) {
                    sleep = 90;
                    logger.warning("switching to 90ms sleep");
                }
                threadPool.execute(() -> {
                    int ndx = windowIndex.incrementAndGet() % failureWindow.length;
                    try (Connection conn = ds.getConnection();
                         Statement stmt = conn.createStatement()) {
                        stmt.execute("SELECT bogus FROM imaginary");
                        failureWindow[ndx] = false;
                    }
                    catch (SQLException e) {
                        logger.info(e.getMessage());
                        failureWindow[ndx] = true;
                    }
                });
                for (boolean b : failureWindow) {
                    if (b) {
                        if (ClockSource.elapsedMillis(start) % (TimeUnit.SECONDS.toMillis(1) - sleep) < sleep) {
                            logger.info(MessageFormat.format("active threads {0}, submissions per second {1}, waiting threads {2}",
                                    threadPool.getActiveCount(),
                                    TimeUnit.SECONDS.toMillis(1) / sleep,
                                    ds.getPool().getThreadsAwaitingConnection()));
                        }
                        continue outer;
                    }
                }
                logger.info(MessageFormat.format("active threads {0}, submissions per second {1}, waiting threads {2}",
                        threadPool.getActiveCount(),
                        TimeUnit.SECONDS.toMillis(1) / sleep,
                        ds.getPool().getThreadsAwaitingConnection()));
                break;
            }
            logger.info("waiting for completion of active tasks: " + threadPool.getActiveCount());
            while (ds.getPool().getActiveConnections() > 0) {
                PoolTestExtension.quietlySleep(50);
            }
            assertEquals(TimeUnit.SECONDS.toMillis(1) / sleep, 10L, "Rate not in balance at 10req/s");
        }
        finally {
            StubStatement.setSimulatedQueryTime(0);
            StubConnection.slowCreate = false;
        }
    }
}
