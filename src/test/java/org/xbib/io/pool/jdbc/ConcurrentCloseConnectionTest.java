package org.xbib.io.pool.jdbc;

import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ConcurrentCloseConnectionTest
{
    @Test
    public void testConcurrentClose() throws Exception
    {
        PoolConfig config = new PoolConfig();
        config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
        try (PoolDataSource ds = new PoolDataSource(config);
             final Connection connection = ds.getConnection()) {
            ExecutorService executorService = Executors.newFixedThreadPool(10);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                final PreparedStatement preparedStatement = connection.prepareStatement("");
                futures.add(executorService.submit((Callable<Void>) () -> {
                    preparedStatement.close();
                    return null;
                }));
            }
            executorService.shutdown();
            for (Future<?> future : futures) {
                future.get();
            }
        }
    }
}
