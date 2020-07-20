package org.xbib.io.pool.jdbc;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;
import org.xbib.io.pool.jdbc.mock.MockDataSource;
import org.xbib.io.pool.jdbc.util.ClockSource;

/**
 * Test for cases when db network connectivity goes down and close is called on
 * existing connections. By default we block longer than getMaximumTimeout
 * (it can hang for a lot of time depending on driver timeout settings).
 * Closing the connections asynchronously fixes this issue.
 */
@ExtendWith(PoolTestExtension.class)
public class ConnectionCloseBlockingTest {

   private static volatile boolean shouldFail = false;

   @Disabled
   @Test
   public void testConnectionCloseBlocking() throws Exception {
      PoolConfig config = new PoolConfig();
      config.setMinimumIdle(0);
      config.setMaximumPoolSize(1);
      config.setConnectionTimeout(1500);
      config.setDataSource(new CustomMockDataSource());
      long start = ClockSource.currentTime();
      try (PoolDataSource ds = new PoolDataSource(config);
            Connection connection = ds.getConnection()) {
            connection.close();
            PoolTestExtension.quietlySleep(1100L);
            shouldFail = true;
            try (Connection connection2 = ds.getConnection()) {
               assertTrue((ClockSource.elapsedMillis(start) < config.getConnectionTimeout()),
                       "waited longer than timeout: " + config.getConnectionTimeout());
            }
      } catch (SQLException e) {
         assertTrue((ClockSource.elapsedMillis(start) < config.getConnectionTimeout()),
                 "getConnection failed because close connection took longer than timeout");
      }
   }

   private static class CustomMockDataSource extends MockDataSource {
      @Override
      public Connection getConnection() throws SQLException {
         Connection mockConnection = super.getConnection();
         when(mockConnection.isValid(anyInt())).thenReturn(!shouldFail);
         doAnswer((Answer<Void>) invocation -> {
            if (shouldFail) {
               SECONDS.sleep(2);
            }
            return null;
         }).when(mockConnection).close();
         return mockConnection;
      }
   }

}
