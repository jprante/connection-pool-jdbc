package org.xbib.io.pool.jdbc;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.io.pool.jdbc.util.Bag;
import java.util.concurrent.CompletableFuture;

@ExtendWith(PoolTestExtension.class)
public class BagTest {

   private static PoolDataSource ds;

   private static Pool pool;

   @BeforeAll
   public static void setup() throws Exception {
      PoolConfig config = new PoolConfig();
      config.setMinimumIdle(1);
      config.setMaximumPoolSize(2);
      config.setInitializationFailTimeout(0);
      config.setConnectionTestQuery("VALUES 1");
      config.setDataSourceClassName("org.xbib.io.pool.jdbc.mock.StubDataSource");
      ds = new PoolDataSource(config);
      pool = ds.getPool();
   }

   @AfterAll
   public static void teardown()
   {
      ds.close();
   }

   @Test
   public void testBag() throws Exception {
      try (Bag<PoolEntry> bag = new Bag<>((x) -> CompletableFuture.completedFuture(Boolean.TRUE))) {
         assertEquals(0, bag.values(8).size());
         PoolEntry reserved = pool.newPoolEntry();
         bag.add(reserved);
         bag.reserve(reserved);
         PoolEntry inuse = pool.newPoolEntry();
         bag.add(inuse);
         bag.borrow(2, MILLISECONDS);
         PoolEntry notinuse = pool.newPoolEntry();
         bag.add(notinuse);
         bag.dumpState();
         bag.requite(reserved);
         bag.remove(notinuse);
         assertTrue(bag.getLastMessage().contains("not borrowed or reserved"));
         bag.unreserve(notinuse);
         assertTrue(bag.getLastMessage().contains("was not reserved"));
         bag.remove(inuse);
         bag.remove(inuse);
         assertTrue(bag.getLastMessage().contains("not borrowed or reserved"));
         bag.close();
         try {
            PoolEntry bagEntry = pool.newPoolEntry();
            bag.add(bagEntry);
            assertNotEquals(bagEntry, bag.borrow(100, MILLISECONDS));
         }
         catch (IllegalStateException e) {
            assertTrue(bag.getLastMessage().contains("ignoring add()"));
         }
         assertNotNull(notinuse.toString());
      }
   }
}
