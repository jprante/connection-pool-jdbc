package org.xbib.io.pool.jdbc;

import java.util.concurrent.ScheduledExecutorService;

/**
 * A factory for {@link ProxyLeakTask} Runnables that are scheduled in the future to report leaks.
 */
public class ProxyLeakTaskFactory {

    private final ScheduledExecutorService executorService;

    private long leakDetectionThreshold;

    public ProxyLeakTaskFactory(final long leakDetectionThreshold, final ScheduledExecutorService executorService) {
        this.executorService = executorService;
        this.leakDetectionThreshold = leakDetectionThreshold;
    }

    public ProxyLeakTask schedule(final PoolEntry poolEntry) {
        return (leakDetectionThreshold == 0) ? ProxyLeakTask.NO_LEAK : scheduleNewTask(poolEntry);
    }

    public void updateLeakDetectionThreshold(final long leakDetectionThreshold) {
        this.leakDetectionThreshold = leakDetectionThreshold;
    }

    private ProxyLeakTask scheduleNewTask(PoolEntry poolEntry) {
        ProxyLeakTask task = new ProxyLeakTask(poolEntry);
        task.schedule(executorService, leakDetectionThreshold);
        return task;
    }
}
