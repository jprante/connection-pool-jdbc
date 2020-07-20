package org.xbib.io.pool.jdbc;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Runnable that is scheduled in the future to report leaks.
 * The ScheduledFuture is cancelled if the connection is closed before the leak time expires.
 */
public class ProxyLeakTask implements Runnable {

    private static final Logger logger = Logger.getLogger(ProxyLeakTask.class.getName());

    public static final ProxyLeakTask NO_LEAK;

    private ScheduledFuture<?> scheduledFuture;

    private String connectionName;

    private Exception exception;

    private String threadName;

    private boolean isLeaked;

    static {
        NO_LEAK = new ProxyLeakTask() {
            @Override
            public void schedule(ScheduledExecutorService executorService, long leakDetectionThreshold) {
            }

            @Override
            public void run() {
            }

            @Override
            public void cancel() {
            }
        };
    }

    public ProxyLeakTask(final PoolEntry poolEntry) {
        this.exception = new Exception("Apparent connection leak detected");
        this.threadName = Thread.currentThread().getName();
        this.connectionName = poolEntry.getConnection().toString();
    }

    private ProxyLeakTask() {
    }

    public void schedule(ScheduledExecutorService executorService, long leakDetectionThreshold) {
        scheduledFuture = executorService.schedule(this, leakDetectionThreshold, TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        isLeaked = true;
        final StackTraceElement[] stackTrace = exception.getStackTrace();
        final StackTraceElement[] trace = new StackTraceElement[stackTrace.length - 5];
        System.arraycopy(stackTrace, 5, trace, 0, trace.length);
        exception.setStackTrace(trace);
        logger.log(Level.WARNING, "Connection leak detection triggered for on thread, stack trace follows: " + connectionName + " " + threadName, exception);
    }

    public void cancel() {
        scheduledFuture.cancel(false);
        if (isLeaked) {
            logger.log(Level.INFO, "Previously reported leaked connection on thread was returned to the pool (unleaked: )" + connectionName + " " + threadName);
        }
    }
}
