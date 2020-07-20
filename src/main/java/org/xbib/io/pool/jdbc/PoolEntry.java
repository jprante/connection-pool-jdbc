package org.xbib.io.pool.jdbc;

import org.xbib.io.pool.jdbc.util.ClockSource;
import org.xbib.io.pool.jdbc.util.FastList;
import org.xbib.io.pool.jdbc.util.BagEntry;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Pool entry used in the Bag to track Connection instances.
 */
public class PoolEntry implements BagEntry {

    private static final Logger logger = Logger.getLogger(PoolEntry.class.getName());

    private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater =
            AtomicIntegerFieldUpdater.newUpdater(PoolEntry.class, "state");

    private Connection connection;

    private long lastAccessed;

    private long lastBorrowed;

    private volatile int state = 0;

    private volatile boolean evict;

    private volatile ScheduledFuture<?> endOfLife;

    private final FastList<Statement> openStatements;

    private final Pool pool;

    private final boolean isReadOnly;

    private final boolean isAutoCommit;

    public PoolEntry(Connection connection, Pool pool, final boolean isReadOnly, final boolean isAutoCommit) {
        this.connection = connection;
        this.pool = pool;
        this.isReadOnly = isReadOnly;
        this.isAutoCommit = isAutoCommit;
        this.lastAccessed = ClockSource.currentTime();
        this.openStatements = new FastList<>(Statement.class, 16);
    }

    public Connection getConnection() {
        return connection;
    }

    public Pool getPool() {
        return pool;
    }

    public long getLastAccessed() {
        return lastAccessed;
    }

    public long getLastBorrowed() {
        return lastBorrowed;
    }

    /**
     * Release this entry back to the pool.
     *
     * @param lastAccessed last access time-stamp
     */
    public void recycle(final long lastAccessed) {
        if (connection != null) {
            this.lastAccessed = lastAccessed;
            pool.recycle(this);
        }
    }

    /**
     * Set the end of life {@link ScheduledFuture}.
     *
     * @param endOfLife this PoolEntry/Connection's end of life {@link ScheduledFuture}
     */
    public void setFutureEol(final ScheduledFuture<?> endOfLife) {
        this.endOfLife = endOfLife;
    }

    public Connection createProxyConnection(final ProxyLeakTask leakTask, final long now) {
        return ProxyFactory.getProxyConnection(this, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
    }

    public String getPoolName() {
        return pool.toString();
    }

    public boolean isMarkedEvicted() {
        return evict;
    }

    public void markEvicted() {
        this.evict = true;
    }

    public void evict(final String closureReason) {
        pool.closeConnection(this, closureReason);
    }

    /**
     * Returns millis since lastBorrowed
     */
    public long getMillisSinceBorrowed() {
        return ClockSource.elapsedMillis(lastBorrowed);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final long now = ClockSource.currentTime();
        return connection
                + ", accessed " + ClockSource.elapsedDisplayString(lastAccessed, now) + " ago, "
                + stateToString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getState() {
        return stateUpdater.get(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean compareAndSet(int expect, int update) {
        return stateUpdater.compareAndSet(this, expect, update);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setState(int update) {
        stateUpdater.set(this, update);
    }

    public Connection close() {
        ScheduledFuture<?> eol = endOfLife;
        if (eol != null && !eol.isDone() && !eol.cancel(false)) {
            logger.log(Level.WARNING, "maxLifeTime expiration task cancellation unexpectedly returned false for connection " + getPoolName() + " " + connection);
        }
        Connection con = connection;
        connection = null;
        endOfLife = null;
        return con;
    }

    private String stateToString() {
        switch (state) {
            case STATE_IN_USE:
                return "IN_USE";
            case STATE_NOT_IN_USE:
                return "NOT_IN_USE";
            case STATE_REMOVED:
                return "REMOVED";
            case STATE_RESERVED:
                return "RESERVED";
            default:
                return "Invalid";
        }
    }
}
