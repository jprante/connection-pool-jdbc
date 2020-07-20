package org.xbib.io.pool.jdbc;

import java.io.Closeable;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * The pooled DataSource.
 */
public class PoolDataSource implements DataSource, Closeable {

    private static final Logger logger = Logger.getLogger(PoolDataSource.class.getName());

    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private final PoolConfig configuration;

    private Pool pool;

    /**
     * Construct a {@link PoolDataSource} with the specified configuration.  The
     * {@link PoolConfig} is copied and the pool is started by invoking this
     * constructor.
     * The {@link PoolConfig} can be modified without affecting the DataSource
     * and used to initialize another DataSource instance.
     *
     * @param configuration a config instance
     */
    public PoolDataSource(PoolConfig configuration)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.configuration = configuration;
        pool = new Pool(configuration);
    }

    public Pool getPool() {
        return pool;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        if (isClosed()) {
            throw new SQLException("PoolDataSource " + this + " has been closed");
        }
        Pool result = pool;
        if (result == null) {
            synchronized (this) {
                result = pool;
                if (result == null) {
                    configuration.validate();
                    logger.log(Level.INFO, "Starting: " + configuration.getPoolName());
                    try {
                        pool = result = new Pool(configuration);
                    } catch (Exception pie) {
                        throw new SQLException(pie);
                    }
                    logger.log(Level.INFO, "Start completed: " + configuration.getPoolName());
                }
            }
        }
        return result.getConnection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        Pool p = pool;
        return (p != null ? p.getUnwrappedDataSource().getLogWriter() : null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        Pool p = pool;
        if (p != null) {
            p.getUnwrappedDataSource().setLogWriter(out);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        Pool p = pool;
        if (p != null) {
            p.getUnwrappedDataSource().setLoginTimeout(seconds);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        Pool p = pool;
        return (p != null ? p.getUnwrappedDataSource().getLoginTimeout() : 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        Pool p = pool;
        if (p != null) {
            final DataSource unwrappedDataSource = p.getUnwrappedDataSource();
            if (iface.isInstance(unwrappedDataSource)) {
                return (T) unwrappedDataSource;
            }
            if (unwrappedDataSource != null) {
                return unwrappedDataSource.unwrap(iface);
            }
        }
        throw new SQLException("wrapped DataSource is not an instance of " + iface);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return true;
        }
        Pool p = pool;
        if (p != null) {
            final DataSource unwrappedDataSource = p.getUnwrappedDataSource();
            if (iface.isInstance(unwrappedDataSource)) {
                return true;
            }

            if (unwrappedDataSource != null) {
                return unwrappedDataSource.isWrapperFor(iface);
            }
        }
        return false;
    }

    /**
     * Evict a connection from the pool.  If the connection has already been closed (returned to the pool)
     * this may result in a "soft" eviction; the connection will be evicted sometime in the future if it is
     * currently in use.  If the connection has not been closed, the eviction is immediate.
     *
     * @param connection the connection to evict from the pool
     */
    public void evictConnection(Connection connection) {
        Pool p;
        if (!isClosed() && (p = pool) != null) {
            p.evictConnection(connection);
        }
    }

    /**
     * Shutdown the DataSource and its associated pool.
     */
    @Override
    public void close() {
        if (isShutdown.getAndSet(true)) {
            return;
        }
        Pool p = pool;
        if (p != null) {
            try {
                logger.log(Level.INFO, () -> "shutdown initiated: " + configuration.getPoolName());
                p.shutdown();
                logger.log(Level.INFO, () -> "shutdown completed: " + configuration.getPoolName());
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "interrupted during closing: " + configuration.getPoolName() + " " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Determine whether the DataSource has been closed.
     *
     * @return true if the DataSource has been closed, false otherwise
     */
    public boolean isClosed() {
        return isShutdown.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "PoolDataSource (" + pool + ")";
    }
}
