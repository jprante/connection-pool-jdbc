package org.xbib.io.pool.jdbc;

import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class PoolConfig {

    private static final Logger logger = Logger.getLogger(PoolConfig.class.getName());

    private static final AtomicLong POOL_COUNTER = new AtomicLong();

    private static final long CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    private static final long VALIDATION_TIMEOUT = TimeUnit.SECONDS.toMillis(5);

    private static final long IDLE_TIMEOUT = TimeUnit.MINUTES.toMillis(10);

    private static final long MAX_LIFETIME = TimeUnit.MINUTES.toMillis(30);

    private static final int DEFAULT_POOL_SIZE = 8;

    private final Properties properties;

    private volatile long connectionTimeout;

    private volatile long validationTimeout;

    private volatile long idleTimeout;

    private volatile long leakDetectionThreshold;

    private volatile long maxLifetime;

    private volatile int maxPoolSize;

    private volatile int minIdle;

    private volatile String username;

    private volatile String password;

    private long initializationFailTimeout;

    private String connectionInitSql;

    private String connectionTestQuery;

    private String dataSourceClassName;

    private String driverClassName;

    private String jdbcUrl;

    private String poolName;

    private String catalog;

    private String schema;

    private String transactionIsolationName;

    private boolean isAutoCommit;

    private boolean isReadOnly;

    private boolean isIsolateInternalQueries;

    private boolean isAllowPoolSuspension;

    private long aliveBypassWindowMs;

    private long housekeepingPeriodMs;

    private DataSource dataSource;

    private ThreadFactory threadFactory;

    private ScheduledExecutorService scheduledExecutor;

    /**
     * Default constructor
     */
    public PoolConfig() {
        this(new Properties());
    }

    /**
     * Construct a {@link PoolConfig} from the specified properties object.
     *
     * @param properties the name of the property file
     */
    public PoolConfig(Properties properties) {
        this.properties = properties;
        this.minIdle = -1;
        this.maxPoolSize = -1;
        this.maxLifetime = MAX_LIFETIME;
        this.connectionTimeout = CONNECTION_TIMEOUT;
        this.validationTimeout = VALIDATION_TIMEOUT;
        this.idleTimeout = IDLE_TIMEOUT;
        this.initializationFailTimeout = -1;
        this.isAutoCommit = true;
        this.jdbcUrl = properties.getProperty("url");
        this.aliveBypassWindowMs = TimeUnit.MILLISECONDS.toMillis(500);
        this.housekeepingPeriodMs = TimeUnit.SECONDS.toMillis(30);
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(long connectionTimeoutMs) {
        if (connectionTimeoutMs == 0) {
            this.connectionTimeout = Integer.MAX_VALUE;
        } else if (connectionTimeoutMs < 250) {
            throw new IllegalArgumentException("connectionTimeout cannot be less than 250ms");
        } else {
            this.connectionTimeout = connectionTimeoutMs;
        }
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeoutMs) {
        if (idleTimeoutMs < 0) {
            throw new IllegalArgumentException("idleTimeout cannot be negative");
        }
        this.idleTimeout = idleTimeoutMs;
    }

    public long getLeakDetectionThreshold() {
        return leakDetectionThreshold;
    }

    public void setLeakDetectionThreshold(long leakDetectionThresholdMs) {
        this.leakDetectionThreshold = leakDetectionThresholdMs;
    }

    public long getMaxLifetime() {
        return maxLifetime;
    }

    public void setMaxLifetime(long maxLifetimeMs) {
        this.maxLifetime = maxLifetimeMs;
    }

    public int getMaximumPoolSize() {
        return maxPoolSize;
    }

    public void setMaximumPoolSize(int maxPoolSize) {
        if (maxPoolSize < 1) {
            throw new IllegalArgumentException("maxPoolSize cannot be less than 1");
        }
        this.maxPoolSize = maxPoolSize;
    }

    public int getMinimumIdle() {
        return minIdle;
    }

    public void setMinimumIdle(int minIdle) {
        if (minIdle < 0) {
            throw new IllegalArgumentException("minimumIdle cannot be negative");
        }
        this.minIdle = minIdle;
    }

    /**
     * Get the default password to use for DataSource.getConnection(username, password) calls.
     *
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Set the default password to use for DataSource.getConnection(username, password) calls.
     *
     * @param password the password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Get the default username used for DataSource.getConnection(username, password) calls.
     *
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Set the default username used for DataSource.getConnection(username, password) calls.
     *
     * @param username the username
     */
    public void setUsername(String username) {
        this.username = username;
    }

    public long getValidationTimeout() {
        return validationTimeout;
    }

    public void setValidationTimeout(long validationTimeoutMs) {
        if (validationTimeoutMs < 250) {
            throw new IllegalArgumentException("validationTimeout cannot be less than 250ms");
        }
        this.validationTimeout = validationTimeoutMs;
    }

    /**
     * Get the SQL query to be executed to test the validity of connections.
     *
     * @return the SQL query string, or null
     */
    public String getConnectionTestQuery() {
        return connectionTestQuery;
    }

    /**
     * Set the SQL query to be executed to test the validity of connections. Using
     * the JDBC4 <code>Connection.isValid()</code> method to test connection validity can
     * be more efficient on some databases and is recommended.
     *
     * @param connectionTestQuery a SQL query string
     */
    public void setConnectionTestQuery(String connectionTestQuery) {
        this.connectionTestQuery = connectionTestQuery;
    }

    /**
     * Get the SQL string that will be executed on all new connections when they are
     * created, before they are added to the pool.
     *
     * @return the SQL to execute on new connections, or null
     */
    public String getConnectionInitSql() {
        return connectionInitSql;
    }

    /**
     * Set the SQL string that will be executed on all new connections when they are
     * created, before they are added to the pool.  If this query fails, it will be
     * treated as a failed connection attempt.
     *
     * @param connectionInitSql the SQL to execute on new connections
     */
    public void setConnectionInitSql(String connectionInitSql) {
        this.connectionInitSql = connectionInitSql;
    }

    /**
     * Get the {@link DataSource} that has been explicitly specified to be wrapped by the
     * pool.
     *
     * @return the {@link DataSource} instance, or null
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Set a {@link DataSource} for the pool to explicitly wrap.  This setter is not
     * available through property file based initialization.
     *
     * @param dataSource a specific {@link DataSource} to be wrapped by the pool
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Get the name of the JDBC {@link DataSource} class used to create Connections.
     *
     * @return the fully qualified name of the JDBC {@link DataSource} class
     */
    public String getDataSourceClassName() {
        return dataSourceClassName;
    }

    /**
     * Set the fully qualified class name of the JDBC {@link DataSource} that will be used create Connections.
     *
     * @param className the fully qualified name of the JDBC {@link DataSource} class
     */
    public void setDataSourceClassName(String className) {
        this.dataSourceClassName = className;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        Class<?> driverClass = attemptFromContextLoader(driverClassName);
        try {
            if (driverClass == null) {
                driverClass = this.getClass().getClassLoader().loadClass(driverClassName);
                logger.log(Level.FINE, () -> "driver class found in the PoolConfig class classloader: " + driverClassName + " " + this.getClass().getClassLoader());
            }
        } catch (ClassNotFoundException e) {
            logger.log(Level.SEVERE, "failed to load driver class from PoolConfig class classloader: " + driverClassName + " " + this.getClass().getClassLoader());
        }
        if (driverClass == null) {
            throw new RuntimeException("failed to load driver class " + driverClassName + " in either of PoolConfig class loader or Thread context classloader");
        }
        try {
            driverClass.getConstructor().newInstance();
            this.driverClassName = driverClassName;
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate class " + driverClassName, e);
        }
    }

    /**
     * Get the default auto-commit behavior of connections in the pool.
     *
     * @return the default auto-commit behavior of connections
     */
    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    /**
     * Set the default auto-commit behavior of connections in the pool.
     *
     * @param isAutoCommit the desired auto-commit default for connections
     */
    public void setAutoCommit(boolean isAutoCommit) {
        this.isAutoCommit = isAutoCommit;
    }

    /**
     * Get the pool suspension behavior (allowed or disallowed).
     *
     * @return the pool suspension behavior
     */
    public boolean isAllowPoolSuspension() {
        return isAllowPoolSuspension;
    }

    /**
     * Set whether or not pool suspension is allowed.  There is a performance
     * impact when pool suspension is enabled.  Unless you need it (for a
     * redundancy system for example) do not enable it.
     *
     * @param isAllowPoolSuspension the desired pool suspension allowance
     */
    public void setAllowPoolSuspension(boolean isAllowPoolSuspension) {
        this.isAllowPoolSuspension = isAllowPoolSuspension;
    }

    /**
     * Get the pool initialization failure timeout.  See {@code #setInitializationFailTimeout(long)}
     * for details.
     *
     * @return the number of milliseconds before the pool initialization fails
     * @see PoolConfig#setInitializationFailTimeout(long)
     */
    public long getInitializationFailTimeout() {
        return initializationFailTimeout;
    }

    /**
     * Set the pool initialization failure timeout.  This setting applies to pool
     * initialization when {@link PoolDataSource} is constructed with a {@link PoolConfig},
     * or when {@link PoolDataSource} is constructed using the no-arg constructor
     * and {@link PoolDataSource#getConnection()} is called.
     * <ul>
     *   <li>Any value greater than zero will be treated as a timeout for pool initialization.
     *       The calling thread will be blocked from continuing until a successful connection
     *       to the database, or until the timeout is reached.  If the timeout is reached, then
     *       a {@code PoolInitializationException} will be thrown. </li>
     *   <li>A value of zero will <i>not</i>  prevent the pool from starting in the
     *       case that a connection cannot be obtained. However, upon start the pool will
     *       attempt to obtain a connection and validate that the {@code connectionTestQuery}
     *       and {@code connectionInitSql} are valid.  If those validations fail, an exception
     *       will be thrown.  If a connection cannot be obtained, the validation is skipped
     *       and the the pool will start and continue to try to obtain connections in the
     *       background.  This can mean that callers to {@code DataSource#getConnection()} may
     *       encounter exceptions. </li>
     *   <li>A value less than zero will bypass any connection attempt and validation during
     *       startup, and therefore the pool will start immediately.  The pool will continue to
     *       try to obtain connections in the background. This can mean that callers to
     *       {@code DataSource#getConnection()} may encounter exceptions. </li>
     * </ul>
     * Note that if this timeout value is greater than or equal to zero (0), and therefore an
     * initial connection validation is performed, this timeout does not override the
     * {@code connectionTimeout} or {@code validationTimeout}; they will be honored before this
     * timeout is applied.  The default value is one millisecond.
     *
     * @param initializationFailTimeout the number of milliseconds before the
     *                                  pool initialization fails, or 0 to validate connection setup but continue with
     *                                  pool start, or less than zero to skip all initialization checks and start the
     *                                  pool without delay.
     */
    public void setInitializationFailTimeout(long initializationFailTimeout) {
        this.initializationFailTimeout = initializationFailTimeout;
    }

    /**
     * Determine whether internal pool queries, principally aliveness checks, will be isolated in their own transaction
     * via {@link Connection#rollback()}.  Defaults to {@code false}.
     *
     * @return {@code true} if internal pool queries are isolated, {@code false} if not
     */
    public boolean isIsolateInternalQueries() {
        return isIsolateInternalQueries;
    }

    /**
     * Configure whether internal pool queries, principally aliveness checks, will be isolated in their own transaction
     * via {@link Connection#rollback()}.  Defaults to {@code false}.
     *
     * @param isolate {@code true} if internal pool queries should be isolated, {@code false} if not
     */
    public void setIsolateInternalQueries(boolean isolate) {
        this.isIsolateInternalQueries = isolate;
    }

    /**
     * Determine whether the Connections in the pool are in read-only mode.
     *
     * @return {@code true} if the Connections in the pool are read-only, {@code false} if not
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * Configures the Connections to be added to the pool as read-only Connections.
     *
     * @param readOnly {@code true} if the Connections in the pool are read-only, {@code false} if not
     */
    public void setReadOnly(boolean readOnly) {
        this.isReadOnly = readOnly;
    }

    public String getPoolName() {
        return poolName;
    }

    /**
     * Set the name of the connection pool.  This is primarily used for the MBean
     * to uniquely identify the pool configuration.
     *
     * @param poolName the name of the connection pool to use
     */
    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    /**
     * Get the ScheduledExecutorService used for housekeeping.
     *
     * @return the executor
     */
    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    /**
     * Set the ScheduledExecutorService used for housekeeping.
     *
     * @param executor the ScheduledExecutorService
     */
    public void setScheduledExecutor(ScheduledExecutorService executor) {
        this.scheduledExecutor = executor;
    }

    public String getTransactionIsolation() {
        return transactionIsolationName;
    }

    /**
     * Get the default schema name to be set on connections.
     *
     * @return the default schema name
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Set the default schema name to be set on connections.
     *
     * @param schema the name of the default schema
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Set the default transaction isolation level.  The specified value is the
     * constant name from the <code>Connection</code> class, eg.
     * <code>TRANSACTION_REPEATABLE_READ</code>.
     *
     * @param isolationLevel the name of the isolation level
     */
    public void setTransactionIsolation(String isolationLevel) {
        this.transactionIsolationName = isolationLevel;
    }

    public void setAliveBypassWindowMs(long aliveBypassWindowMs) {
        this.aliveBypassWindowMs = aliveBypassWindowMs;
    }

    public long getAliveBypassWindowMs() {
        return aliveBypassWindowMs;
    }

    public void setHousekeepingPeriodMs(long housekeepingPeriodMs) {
        this.housekeepingPeriodMs = housekeepingPeriodMs;
    }

    public long getHousekeepingPeriodMs() {
        return housekeepingPeriodMs;
    }

    /**
     * Get the thread factory used to create threads.
     *
     * @return the thread factory (may be null, in which case the default thread factory is used)
     */
    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    /**
     * Set the thread factory to be used to create threads.
     *
     * @param threadFactory the thread factory (setting to null causes the default thread factory to be used)
     */
    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    private Class<?> attemptFromContextLoader(final String driverClassName) {
        final ClassLoader threadContextClassLoader = Thread.currentThread().getContextClassLoader();
        if (threadContextClassLoader != null) {
            try {
                final Class<?> driverClass = threadContextClassLoader.loadClass(driverClassName);
                logger.log(Level.FINE, "Driver class found in Thread context class loader:" +
                        driverClassName + " " + threadContextClassLoader);
                return driverClass;
            } catch (ClassNotFoundException e) {
                logger.log(Level.FINE, "Driver class not found in Thread context class loader, trying classloader: " +
                        driverClassName + " " + threadContextClassLoader + " " + this.getClass().getClassLoader());
            }
        }

        return null;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void validate() {
        if (poolName == null) {
            poolName = generatePoolName();
        }
        catalog = getNullIfEmpty(catalog);
        connectionInitSql = getNullIfEmpty(connectionInitSql);
        connectionTestQuery = getNullIfEmpty(connectionTestQuery);
        transactionIsolationName = getNullIfEmpty(transactionIsolationName);
        dataSourceClassName = getNullIfEmpty(dataSourceClassName);
        driverClassName = getNullIfEmpty(driverClassName);
        jdbcUrl = getNullIfEmpty(jdbcUrl);
        if (dataSource != null) {
            if (dataSourceClassName != null) {
                logger.log(Level.WARNING, "using dataSource and ignoring dataSourceClassName: " + poolName);
            }
        } else if (dataSourceClassName != null) {
            if (driverClassName != null) {
                logger.log(Level.SEVERE, "cannot use driverClassName and dataSourceClassName together: " + poolName);
                throw new IllegalStateException("cannot use driverClassName and dataSourceClassName together.");
            } else if (jdbcUrl != null) {
                logger.log(Level.WARNING, "using dataSourceClassName and ignoring jdbcUrl: " + poolName);
            }
        } else if (jdbcUrl != null) {
            // ok
        } else if (driverClassName != null) {
            logger.log(Level.SEVERE, "jdbcUrl is required with driverClassName: " + poolName);
            throw new IllegalArgumentException("jdbcUrl is required with driverClassName.");
        } else {
            logger.log(Level.SEVERE, "dataSource or dataSourceClassName or jdbcUrl is required: " + poolName);
            throw new IllegalArgumentException("dataSource or dataSourceClassName or jdbcUrl is required.");
        }
        validateNumerics();
    }

    /**
     * @return null if string is null or empty
     */
    private static String getNullIfEmpty(final String text) {
        return text == null ? null : text.trim().isEmpty() ? null : text.trim();
    }

    private void validateNumerics() {
        if (maxLifetime != 0 && maxLifetime < TimeUnit.SECONDS.toMillis(30)) {
            logger.log(Level.WARNING, "maxLifetime is less than 30000ms, setting to default ms: " +
                    poolName + " " + MAX_LIFETIME);
            maxLifetime = MAX_LIFETIME;
        }
        if (leakDetectionThreshold > 0) {
            if (leakDetectionThreshold < TimeUnit.SECONDS.toMillis(2) || (leakDetectionThreshold > maxLifetime && maxLifetime > 0)) {
                logger.log(Level.WARNING, "leakDetectionThreshold is less than 2000ms or more than maxLifetime, disabling it: " +
                        poolName);
                leakDetectionThreshold = 0;
            }
        }
        if (connectionTimeout < 250) {
            logger.log(Level.WARNING, "connectionTimeout is less than 250ms, setting to ms: " +
                    poolName + " " + CONNECTION_TIMEOUT);
            connectionTimeout = CONNECTION_TIMEOUT;
        }
        if (validationTimeout < 250) {
            logger.log(Level.WARNING, "validationTimeout is less than 250ms, setting to ms" +
                    poolName + " " + VALIDATION_TIMEOUT);
            validationTimeout = VALIDATION_TIMEOUT;
        }
        if (maxPoolSize < 1) {
            maxPoolSize = DEFAULT_POOL_SIZE;
        }
        if (minIdle < 0 || minIdle > maxPoolSize) {
            minIdle = maxPoolSize;
        }
        if (idleTimeout + TimeUnit.SECONDS.toMillis(1) > maxLifetime && maxLifetime > 0 && minIdle < maxPoolSize) {
            logger.log(Level.WARNING, "idleTimeout is close to or more than maxLifetime, disabling it:" + poolName);
            idleTimeout = 0;
        } else if (idleTimeout != 0 && idleTimeout < TimeUnit.SECONDS.toMillis(10) && minIdle < maxPoolSize) {
            logger.log(Level.WARNING, "idleTimeout is less than 10000ms, setting to default ms: " +
                    poolName + " " + IDLE_TIMEOUT);
            idleTimeout = IDLE_TIMEOUT;
        } else if (idleTimeout != IDLE_TIMEOUT && idleTimeout != 0 && minIdle == maxPoolSize) {
            logger.log(Level.WARNING, "idleTimeout has been set but has no effect because the pool is operating as a fixed size pool: " + poolName);
        }
    }

    private static String generatePoolName() {
        return "xbib-pool-jdbc-" + POOL_COUNTER.getAndIncrement();
    }
}
