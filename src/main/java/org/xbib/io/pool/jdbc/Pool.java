package org.xbib.io.pool.jdbc;

import org.xbib.io.pool.jdbc.util.ClockSource;
import org.xbib.io.pool.jdbc.util.Bag;
import org.xbib.io.pool.jdbc.util.DefaultThreadFactory;
import org.xbib.io.pool.jdbc.util.DriverDataSource;
import org.xbib.io.pool.jdbc.util.BagStateListener;
import org.xbib.io.pool.jdbc.util.BagEntry;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * This is the primary connection pool class that provides the basic pooling.
 */
public class Pool implements BagStateListener {

    private static final Logger logger = Logger.getLogger(Pool.class.getName());

    private static final int POOL_NORMAL = 0;

    private static final int POOL_SHUTDOWN = 2;

    private volatile int poolState;

    private static final String EVICTED_CONNECTION_MESSAGE = "(connection was evicted)";

    private static final String DEAD_CONNECTION_MESSAGE = "(connection is dead)";

    private final PoolEntryCreator poolEntryCreator = new PoolEntryCreator(null);

    private final PoolEntryCreator postFillPoolEntryCreator = new PoolEntryCreator("after adding ");

    private final Collection<Runnable> addConnectionQueueReadOnlyView;

    private final ThreadPoolExecutor addConnectionExecutor;

    private final ThreadPoolExecutor closeConnectionExecutor;

    private final Bag<PoolEntry> bag;

    private final ProxyLeakTaskFactory leakTaskFactory;

    private final ScheduledExecutorService houseKeepingExecutorService;

    private ScheduledFuture<?> houseKeeperTask;

    private final PoolConfig config;

    private final String poolName;

    private String catalog;

    private final AtomicReference<Exception> lastConnectionFailure;

    private long connectionTimeout;

    private  long validationTimeout;

    private static final int UNINITIALIZED = -1;

    private static final int TRUE = 1;

    private static final int FALSE = 0;

    private int networkTimeout;

    private int isNetworkTimeoutSupported;

    private int isQueryTimeoutSupported;

    private int defaultTransactionIsolation;

    private int transactionIsolation;

    private DataSource dataSource;

    private final String schema;

    private final boolean isReadOnly;

    private final boolean isAutoCommit;

    private final boolean isUseJdbc4Validation;

    private final boolean isIsolateInternalQueries;

    private volatile boolean isValidChecked;

    /**
     * Construct a {@link Pool} with the specified configuration.
     *
     * @param config the config
     */
    public Pool(PoolConfig config) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        config.validate();
        this.config = config;
        logger.log(Level.INFO, () -> "starting new pool: " + config.getPoolName());
        this.networkTimeout = UNINITIALIZED;
        this.catalog = config.getCatalog();
        this.schema = config.getSchema();
        this.isReadOnly = config.isReadOnly();
        this.isAutoCommit = config.isAutoCommit();
        this.transactionIsolation = getTransactionIsolation(config.getTransactionIsolation());
        this.isQueryTimeoutSupported = UNINITIALIZED;
        this.isNetworkTimeoutSupported = UNINITIALIZED;
        this.isUseJdbc4Validation = config.getConnectionTestQuery() == null;
        this.isIsolateInternalQueries = config.isIsolateInternalQueries();
        this.poolName = config.getPoolName();
        this.connectionTimeout = config.getConnectionTimeout();
        this.validationTimeout = config.getValidationTimeout();
        this.lastConnectionFailure = new AtomicReference<>();
        initializeDataSource();
        this.bag = new Bag<>(this);
        this.houseKeepingExecutorService = initializeHouseKeepingExecutorService();
        long initializationTimeout = config.getInitializationFailTimeout();
        if (initializationTimeout >= 0) {
            checkFailFast(initializationTimeout);
        }
        ThreadFactory threadFactory = config.getThreadFactory();
        int maxPoolSize = config.getMaximumPoolSize();
        LinkedBlockingQueue<Runnable> addConnectionQueue = new LinkedBlockingQueue<>(maxPoolSize);
        this.addConnectionQueueReadOnlyView = Collections.unmodifiableCollection(addConnectionQueue);
        this.addConnectionExecutor = createThreadPoolExecutor(addConnectionQueue, poolName + " connection adder", threadFactory, new ThreadPoolExecutor.DiscardOldestPolicy());
        this.closeConnectionExecutor = createThreadPoolExecutor(maxPoolSize, poolName + " connection closer", threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
        this.leakTaskFactory = new ProxyLeakTaskFactory(config.getLeakDetectionThreshold(), houseKeepingExecutorService);
        this.houseKeeperTask = houseKeepingExecutorService.scheduleWithFixedDelay(new HouseKeeper(), 100L, config.getHousekeepingPeriodMs(), TimeUnit.MILLISECONDS);
        if (Boolean.getBoolean("pool.jdbc.blockUntilFilled") && config.getInitializationFailTimeout() > 1) {
            addConnectionExecutor.setCorePoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));
            addConnectionExecutor.setMaximumPoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));
            final long startTime = ClockSource.currentTime();
            while (ClockSource.elapsedMillis(startTime) < config.getInitializationFailTimeout() && getTotalConnections() < config.getMinimumIdle()) {
                quietlySleep(TimeUnit.MILLISECONDS.toMillis(100));
            }
            addConnectionExecutor.setCorePoolSize(1);
            addConnectionExecutor.setMaximumPoolSize(1);
        }
    }

    public PoolConfig getConfig() {
        return config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return poolName;
    }

    public void quietlyCloseConnection(Connection connection, String closureReason) {
        if (connection != null) {
            try {
                logger.log(Level.FINE, () -> MessageFormat.format("{0} closing connection {1} {2}", poolName, connection, closureReason));
                try (connection) {
                    setNetworkTimeout(connection, TimeUnit.SECONDS.toMillis(15));
                } catch (SQLException e) {
                    // ignore
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "closing connection failed: " + poolName + " " + connection, e);
            }
        }
    }

    public boolean isConnectionAlive(Connection connection) {
        try {
            try {
                setNetworkTimeout(connection, validationTimeout);
                final int validationSeconds = (int) Math.max(1000L, validationTimeout) / 1000;
                if (isUseJdbc4Validation) {
                    return connection.isValid(validationSeconds);
                }
                try (Statement statement = connection.createStatement()) {
                    if (isNetworkTimeoutSupported != TRUE) {
                        setQueryTimeout(statement, validationSeconds);
                    }
                    statement.execute(config.getConnectionTestQuery());
                }
            } finally {
                setNetworkTimeout(connection, networkTimeout);
                if (isIsolateInternalQueries && !isAutoCommit) {
                    connection.rollback();
                }
            }
            return true;
        } catch (Exception e) {
            lastConnectionFailure.set(e);
            logger.log(Level.WARNING, "failed to validate connection, possibly consider using a shorter maxLifetime value: " +
                    poolName + " " + connection + " " + e.getMessage(), e);
            return false;
        }
    }

    public Exception getLastConnectionFailure() {
        return lastConnectionFailure.get();
    }

    public DataSource getUnwrappedDataSource() {
        return dataSource;
    }

    public PoolEntry newPoolEntry() throws Exception {
        return new PoolEntry(newConnection(), this, isReadOnly, isAutoCommit);
    }

    public long getLoginTimeout() {
        try {
            return (dataSource != null) ? dataSource.getLoginTimeout() : TimeUnit.SECONDS.toSeconds(5);
        } catch (SQLException e) {
            return TimeUnit.SECONDS.toSeconds(5);
        }
    }

    /**
     * Get a connection from the pool, or timeout after connectionTimeout milliseconds.
     *
     * @return a java.sql.Connection instance
     * @throws SQLException thrown if a timeout occurs trying to obtain a connection
     */
    public Connection getConnection() throws SQLException {
        return getConnection(connectionTimeout);
    }

    /**
     * Get a connection from the pool, or timeout after the specified number of milliseconds.
     *
     * @param hardTimeout the maximum time to wait for a connection from the pool
     * @return a java.sql.Connection instance
     * @throws SQLException thrown if a timeout occurs trying to obtain a connection
     */
    public Connection getConnection(long hardTimeout) throws SQLException {
        long startTime = ClockSource.currentTime();
        try {
            long timeout = hardTimeout;
            do {
                PoolEntry poolEntry = bag.borrow(timeout, TimeUnit.MILLISECONDS);
                if (poolEntry == null) {
                    break;
                }
                long now = ClockSource.currentTime();
                if (poolEntry.isMarkedEvicted() ||
                        (ClockSource.elapsedMillis(poolEntry.getLastAccessed(), now) > config.getAliveBypassWindowMs() &&
                        !isConnectionAlive(poolEntry.getConnection()))) {
                    closeConnection(poolEntry, poolEntry.isMarkedEvicted() ? EVICTED_CONNECTION_MESSAGE : DEAD_CONNECTION_MESSAGE);
                    timeout = hardTimeout - ClockSource.elapsedMillis(startTime);
                } else {
                    return poolEntry.createProxyConnection(leakTaskFactory.schedule(poolEntry), now);
                }
            } while (timeout > 0L);
            throw createTimeoutException(startTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException(poolName + " - Interrupted during connection acquisition", e);
        }
    }

    /**
     * Shutdown the pool, closing all idle connections and aborting or closing
     * active connections.
     *
     * @throws InterruptedException thrown if the thread is interrupted during shutdown
     */
    public synchronized void shutdown() throws InterruptedException {
        try {
            poolState = POOL_SHUTDOWN;
            if (addConnectionExecutor == null) {
                return;
            }
            logPoolState("before shutdown");
            if (houseKeeperTask != null) {
                houseKeeperTask.cancel(false);
                houseKeeperTask = null;
            }
            softEvictConnections();
            addConnectionExecutor.shutdown();
            addConnectionExecutor.awaitTermination(getLoginTimeout(), TimeUnit.SECONDS);
            destroyHouseKeepingExecutorService();
            bag.close();
            final ExecutorService assassinExecutor = createThreadPoolExecutor(config.getMaximumPoolSize(), poolName + " connection assassinator",
                    config.getThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
            try {
                final long start = ClockSource.currentTime();
                do {
                    abortActiveConnections(assassinExecutor);
                    softEvictConnections();
                } while (getTotalConnections() > 0 && ClockSource.elapsedMillis(start) < TimeUnit.SECONDS.toMillis(10));
            } finally {
                assassinExecutor.shutdown();
                assassinExecutor.awaitTermination(10L, TimeUnit.SECONDS);
            }
            closeConnectionExecutor.shutdown();
            closeConnectionExecutor.awaitTermination(10L, TimeUnit.SECONDS);
        } finally {
            logPoolState("after shutdown");
        }
    }

    /**
     * Evict a Connection from the pool.
     *
     * @param connection the Connection to evict (actually a {@link ProxyConnection})
     */
    public void evictConnection(Connection connection) {
        ProxyConnection proxyConnection = (ProxyConnection) connection;
        proxyConnection.cancelLeakTask();
        try {
            softEvictConnection(proxyConnection.getPoolEntry(), "(connection evicted by user)", !connection.isClosed());
        } catch (SQLException e) {
            // unreachable
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBagItem(final int waiting) {
        final boolean shouldAdd = waiting - addConnectionQueueReadOnlyView.size() >= 0;
        if (shouldAdd) {
            addConnectionExecutor.submit(poolEntryCreator);
        } else {
            logger.log(Level.FINE, () -> "add connection elided, waiting, queue: " +
                    poolName + " " + waiting + " " + addConnectionQueueReadOnlyView.size());
        }
    }

    public int getActiveConnections() {
        return bag.getCount(BagEntry.STATE_IN_USE);
    }

    public int getIdleConnections() {
        return bag.getCount(BagEntry.STATE_NOT_IN_USE);
    }

    public int getTotalConnections() {
        return bag.size();
    }

    public int getThreadsAwaitingConnection() {
        return bag.getWaitingThreadCount();
    }

    public void softEvictConnections() {
        bag.values().forEach(poolEntry -> softEvictConnection(poolEntry, "(connection evicted)", false));
    }

    /**
     * Log the current pool state.
     *
     * @param prefix an optional prefix to prepend the log message
     */
    public void logPoolState(String... prefix) {
        logger.log(Level.FINE, () -> MessageFormat.format("{0} {1} stats: total={2} active={3} idle={4} waiting={5}",
                poolName, (prefix.length > 0 ? prefix[0] : ""),
                getTotalConnections(), getActiveConnections(), getIdleConnections(), getThreadsAwaitingConnection()));
    }

    /**
     * Recycle PoolEntry (add back to the pool)
     *
     * @param poolEntry the PoolEntry to recycle
     */
    public void recycle(final PoolEntry poolEntry) {
        bag.requite(poolEntry);
    }

    /**
     * Permanently close the real (underlying) connection (eat any exception).
     *
     * @param poolEntry     poolEntry having the connection to close
     * @param closureReason reason to close
     */
    public void closeConnection(PoolEntry poolEntry, String closureReason) {
        if (bag.remove(poolEntry)) {
            final Connection connection = poolEntry.close();
            closeConnectionExecutor.execute(() -> {
                quietlyCloseConnection(connection, closureReason);
                if (poolState == POOL_NORMAL) {
                    fillPool();
                }
            });
        }
    }

    private void initializeDataSource()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String jdbcUrl = config.getProperties().getProperty("url");
        String dsClassName = config.getDataSourceClassName();
        DataSource ds = config.getDataSource();
        if (ds == null) {
            if (dsClassName != null) {
                Class<?> clazz = Class.forName(dsClassName, true, ClassLoader.getSystemClassLoader());
                ds = (DataSource) clazz.getDeclaredConstructor().newInstance();
            } else if (jdbcUrl != null) {
                ds = new DriverDataSource(jdbcUrl, config.getDriverClassName(), config.getProperties(), config.getUsername(), config.getPassword());
            }
        }
        logger.log(Level.INFO, () -> "got data source, setting props = " + config.getProperties());
        setTargetFromProperties(ds, config.getProperties());
        setLoginTimeout(ds);
        this.dataSource = ds;
    }

    private Connection newConnection() throws Exception {
        Connection connection = null;
        try {
            String username = config.getUsername();
            String password = config.getPassword();
            connection = username == null ?
                    dataSource.getConnection() : dataSource.getConnection(username, password);
            if (connection == null) {
                throw new SQLTransientConnectionException("dataSource returned null unexpectedly");
            }
            setupConnection(connection);
            lastConnectionFailure.set(null);
            return connection;
        } catch (Exception e) {
            if (connection != null) {
                quietlyCloseConnection(connection, "(failed to create/setup connection)");
            } else if (getLastConnectionFailure() == null) {
                logger.log(Level.FINE, () -> "failed to create/setup connection:" +
                        poolName + " " + e.getMessage());
            }
            lastConnectionFailure.set(e);
            throw e;
        }
    }

    /**
     * Setup a connection initial state.
     *
     * @param connection a Connection
     * @throws PoolEntryException thrown if any exception is encountered
     */
    private void setupConnection(final Connection connection) throws PoolEntryException {
        try {
            if (networkTimeout == UNINITIALIZED) {
                networkTimeout = getAndSetNetworkTimeout(connection, validationTimeout);
            } else {
                setNetworkTimeout(connection, validationTimeout);
            }
            if (connection.isReadOnly() != isReadOnly) {
                connection.setReadOnly(isReadOnly);
            }
            if (connection.getAutoCommit() != isAutoCommit) {
                connection.setAutoCommit(isAutoCommit);
            }
            checkDriverSupport(connection);
            if (transactionIsolation != defaultTransactionIsolation) {
                connection.setTransactionIsolation(transactionIsolation);
            }
            if (catalog != null) {
                connection.setCatalog(catalog);
            }
            if (schema != null) {
                connection.setSchema(schema);
            }
            executeSql(connection, config.getConnectionInitSql(), true);
            setNetworkTimeout(connection, networkTimeout);
        } catch (SQLException e) {
            throw new PoolEntryException(e);
        }
    }

    /**
     * Execute isValid() or connection test query.
     *
     * @param connection a Connection to check
     */
    private void checkDriverSupport(final Connection connection) throws SQLException {
        if (!isValidChecked) {
            checkValidationSupport(connection);
            checkDefaultIsolation(connection);
            isValidChecked = true;
        }
    }

    /**
     * Check whether Connection.isValid() is supported, or that the user has test query configured.
     *
     * @param connection a Connection to check
     * @throws SQLException rethrown from the driver
     */
    private void checkValidationSupport(final Connection connection) throws SQLException {
        try {
            if (isUseJdbc4Validation) {
                connection.isValid(1);
            } else {
                executeSql(connection, config.getConnectionTestQuery(), false);
            }
        } catch (Exception | AbstractMethodError e) {
            logger.log(Level.SEVERE, () -> "failed to execute connection test query: " +
                    poolName + " " + (isUseJdbc4Validation ? " isValid() for connection, configure" : "") + " " + e.getMessage());
            throw e;
        }
    }

    /**
     * Check the default transaction isolation of the Connection.
     *
     * @param connection a Connection to check
     * @throws SQLException rethrown from the driver
     */
    private void checkDefaultIsolation(final Connection connection) throws SQLException {
        try {
            defaultTransactionIsolation = connection.getTransactionIsolation();
            if (transactionIsolation == -1) {
                transactionIsolation = defaultTransactionIsolation;
            }
        } catch (SQLException e) {
            logger.log(Level.WARNING, () -> "default transaction isolation level detection failed: " +
                    poolName + " " + e.getMessage());
            if (e.getSQLState() != null && !e.getSQLState().startsWith("08")) {
                throw e;
            }
        }
    }

    /**
     * Set the query timeout, if it is supported by the driver.
     *
     * @param statement  a statement to set the query timeout on
     * @param timeoutSec the number of seconds before timeout
     */
    private void setQueryTimeout(final Statement statement, final int timeoutSec) {
        if (isQueryTimeoutSupported != FALSE) {
            try {
                statement.setQueryTimeout(timeoutSec);
                isQueryTimeoutSupported = TRUE;
            } catch (Exception e) {
                if (isQueryTimeoutSupported == UNINITIALIZED) {
                    isQueryTimeoutSupported = FALSE;
                    logger.log(Level.INFO, () -> "failed to set query timeout for statement: " +
                            poolName + " " + e.getMessage());
                }
            }
        }
    }

    /**
     * Set the network timeout.  Return the pre-existing value of the network timeout.
     *
     * @param connection the connection to set the network timeout on
     * @param timeoutMs  the number of milliseconds before timeout
     * @return the pre-existing network timeout value
     */
    private int getAndSetNetworkTimeout(final Connection connection, final long timeoutMs) {
        if (isNetworkTimeoutSupported != FALSE) {
            try {
                final int originalTimeout = connection.getNetworkTimeout();
                connection.setNetworkTimeout(Runnable::run, (int) timeoutMs);
                isNetworkTimeoutSupported = TRUE;
                return originalTimeout;
            } catch (Exception | AbstractMethodError e) {
                if (isNetworkTimeoutSupported == UNINITIALIZED) {
                    isNetworkTimeoutSupported = FALSE;
                    logger.log(Level.INFO, () -> "driver does not support get/set network timeout for connections: " +
                            poolName + " " + e.getMessage());
                    if (validationTimeout < TimeUnit.SECONDS.toMillis(1)) {
                        logger.log(Level.WARNING, () -> "a validationTimeout of less than 1 second cannot be honored on drivers without setNetworkTimeout() support: " + poolName);
                    } else if (validationTimeout % TimeUnit.SECONDS.toMillis(1) != 0) {
                        logger.log(Level.WARNING, () -> "a validationTimeout with fractional second granularity cannot be honored on drivers without setNetworkTimeout() support: " + poolName);
                    }
                }
            }
        }
        return 0;
    }

    /**
     * Set the network timeout,
     * @param connection the connection to set the network timeout on
     * @param timeoutMs  the number of milliseconds before timeout
     * @throws SQLException throw if the connection.setNetworkTimeout() call throws
     */
    private void setNetworkTimeout(final Connection connection, final long timeoutMs) throws SQLException {
        if (isNetworkTimeoutSupported == TRUE) {
            connection.setNetworkTimeout(Runnable::run, (int) timeoutMs);
        }
    }

    /**
     * Execute the user-specified init SQL.
     *
     * @param connection the connection to initialize
     * @param sql        the SQL to execute
     * @param isCommit   whether to commit the SQL after execution or not
     * @throws SQLException throws if the init SQL execution fails
     */
    private void executeSql(final Connection connection, final String sql, final boolean isCommit) throws SQLException {
        if (sql != null) {
            try (Statement statement = connection.createStatement()) {
                // connection was created a few milliseconds before, so set query timeout is omitted (we assume it will succeed)
                statement.execute(sql);
            }
            if (isIsolateInternalQueries && !isAutoCommit) {
                if (isCommit) {
                    connection.commit();
                } else {
                    connection.rollback();
                }
            }
        }
    }

    /**
     * Set the loginTimeout on the specified DataSource.
     *
     * @param dataSource the DataSource
     */
    private void setLoginTimeout(final DataSource dataSource) {
        if (connectionTimeout != Integer.MAX_VALUE) {
            try {
                dataSource.setLoginTimeout(Math.max(1, (int) TimeUnit.MILLISECONDS.toSeconds(500L + connectionTimeout)));
            } catch (Exception e) {
                logger.log(Level.INFO, () -> "failed to set login timeout for data source: " +
                        poolName + " " + e.getMessage());
            }
        }
    }

    /**
     * Get the int value of a transaction isolation level by name.
     *
     * @param transactionIsolationName the name of the transaction isolation level
     * @return the int value of the isolation level or -1
     */
    private int getTransactionIsolation(String transactionIsolationName) {
        if (transactionIsolationName != null) {
            try {
                // use the english locale to avoid the infamous turkish locale bug
                final String upperCaseIsolationLevelName = transactionIsolationName.toUpperCase(Locale.ENGLISH);
                return IsolationLevel.valueOf(upperCaseIsolationLevelName).getLevelId();
            } catch (IllegalArgumentException e) {
                // legacy support for passing an integer version of the isolation level
                try {
                    final int level = Integer.parseInt(transactionIsolationName);
                    for (IsolationLevel iso : IsolationLevel.values()) {
                        if (iso.getLevelId() == level) {
                            return iso.getLevelId();
                        }
                    }
                    throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName, nfe);
                }
            }
        }
        return -1;
    }

    /**
     * Creating new poolEntry.  If maxLifetime is configured, create a future End-of-life task with 2.5% variance from
     * the maxLifetime time to ensure there is no massive die-off of Connections in the pool.
     */
    private PoolEntry createPoolEntry() {
        try {
            final PoolEntry poolEntry = newPoolEntry();
            final long maxLifetime = config.getMaxLifetime();
            if (maxLifetime > 0) {
                final long variance = maxLifetime > 10_000 ? ThreadLocalRandom.current().nextLong(maxLifetime / 40) : 0;
                final long lifetime = maxLifetime - variance;
                poolEntry.setFutureEol(houseKeepingExecutorService.schedule(() -> {
                    logger.log(Level.FINE, () -> "end-of life check, lifetime = " + lifetime + " variance = " + variance);
                    if (softEvictConnection(poolEntry, "(connection has passed maxLifetime)", false)) {
                        logger.log(Level.FINE, () -> "end-of life check: connection has passed life time");
                        addBagItem(bag.getWaitingThreadCount());
                    }
                }, lifetime, TimeUnit.MILLISECONDS));
            } else {
                logger.log(Level.FINE, () -> "max life time is 0 or less, ignoring");
            }
            return poolEntry;
        } catch (PoolEntryException e) {
            if (poolState == POOL_NORMAL) { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
                logger.log(Level.SEVERE, "error thrown while acquiring connection from data source: " + poolName + " " + e.getCause());
                lastConnectionFailure.set(e);
            }
        } catch (Exception e) {
            if (poolState == POOL_NORMAL) { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
                logger.log(Level.FINE, () -> "can not acquire connection from data source: " + poolName + " " + e.getMessage());
            }
        }
        return null;
    }

    /**
     * Fill pool up from current idle connections (as they are perceived at the point of execution) to minimumIdle connections.
     */
    private synchronized void fillPool() {
        int connectionsToAdd = Math.min(config.getMaximumPoolSize() - getTotalConnections(), config.getMinimumIdle() - getIdleConnections())
                - addConnectionQueueReadOnlyView.size();
        if (connectionsToAdd <= 0) {
            logger.log(Level.FINE, () -> "fill pool skipped, pool is at sufficient level: " + poolName);
        }
        for (int i = 0; i < connectionsToAdd; i++) {
            addConnectionExecutor.submit((i < connectionsToAdd - 1) ? poolEntryCreator : postFillPoolEntryCreator);
        }
    }

    /**
     * Attempt to abort or close active connections.
     *
     * @param assassinExecutor the ExecutorService to pass to Connection.abort()
     */
    private void abortActiveConnections(final ExecutorService assassinExecutor) {
        for (PoolEntry poolEntry : bag.values(BagEntry.STATE_IN_USE)) {
            Connection connection = poolEntry.close();
            try {
                connection.abort(assassinExecutor);
            } catch (Throwable e) {
                quietlyCloseConnection(connection, "(connection aborted during shutdown)");
            } finally {
                bag.remove(poolEntry);
            }
        }
    }

    /**
     * If initializationFailFast is configured, check that we have DB connectivity.
     * @param initializationTimeout initialization timeout
     * @throws PoolInitializationException if fails to create or validate connection
     * @see PoolConfig#setInitializationFailTimeout(long)
     */
    private void checkFailFast(long initializationTimeout) {
        final long startTime = ClockSource.currentTime();
        do {
            PoolEntry poolEntry = createPoolEntry();
            if (poolEntry != null) {
                if (config.getMinimumIdle() >= 0) {
                    bag.add(poolEntry);
                    logger.log(Level.FINE, () -> MessageFormat.format("{0} added connection: {1}",
                            poolName, poolEntry.getConnection()));
                } else {
                    quietlyCloseConnection(poolEntry.close(), "(initialization check complete and minimumIdle is zero)");
                }
                return;
            }
            if (getLastConnectionFailure() instanceof PoolEntryException) {
                throwPoolInitializationException(getLastConnectionFailure().getCause());
            }
            quietlySleep(TimeUnit.SECONDS.toMillis(1));
        } while (ClockSource.elapsedMillis(startTime) < initializationTimeout);
        if (initializationTimeout > 0) {
            throwPoolInitializationException(getLastConnectionFailure());
        }
    }

    /**
     * Log the Throwable that caused pool initialization to fail, and then throw a PoolInitializationException with
     * that cause attached.
     *
     * @param t the Throwable that caused the pool to fail to initialize (possibly null)
     */
    private void throwPoolInitializationException(Throwable t) {
        logger.log(Level.SEVERE, "exception during pool initialization: " + poolName + " " + t.getMessage(), t);
        destroyHouseKeepingExecutorService();
        throw new PoolInitializationException(t);
    }

    /**
     * "Soft" evict a Connection (/PoolEntry) from the pool.  If this method is being called by the user directly
     * through {@link PoolDataSource#evictConnection(Connection)} then {@code owner} is {@code true}.
     * If the caller is the owner, or if the Connection is idle (i.e. can be "reserved" in the {@link Bag}),
     * then we can close the connection immediately.  Otherwise, we leave it "marked" for eviction so that it is evicted
     * the next time someone tries to acquire it from the pool.
     *
     * @param poolEntry the PoolEntry (/Connection) to "soft" evict from the pool
     * @param reason    the reason that the connection is being evicted
     * @param owner     true if the caller is the owner of the connection, false otherwise
     * @return true if the connection was evicted (closed), false if it was merely marked for eviction
     */
    private boolean softEvictConnection(final PoolEntry poolEntry, final String reason, final boolean owner) {
        poolEntry.markEvicted();
        if (owner || bag.reserve(poolEntry)) {
            closeConnection(poolEntry, reason);
            return true;
        }
        return false;
    }

    /**
     * Create/initialize the Housekeeping service {@link ScheduledExecutorService}.  If the user specified an Executor
     * to be used in the {@link PoolConfig}, then we use that.  If no Executor was specified (typical), then create
     * an Executor and configure it.
     *
     * @return either the user specified {@link ScheduledExecutorService}, or the one we created
     */
    private ScheduledExecutorService initializeHouseKeepingExecutorService() {
        if (config.getScheduledExecutor() == null) {
            ThreadFactory threadFactory = Optional.ofNullable(config.getThreadFactory()).orElseGet(() ->
                    new DefaultThreadFactory(poolName + "-housekeeper", true));
            ScheduledThreadPoolExecutor executor =
                    new ScheduledThreadPoolExecutor(1, threadFactory,
                            new ThreadPoolExecutor.DiscardPolicy());
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        } else {
            return config.getScheduledExecutor();
        }
    }

    /**
     * Destroy (/shutdown) the Housekeeping service Executor, if it was the one that we created.
     */
    private void destroyHouseKeepingExecutorService() {
        if (config.getScheduledExecutor() == null) {
            houseKeepingExecutorService.shutdownNow();
        }
    }

    /**
     * Create a timeout exception (specifically, {@link SQLTransientConnectionException}) to be thrown, because a
     * timeout occurred when trying to acquire a Connection from the pool.  If there was an underlying cause for the
     * timeout, e.g. a SQLException thrown by the driver while trying to create a new Connection, then use the
     * SQL State from that exception as our own and additionally set that exception as the "next" SQLException inside
     * of our exception.
     * As a side-effect, log the timeout failure at DEBUG, and record the timeout failure in the metrics tracker.
     *
     * @param startTime the start time (timestamp) of the acquisition attempt
     * @return a SQLException to be thrown from {@link #getConnection()}
     */
    private SQLException createTimeoutException(long startTime) {
        logPoolState("timeout failure");
        String sqlState = null;
        Throwable originalException = getLastConnectionFailure();
        if (originalException instanceof SQLException) {
            sqlState = ((SQLException) originalException).getSQLState();
        }
        SQLException connectionException = new SQLTransientConnectionException(poolName +
                " connection is not available, request timed out after " +
                ClockSource.elapsedMillis(startTime) + " ms", sqlState, originalException);
        if (originalException instanceof SQLException) {
            connectionException.setNextException((SQLException) originalException);
        }
        return connectionException;
    }

    /**
     * Create a ThreadPoolExecutor.
     *
     * @param queueSize     the queue size
     * @param threadName    the thread name
     * @param threadFactory an optional ThreadFactory
     * @param policy        the RejectedExecutionHandler policy
     * @return a ThreadPoolExecutor
     */
    private ThreadPoolExecutor createThreadPoolExecutor(int queueSize,
                                                        String threadName,
                                                        ThreadFactory threadFactory,
                                                        RejectedExecutionHandler policy) {
        if (threadFactory == null) {
            threadFactory = new DefaultThreadFactory(threadName, true);
        }
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                5, TimeUnit.SECONDS, queue, threadFactory, policy);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Create a ThreadPoolExecutor.
     *
     * @param queue         the BlockingQueue to use
     * @param threadName    the thread name
     * @param threadFactory an optional ThreadFactory
     * @param policy        the RejectedExecutionHandler policy
     * @return a ThreadPoolExecutor
     */
    private ThreadPoolExecutor createThreadPoolExecutor(BlockingQueue<Runnable> queue,
                                                        String threadName,
                                                        ThreadFactory threadFactory,
                                                        RejectedExecutionHandler policy) {
        if (threadFactory == null) {
            threadFactory = new DefaultThreadFactory(threadName, true);
        }
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                5, TimeUnit.SECONDS, queue, threadFactory, policy);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Sleep and suppress InterruptedException (but re-signal it).
     *
     * @param millis the number of milliseconds to sleep
     */
    private static void quietlySleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void setTargetFromProperties(Object target, Properties properties) {
        if (target == null || properties == null) {
            return;
        }
        List<Method> methods = Arrays.asList(target.getClass().getMethods());
        properties.forEach((key, value) -> setProperty(target, key.toString(), value, methods));
    }

    private static void setProperty(Object target, String propName, Object propValue, List<Method> methods) {
        String methodName = "set" + propName.substring(0, 1).toUpperCase(Locale.ENGLISH) + propName.substring(1);
        Method writeMethod = methods.stream()
                .filter(m -> m.getName().equals(methodName) && m.getParameterCount() == 1)
                .findFirst().orElse(null);
        if (writeMethod == null) {
            String methodName2 = "set" + propName.toUpperCase(Locale.ENGLISH);
            writeMethod = methods.stream()
                    .filter(m -> m.getName().equals(methodName2) && m.getParameterCount() == 1)
                    .findFirst().orElse(null);
        }
        if (writeMethod == null) {
            logger.log(Level.SEVERE, "property does not exist on target: " + propName + " " + target.getClass());
            throw new RuntimeException(String.format("property %s does not exist on target %s", propName, target.getClass()));
        }
        try {
            Class<?> paramClass = writeMethod.getParameterTypes()[0];
            if (paramClass == int.class) {
                writeMethod.invoke(target, Integer.parseInt(propValue.toString()));
            } else if (paramClass == long.class) {
                writeMethod.invoke(target, Long.parseLong(propValue.toString()));
            } else if (paramClass == boolean.class || paramClass == Boolean.class) {
                writeMethod.invoke(target, Boolean.parseBoolean(propValue.toString()));
            } else if (paramClass == String.class) {
                logger.log(Level.FINE, () ->
                        MessageFormat.format("write method {0} {1}", target, propValue));
                writeMethod.invoke(target, propValue.toString());
            } else {
                try {
                    logger.log(Level.FINE, () ->
                            MessageFormat.format("try to create a new instance of {0}", propValue));
                    writeMethod.invoke(target, Class.forName(propValue.toString()).getDeclaredConstructor().newInstance());
                } catch (InstantiationException | ClassNotFoundException e) {
                    logger.log(Level.FINE, () ->
                            MessageFormat.format("class not found or could not instantiate it (Default constructor): {0}",
                                    propValue));
                    writeMethod.invoke(target, propValue);
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, () ->
                    MessageFormat.format("failed to set property {0} on target {1}", propName, target.getClass()));
            throw new RuntimeException(e);
        }
    }

    /**
     * Creating and adding poolEntries (connections) to the pool.
     */
    private final class PoolEntryCreator implements Callable<Boolean> {

        private final String loggingPrefix;

        private PoolEntryCreator(String loggingPrefix) {
            this.loggingPrefix = loggingPrefix;
        }

        @Override
        public Boolean call() {
            long sleepBackoff = 250L;
            while (poolState == POOL_NORMAL && shouldCreateAnotherConnection()) {
                final PoolEntry poolEntry = createPoolEntry();
                if (poolEntry != null) {
                    bag.add(poolEntry);
                    logger.log(Level.FINE, () -> MessageFormat.format("{0}: added connection {1} ",
                            poolName, poolEntry.getConnection()));
                    if (loggingPrefix != null) {
                        logPoolState(loggingPrefix);
                    }
                    return Boolean.TRUE;
                }
                if (loggingPrefix != null) {
                    logger.log(Level.FINE, () -> "connection add failed, sleeping with backoff" + poolName);
                }
                quietlySleep(sleepBackoff);
                sleepBackoff = Math.min(TimeUnit.SECONDS.toMillis(10), Math.min(connectionTimeout, (long) (sleepBackoff * 1.5)));
            }
            return Boolean.FALSE;
        }

        /**
         * We only create connections if we need another idle connection or have threads still waiting
         * for a new connection.  Otherwise we bail out of the request to create.
         *
         * @return true if we should create a connection, false if the need has disappeared
         */
        private synchronized boolean shouldCreateAnotherConnection() {
            return getTotalConnections() < config.getMaximumPoolSize() &&
                    (bag.getWaitingThreadCount() > 0 || getIdleConnections() < config.getMinimumIdle());
        }
    }

    /**
     * The house keeping task to retire and maintain minimum idle connections.
     */
    private final class HouseKeeper implements Runnable {

        private volatile long previous = ClockSource.plusMillis(ClockSource.currentTime(), -config.getHousekeepingPeriodMs());

        @Override
        public void run() {
            try {
                logger.log(Level.FINE, () -> "housekeeper running");
                connectionTimeout = config.getConnectionTimeout();
                validationTimeout = config.getValidationTimeout();
                leakTaskFactory.updateLeakDetectionThreshold(config.getLeakDetectionThreshold());
                catalog = (config.getCatalog() != null && !config.getCatalog().equals(catalog)) ? config.getCatalog() : catalog;
                final long idleTimeout = config.getIdleTimeout();
                final long now = ClockSource.currentTime();
                // allowing +128ms as per NTP spec
                if (ClockSource.plusMillis(now, 128) < ClockSource.plusMillis(previous, config.getHousekeepingPeriodMs())) {
                    logger.log(Level.WARNING, "retrograde clock change detected (housekeeper delta=), soft-evicting connections from pool: " +
                            poolName + " " + ClockSource.elapsedDisplayString(previous, now));
                    previous = now;
                    softEvictConnections();
                    return;
                } else if (now > ClockSource.plusMillis(previous, (3 * config.getHousekeepingPeriodMs()) / 2)) {
                    logger.log(Level.WARNING, "thread starvation or clock leap detected: " +
                            poolName + "  housekeeper delta=" + ClockSource.elapsedDisplayString(previous, now));
                }
                previous = now;
                if (idleTimeout > 0L && config.getMinimumIdle() < config.getMaximumPoolSize()) {
                    logPoolState("before cleanup");
                    final List<PoolEntry> notInUse = bag.values(BagEntry.STATE_NOT_IN_USE);
                    int toRemove = notInUse.size() - config.getMinimumIdle();
                    for (PoolEntry entry : notInUse) {
                        if (toRemove > 0 && ClockSource.elapsedMillis(entry.getLastAccessed(), now) > idleTimeout && bag.reserve(entry)) {
                            closeConnection(entry, "(connection has passed idleTimeout)");
                            toRemove--;
                        }
                    }
                    logPoolState("after cleanup");
                } else {
                    logPoolState("pool");
                }
                fillPool();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "unexpected exception in housekeeping task: " + e.getMessage(), e);
            }
        }
    }
}
