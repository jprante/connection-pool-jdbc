package org.xbib.io.pool.jdbc;

import org.xbib.io.pool.jdbc.util.ClockSource;
import org.xbib.io.pool.jdbc.util.FastList;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the proxy class for java.sql.Connection.
 */
public class ProxyConnection implements Connection {

    private static final Logger logger = Logger.getLogger(ProxyConnection.class.getName());

    private static final int DIRTY_BIT_READONLY = 0b000001;

    private static final int DIRTY_BIT_AUTOCOMMIT = 0b000010;

    private static final int DIRTY_BIT_ISOLATION = 0b000100;

    private static final int DIRTY_BIT_CATALOG = 0b001000;

    private static final int DIRTY_BIT_NETTIMEOUT = 0b010000;

    private static final int DIRTY_BIT_SCHEMA = 0b100000;

    private static final Set<String> ERROR_STATES;

    private static final Set<Integer> ERROR_CODES;

    private Connection delegate;

    private final PoolEntry poolEntry;

    private final ProxyLeakTask leakTask;

    private final FastList<Statement> openStatements;

    private int dirtyBits;

    private long lastAccess;

    private boolean isCommitStateDirty;

    private boolean isReadOnly;

    private boolean isAutoCommit;

    private int networkTimeout;

    private int transactionIsolation;

    private String dbcatalog;

    private String dbschema;

    static {
        ERROR_STATES = new HashSet<>();
        ERROR_STATES.add("0A000"); // FEATURE UNSUPPORTED
        ERROR_STATES.add("57P01"); // ADMIN SHUTDOWN
        ERROR_STATES.add("57P02"); // CRASH SHUTDOWN
        ERROR_STATES.add("57P03"); // CANNOT CONNECT NOW
        ERROR_STATES.add("01002"); // SQL92 disconnect error
        ERROR_STATES.add("JZ0C0"); // Sybase disconnect error
        ERROR_STATES.add("JZ0C1"); // Sybase disconnect error

        ERROR_CODES = new HashSet<>();
        ERROR_CODES.add(500150);
        ERROR_CODES.add(2399);
    }

    public ProxyConnection(PoolEntry poolEntry,
                           Connection connection,
                           FastList<Statement> openStatements,
                           ProxyLeakTask leakTask,
                           long now,
                           boolean isReadOnly,
                           boolean isAutoCommit) {
        this.poolEntry = poolEntry;
        this.delegate = connection;
        this.openStatements = openStatements;
        this.leakTask = leakTask;
        this.lastAccess = now;
        this.isReadOnly = isReadOnly;
        this.isAutoCommit = isAutoCommit;
    }

    public boolean isCommitStateDirty() {
        return isCommitStateDirty;
    }

    /**
     *
     * {@inheritDoc}
     */
    @Override
    public final String toString() {
        return getClass().getSimpleName() + '@' + System.identityHashCode(this) + " wrapping " + delegate;
    }

    public boolean getAutoCommitState() {
        return isAutoCommit;
    }

    public String getCatalogState() {
        return dbcatalog;
    }

    public String getSchemaState() {
        return dbschema;
    }

    public int getTransactionIsolationState() {
        return transactionIsolation;
    }

    public boolean getReadOnlyState() {
        return isReadOnly;
    }

    public int getNetworkTimeoutState() {
        return networkTimeout;
    }

    public PoolEntry getPoolEntry() {
        return poolEntry;
    }

    public SQLException checkException(SQLException sqle) {
        boolean evict = false;
        SQLException nse = sqle;
        for (int depth = 0; delegate != CLOSED_CONNECTION && nse != null && depth < 10; depth++) {
            final String sqlState = nse.getSQLState();
            if (sqlState != null && sqlState.startsWith("08")
                    || nse instanceof SQLTimeoutException
                    || ERROR_STATES.contains(sqlState)
                    || ERROR_CODES.contains(nse.getErrorCode())) {
                // broken connection
                evict = true;
                break;
            } else {
                nse = nse.getNextException();
            }
        }
        if (evict) {
            logger.log(Level.WARNING, "Connection marked as broken because of SQLSTATE(), ErrorCode(): " +
                    poolEntry.getPoolName() + " " + delegate + " " + nse.getSQLState() + " " + nse.getErrorCode(), nse);
            leakTask.cancel();
            poolEntry.evict("(connection is broken)");
            delegate = CLOSED_CONNECTION;
        }
        return sqle;
    }

    public synchronized void untrackStatement(final Statement statement) {
        openStatements.remove(statement);
    }

    public void markCommitStateDirty() {
        if (isAutoCommit) {
            lastAccess = ClockSource.currentTime();
        } else {
            isCommitStateDirty = true;
        }
    }

    public void cancelLeakTask() {
        leakTask.cancel();
    }

    private synchronized <T extends Statement> T trackStatement(final T statement) {
        openStatements.add(statement);
        return statement;
    }

    @SuppressWarnings("try")
    private synchronized void closeStatements() {
        final int size = openStatements.size();
        if (size > 0) {
            for (int i = 0; i < size && delegate != CLOSED_CONNECTION; i++) {
                try (Statement ignored = openStatements.get(i)) {
                    // automatic resource cleanup
                } catch (SQLException e) {
                    logger.log(Level.WARNING, "Connection marked as broken because of an exception closing open statements during Connection.close(): " +
                            poolEntry.getPoolName() + " " + delegate);
                    leakTask.cancel();
                    poolEntry.evict("(exception closing Statements during Connection.close())");
                    delegate = CLOSED_CONNECTION;
                }
            }
            openStatements.clear();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws SQLException {
        closeStatements();
        if (delegate != CLOSED_CONNECTION) {
            leakTask.cancel();
            try {
                if (isCommitStateDirty && !isAutoCommit) {
                    delegate.rollback();
                    lastAccess = ClockSource.currentTime();
                    logger.log(Level.FINE, "Executed rollback on connection due to dirty commit state on close(): " + poolEntry.getPoolName() + " " + delegate);
                }
                if (dirtyBits != 0) {
                    //poolEntry.resetConnectionState(this, dirtyBits);
                    resetConnectionState(poolEntry.getConnection(), dirtyBits,
                            poolEntry.getPool().getConfig().getCatalog() ,
                            poolEntry.getPool().getConfig().getSchema());
                    lastAccess = ClockSource.currentTime();
                }
                delegate.clearWarnings();
            } catch (SQLException e) {
                // when connections are aborted, exceptions are often thrown that should not reach the application
                if (!poolEntry.isMarkedEvicted()) {
                    throw checkException(e);
                }
            } finally {
                delegate = CLOSED_CONNECTION;
                poolEntry.recycle(lastAccess);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() throws SQLException {
        return delegate == CLOSED_CONNECTION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement() throws SQLException {
        return ProxyFactory.getProxyStatement(this, trackStatement(delegate.createStatement()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement(int resultSetType, int concurrency) throws SQLException {
        return ProxyFactory.getProxyStatement(this, trackStatement(delegate.createStatement(resultSetType, concurrency)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement(int resultSetType, int concurrency, int holdability) throws SQLException {
        return ProxyFactory.getProxyStatement(this, trackStatement(delegate.createStatement(resultSetType, concurrency, holdability)));
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return ProxyFactory.getProxyCallableStatement(this, trackStatement(delegate.prepareCall(sql)));
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return delegate.nativeSQL(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int concurrency) throws SQLException {
        return ProxyFactory.getProxyCallableStatement(this, trackStatement(delegate.prepareCall(sql, resultSetType, concurrency)));
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return delegate.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        delegate.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        delegate.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return delegate.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return delegate.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return delegate.setSavepoint(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int concurrency, int holdability) throws SQLException {
        return ProxyFactory.getProxyCallableStatement(this, trackStatement(delegate.prepareCall(sql, resultSetType, concurrency, holdability)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, autoGeneratedKeys)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, resultSetType, concurrency)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency, int holdability) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, resultSetType, concurrency, holdability)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, columnIndexes)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this, trackStatement(delegate.prepareStatement(sql, columnNames)));
    }

    @Override
    public Clob createClob() throws SQLException {
        return delegate.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return delegate.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return delegate.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return delegate.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return delegate.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        delegate.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        delegate.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return delegate.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return delegate.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return delegate.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return delegate.createStruct(typeName, attributes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        markCommitStateDirty();
        return ProxyFactory.getProxyDatabaseMetaData(this, delegate.getMetaData());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() throws SQLException {
        delegate.commit();
        isCommitStateDirty = false;
        lastAccess = ClockSource.currentTime();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback() throws SQLException {
        delegate.rollback();
        isCommitStateDirty = false;
        lastAccess = ClockSource.currentTime();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        delegate.rollback(savepoint);
        isCommitStateDirty = false;
        lastAccess = ClockSource.currentTime();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        delegate.releaseSavepoint(savepoint);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        delegate.setAutoCommit(autoCommit);
        isAutoCommit = autoCommit;
        dirtyBits |= DIRTY_BIT_AUTOCOMMIT;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return delegate.getAutoCommit();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        delegate.setReadOnly(readOnly);
        isReadOnly = readOnly;
        isCommitStateDirty = false;
        dirtyBits |= DIRTY_BIT_READONLY;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return delegate.isReadOnly();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        delegate.setTransactionIsolation(level);
        transactionIsolation = level;
        dirtyBits |= DIRTY_BIT_ISOLATION;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return delegate.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        delegate.setCatalog(catalog);
        dbcatalog = catalog;
        dirtyBits |= DIRTY_BIT_CATALOG;
    }

    @Override
    public String getCatalog() throws SQLException {
        return delegate.getCatalog();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        delegate.setNetworkTimeout(executor, milliseconds);
        networkTimeout = milliseconds;
        dirtyBits |= DIRTY_BIT_NETTIMEOUT;
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return delegate.getNetworkTimeout();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        delegate.setSchema(schema);
        dbschema = schema;
        dirtyBits |= DIRTY_BIT_SCHEMA;
    }

    @Override
    public String getSchema() throws SQLException {
        return delegate.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        delegate.abort(executor);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(delegate) || (delegate != null && delegate.isWrapperFor(iface));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(delegate)) {
            return (T) delegate;
        } else if (delegate != null) {
            return delegate.unwrap(iface);
        }
        throw new SQLException("Wrapped connection is not an instance of " + iface);
    }

    static final Connection CLOSED_CONNECTION = getClosedConnection();

    private static Connection getClosedConnection() {
        InvocationHandler handler = (proxy, method, args) -> {
            final String methodName = method.getName();
            if ("isClosed".equals(methodName)) {
                return Boolean.TRUE;
            } else if ("isValid".equals(methodName)) {
                return Boolean.FALSE;
            }
            if ("abort".equals(methodName)) {
                return Void.TYPE;
            }
            if ("close".equals(methodName)) {
                return Void.TYPE;
            } else if ("toString".equals(methodName)) {
                return ProxyConnection.class.getCanonicalName();
            }
            throw new SQLException("connection is closed");
        };
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] { Connection.class }, handler);
    }

    private void resetConnectionState(Connection connection, int dirtyBits, String catalog, String schema) throws SQLException {
        int resetBits = 0;
        if ((dirtyBits & DIRTY_BIT_READONLY) != 0 && getReadOnlyState() != isReadOnly) {
            connection.setReadOnly(isReadOnly);
            resetBits |= DIRTY_BIT_READONLY;
        }
        if ((dirtyBits & DIRTY_BIT_AUTOCOMMIT) != 0 && getAutoCommitState() != isAutoCommit) {
            connection.setAutoCommit(isAutoCommit);
            resetBits |= DIRTY_BIT_AUTOCOMMIT;
        }
        if ((dirtyBits & DIRTY_BIT_ISOLATION) != 0 && getTransactionIsolationState() != transactionIsolation) {
            connection.setTransactionIsolation(transactionIsolation);
            resetBits |= DIRTY_BIT_ISOLATION;
        }
        if ((dirtyBits & DIRTY_BIT_CATALOG) != 0 && catalog != null && !catalog.equals(getCatalogState())) {
            connection.setCatalog(catalog);
            resetBits |= DIRTY_BIT_CATALOG;
        }
        if ((dirtyBits & DIRTY_BIT_NETTIMEOUT) != 0 && getNetworkTimeoutState() != networkTimeout) {
            connection.setNetworkTimeout(Runnable::run, networkTimeout);
            resetBits |= DIRTY_BIT_NETTIMEOUT;
        }
        if ((dirtyBits & DIRTY_BIT_SCHEMA) != 0 && schema != null && !schema.equals(getSchemaState())) {
            connection.setSchema(schema);
            resetBits |= DIRTY_BIT_SCHEMA;
        }
        if (resetBits != 0 && logger.isLoggable(Level.FINE)) {
            final String string = stringFromResetBits(resetBits);
            logger.log(Level.FINE, () -> "reset on connection: " + string + " " + connection);
        }
    }

    /**
     * This will create a string for debug logging. Given a set of "reset bits", this
     * method will return a concatenated string, for example
     * Input : 0b00110
     * Output: "autoCommit, isolation"
     *
     * @param bits a set of "reset bits"
     * @return a string of which states were reset
     */
    private String stringFromResetBits(final int bits) {
        final StringBuilder sb = new StringBuilder();
        for (int ndx = 0; ndx < RESET_STATES.length; ndx++) {
            if ((bits & (0b1 << ndx)) != 0) {
                sb.append(RESET_STATES[ndx]).append(", ");
            }
        }
        sb.setLength(sb.length() - 2);  // trim trailing comma
        return sb.toString();
    }

    private static final String[] RESET_STATES = {"readOnly", "autoCommit", "isolation", "catalog", "netTimeout", "schema"};

}
