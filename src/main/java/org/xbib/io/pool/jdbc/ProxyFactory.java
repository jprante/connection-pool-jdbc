package org.xbib.io.pool.jdbc;

import org.xbib.io.pool.jdbc.util.FastList;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * A factory class that produces proxies around instances of the standard JDBC interfaces.
 */
@SuppressWarnings("unused")
public class ProxyFactory {

    private ProxyFactory() {
        // unconstructable
    }

    /**
     * Create a proxy for the specified {@link Connection} instance.
     *
     * @param poolEntry      the PoolEntry holding pool state
     * @param connection     the raw database Connection
     * @param openStatements a reusable list to track open Statement instances
     * @param leakTask       the ProxyLeakTask for this connection
     * @param now            the current timestamp
     * @param isReadOnly     the default readOnly state of the connection
     * @param isAutoCommit   the default autoCommit state of the connection
     * @return a proxy that wraps the specified {@link Connection}
     */
    public static Connection getProxyConnection(PoolEntry poolEntry,
                                                     Connection connection,
                                                     FastList<Statement> openStatements,
                                                     ProxyLeakTask leakTask,
                                                     long now,
                                                     boolean isReadOnly,
                                                     boolean isAutoCommit) {
        return new ProxyConnection(poolEntry, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
    }

    public static Statement getProxyStatement(ProxyConnection connection,
                                              Statement statement) {
        return new ProxyStatement(connection, statement);
    }

    public static CallableStatement getProxyCallableStatement(ProxyConnection connection,
                                                              CallableStatement statement) {
        return new ProxyCallableStatement(connection, statement);
    }

    public static PreparedStatement getProxyPreparedStatement(ProxyConnection connection,
                                                              PreparedStatement statement) {
        return new ProxyPreparedStatement(connection, statement);
    }

    public static ResultSet getProxyResultSet(ProxyConnection connection,
                                              ProxyStatement statement,
                                              ResultSet resultSet) {
        return new ProxyResultSet(connection, statement, resultSet);
    }

    public static DatabaseMetaData getProxyDatabaseMetaData(ProxyConnection connection,
                                                            DatabaseMetaData metaData) {
        return new ProxyDatabaseMetaData(connection, metaData);
    }
}
