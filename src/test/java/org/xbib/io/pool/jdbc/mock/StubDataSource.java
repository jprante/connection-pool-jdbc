package org.xbib.io.pool.jdbc.mock;

import org.xbib.io.pool.jdbc.PoolTestExtension;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class StubDataSource implements DataSource {
    private String user;
    private String password;
    private PrintWriter logWriter;
    private SQLException throwException;
    private long connectionAcquistionTime = 0;
    private int loginTimeout;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setURL(String url) {
        // we don't care
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    /**
     * {@inheritDoc}
     */
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        throw new SQLException("wrapped DataSource is not an instance of " + iface);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        if (throwException != null) {
            throw throwException;
        }
        if (connectionAcquistionTime > 0) {
            PoolTestExtension.quietlySleep(connectionAcquistionTime);
        }
        return new StubConnection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return new StubConnection();
    }

    public void setThrowException(SQLException e) {
        this.throwException = e;
    }

    public void setConnectionAcquistionTime(long connectionAcquisitionTime) {
        this.connectionAcquistionTime = connectionAcquisitionTime;
    }
}
