package org.xbib.io.pool.jdbc.mock;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class StubDriver implements Driver {
    private static final Driver driver;

    static {
        driver = new StubDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new StubConnection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return "jdbc:stub".equals(url);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMajorVersion() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMinorVersion() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
