package org.xbib.io.pool.jdbc.util;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DriverDataSource implements DataSource {

    private static final Logger logger = Logger.getLogger(DriverDataSource.class.getName());

    private static final String PASSWORD = "password";

    private static final String USER = "user";

    private String jdbcUrl;

    private final Properties driverProperties;

    private Driver driver;

    public DriverDataSource(String jdbcUrl, String driverClassName, Properties properties, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.driverProperties = new Properties();
        for (Entry<Object, Object> entry : properties.entrySet()) {
            driverProperties.setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
        if (username != null) {
            setUser(username);
        }
        if (password != null) {
            setPassword(password);
        }
        if (driverClassName != null) {
            Enumeration<Driver> drivers = DriverManager.getDrivers();
            while (drivers.hasMoreElements()) {
                Driver d = drivers.nextElement();
                if (d.getClass().getName().equals(driverClassName)) {
                    driver = d;
                    break;
                }
            }
            if (driver == null) {
                logger.warning("Registered driver with driverClassName was not found, trying direct instantiation: " + driverClassName);
                Class<?> driverClass = null;
                ClassLoader threadContextClassLoader = Thread.currentThread().getContextClassLoader();
                try {
                    if (threadContextClassLoader != null) {
                        try {
                            driverClass = threadContextClassLoader.loadClass(driverClassName);
                            logger.fine("Driver class found in Thread context class loader: " + driverClassName + " " + threadContextClassLoader);
                        } catch (ClassNotFoundException e) {
                            logger.fine("Driver class not found in Thread context class loader, trying classloader: " +
                                    driverClassName + " " + threadContextClassLoader + " " + this.getClass().getClassLoader());
                        }
                    }
                    if (driverClass == null) {
                        driverClass = this.getClass().getClassLoader().loadClass(driverClassName);
                        logger.fine("Driver class found in the PoolConfig class classloader:" + driverClassName + " " + this.getClass().getClassLoader());
                    }
                } catch (ClassNotFoundException e) {
                    logger.fine("Failed to load driver class from PoolConfig class classloader: " + driverClassName + " " + this.getClass().getClassLoader());
                }
                if (driverClass != null) {
                    try {
                        driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
                    } catch (Exception e) {
                        logger.log(Level.WARNING, "Failed to create instance of driver class, trying jdbcUrl resolution: " + driverClassName + " " + e.getMessage(), e);
                    }
                }
            }
        }
        final String sanitizedUrl = jdbcUrl.replaceAll("([?&;]password=)[^&#;]*(.*)", "$1<masked>$2");
        try {
            if (driver == null) {
                driver = DriverManager.getDriver(jdbcUrl);
                logger.fine("Loaded driver with class name for jdbcUrl: " + driver.getClass().getName() + " " + sanitizedUrl);
            } else if (!driver.acceptsURL(jdbcUrl)) {
                throw new RuntimeException("Driver " + driverClassName + " claims to not accept jdbcUrl, " + sanitizedUrl);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get driver instance for jdbcUrl=" + sanitizedUrl, e);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(jdbcUrl, driverProperties);
    }

    public void setUrl(String url) {
        this.jdbcUrl = url;
    }

    public void setUser(String user) {
        driverProperties.put(USER, driverProperties.getProperty("user", user));
    }

    public void setPassword(String password) {
        driverProperties.put(PASSWORD, driverProperties.getProperty("password", password));
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        final Properties cloned = (Properties) driverProperties.clone();
        if (username != null) {
            cloned.put("user", username);
            if (cloned.containsKey("username")) {
                cloned.put("username", username);
            }
        }
        if (password != null) {
            cloned.put("password", password);
        }

        return driver.connect(jdbcUrl, cloned);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLogWriter(PrintWriter logWriter) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
        DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return driver.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
