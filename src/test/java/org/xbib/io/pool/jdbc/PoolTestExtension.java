package org.xbib.io.pool.jdbc;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import java.util.Locale;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class PoolTestExtension implements BeforeAllCallback {

    @Override
    public void beforeAll(ExtensionContext context) {
        //Level level = Level.INFO;
        Level level = Level.ALL;
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] %5$s %6$s%n");
        LogManager.getLogManager().reset();
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        Handler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        rootLogger.addHandler(handler);
        rootLogger.setLevel(level);
        for (Handler h : rootLogger.getHandlers()) {
            handler.setFormatter(new SimpleFormatter());
            h.setLevel(level);
        }
    }

    public static void quietlySleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Get the int value of a transaction isolation level by name.
     *
     * @param transactionIsolationName the name of the transaction isolation level
     * @return the int value of the isolation level or -1
     */
    public static int getTransactionIsolation(final String transactionIsolationName) {
        if (transactionIsolationName != null) {
            try {
                final String upperCaseIsolationLevelName = transactionIsolationName.toUpperCase(Locale.ENGLISH);
                return IsolationLevel.valueOf(upperCaseIsolationLevelName).getLevelId();
            } catch (IllegalArgumentException e) {
                try {
                    final int level = Integer.parseInt(transactionIsolationName);
                    for (IsolationLevel iso : IsolationLevel.values()) {
                        if (iso.getLevelId() == level) {
                            return iso.getLevelId();
                        }
                    }
                    throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName);
                }
                catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName, nfe);
                }
            }
        }
        return -1;
    }
}
