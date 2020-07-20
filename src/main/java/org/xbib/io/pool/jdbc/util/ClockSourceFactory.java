package org.xbib.io.pool.jdbc.util;

/**
 * Factory class used to create a platform-specific ClockSource.
 */
public class ClockSourceFactory {
    public static ClockSource create() {
        return new NanosecondClockSource();
    }
}
