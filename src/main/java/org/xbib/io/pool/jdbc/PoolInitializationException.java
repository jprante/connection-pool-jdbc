package org.xbib.io.pool.jdbc;

@SuppressWarnings("serial")
public class PoolInitializationException extends RuntimeException {

    /**
     * Construct an exception, possibly wrapping the provided Throwable as the cause.
     *
     * @param t the Throwable to wrap
     */
    public PoolInitializationException(Throwable t) {
        super("Failed to initialize pool: " + t.getMessage(), t);
    }
}
