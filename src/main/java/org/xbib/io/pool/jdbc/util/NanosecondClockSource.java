package org.xbib.io.pool.jdbc.util;

import java.util.concurrent.TimeUnit;

public class NanosecondClockSource implements ClockSource {
    /**
     * {@inheritDoc}
     */
    @Override
    public long currentTime0() {
        return System.nanoTime();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long toMillis0(final long time) {
        return TimeUnit.NANOSECONDS.toMillis(time);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long toNanos0(final long time) {
        return time;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long elapsedMillis0(final long startTime) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long elapsedMillis0(final long startTime, final long endTime) {
        return TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long elapsedNanos0(final long startTime) {
        return System.nanoTime() - startTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long elapsedNanos0(final long startTime, final long endTime) {
        return endTime - startTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long plusMillis0(final long time, final long millis) {
        return time + TimeUnit.MILLISECONDS.toNanos(millis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeUnit getSourceTimeUnit0() {
        return TimeUnit.NANOSECONDS;
    }
}
