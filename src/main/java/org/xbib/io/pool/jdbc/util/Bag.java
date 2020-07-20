package org.xbib.io.pool.jdbc.util;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to {@link java.util.concurrent.LinkedBlockingQueue} and
 * {@link java.util.concurrent.LinkedTransferQueue} for the purposes of a
 * connection pool.  It uses {@link ThreadLocal} storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the {@link ThreadLocal} list.  Not-in-use items in the
 * {@link ThreadLocal} lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * {@link AbstractQueuedLongSynchronizer} to manage cross-thread signaling.
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * {@link Bag#requite(T)} borrowed objects otherwise a memory leak will result.
 * Only the {@link Bag#remove(T)} method can completely remove an object.
 *
 * @param <T> the templated type to store in the bag
 */
public class Bag<T extends BagEntry> implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(Bag.class.getName());

    private final CopyOnWriteArrayList<T> sharedList;

    private final boolean weakThreadLocals;

    private final ThreadLocal<List<Object>> threadList;

    private final BagStateListener listener;

    private final AtomicInteger waiters;

    private volatile boolean closed;

    private final SynchronousQueue<T> handoffQueue;

    private String lastMessage;

    /**
     * Construct a Bag with the specified listener.
     *
     * @param listener the IBagStateListener to attach to this bag
     */
    public Bag(BagStateListener listener) {
        this.listener = listener;
        this.weakThreadLocals = useWeakThreadLocals();
        this.handoffQueue = new SynchronousQueue<>(true);
        this.waiters = new AtomicInteger();
        this.sharedList = new CopyOnWriteArrayList<>();
        if (weakThreadLocals) {
            this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
        } else {
            this.threadList = ThreadLocal.withInitial(() -> new FastList<>(BagEntry.class, 16));
        }
    }

    public String getLastMessage() {
        return lastMessage;
    }

    /**
     * The method will borrow a BagEntry from the bag, blocking for the
     * specified timeout if none are available.
     *
     * @param timeout  how long to wait before giving up, in units of unit
     * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
     * @return a borrowed instance from the bag or null if a timeout occurs
     * @throws InterruptedException if interrupted while waiting
     */
    public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException {
        // Try the thread-local list first
        final List<Object> list = threadList.get();
        for (int i = list.size() - 1; i >= 0; i--) {
            final Object entry = list.remove(i);
            @SuppressWarnings("unchecked")
            final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
            if (bagEntry != null && bagEntry.compareAndSet(BagEntry.STATE_NOT_IN_USE, BagEntry.STATE_IN_USE)) {
                return bagEntry;
            }
        }
        final int waiting = waiters.incrementAndGet();
        try {
            for (T bagEntry : sharedList) {
                if (bagEntry.compareAndSet(BagEntry.STATE_NOT_IN_USE, BagEntry.STATE_IN_USE)) {
                    if (waiting > 1) {
                        listener.addBagItem(waiting - 1);
                    }
                    return bagEntry;
                }
            }
            listener.addBagItem(waiting);
            timeout = timeUnit.toNanos(timeout);
            do {
                final long start = ClockSource.currentTime();
                final T bagEntry = handoffQueue.poll(timeout, TimeUnit.NANOSECONDS);
                if (bagEntry == null || bagEntry.compareAndSet(BagEntry.STATE_NOT_IN_USE, BagEntry.STATE_IN_USE)) {
                    return bagEntry;
                }

                timeout -= ClockSource.elapsedNanos(start);
            } while (timeout > 10_000);
            return null;
        } finally {
            waiters.decrementAndGet();
        }
    }

    /**
     * This method will return a borrowed object to the bag.  Objects
     * that are borrowed from the bag but never "requited" will result
     * in a memory leak.
     *
     * @param bagEntry the value to return to the bag
     * @throws NullPointerException  if value is null
     * @throws IllegalStateException if the bagEntry was not borrowed from the bag
     */
    public void requite(final T bagEntry) {
        bagEntry.setState(BagEntry.STATE_NOT_IN_USE);
        for (int i = 0; waiters.get() > 0; i++) {
            if (bagEntry.getState() != BagEntry.STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {
                return;
            } else if ((i & 0xff) == 0xff) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(10));
            } else {
                Thread.yield();
            }
        }
        final List<Object> threadLocalList = threadList.get();
        if (threadLocalList.size() < 50) {
            threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
        }
    }

    /**
     * Add a new object to the bag for others to borrow.
     *
     * @param bagEntry an object to add to the bag
     */
    public void add(final T bagEntry) {
        if (closed) {
            lastMessage = "Bag has been closed, ignoring add()";
            logger.info(lastMessage);
            throw new IllegalStateException("Bag has been closed, ignoring add()");
        }
        sharedList.add(bagEntry);
        // spin until a thread takes it or none are waiting
        while (waiters.get() > 0 && bagEntry.getState() == BagEntry.STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) {
            Thread.yield();
        }
    }

    /**
     * Remove a value from the bag.  This method should only be called
     * with objects obtained by {@link Bag#borrow(long, TimeUnit)} or
     * {@link Bag#reserve(T)}.
     *
     * @param bagEntry the value to remove
     * @return true if the entry was removed, false otherwise
     * @throws IllegalStateException if an attempt is made to remove an object
     *                               from the bag that was not borrowed or reserved first
     */
    public boolean remove(final T bagEntry) {
        if (!bagEntry.compareAndSet(BagEntry.STATE_IN_USE, BagEntry.STATE_REMOVED) &&
                !bagEntry.compareAndSet(BagEntry.STATE_RESERVED, BagEntry.STATE_REMOVED) && !closed) {
            lastMessage = "attempt to remove an object from the bag that was not borrowed or reserved: " + bagEntry;
            logger.warning(lastMessage);
            return false;
        }
        boolean removed = sharedList.remove(bagEntry);
        if (!removed && !closed) {
            lastMessage = "attempt to remove an object from the bag that does not exist: " + bagEntry;
            logger.warning(lastMessage);
        }
        threadList.get().remove(bagEntry);
        return removed;
    }

    /**
     * Close the bag to further adds.
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * This method provides a "snapshot" in time of the BagEntry
     * items in the bag in the specified state.  It does not "lock"
     * or reserve items in any way. Call {@link Bag#reserve(T)}
     * on items in list before performing any action on them.
     *
     * @param state one of the {@link BagEntry} states
     * @return a possibly empty list of objects having the state specified
     */
    public List<T> values(int state) {
        final List<T> list = sharedList.stream()
                .filter(e -> e.getState() == state)
                .collect(Collectors.toList());
        Collections.reverse(list);
        return list;
    }

    /**
     * This method provides a "snapshot" in time of the bag items.  It
     * does not "lock" or reserve items in any way.  Call {@link Bag#reserve(T)}
     * on items in the list, or understand the concurrency implications of
     * modifying items, before performing any action on them.
     *
     * @return a possibly empty list of (all) bag items
     */
    @SuppressWarnings("unchecked")
    public List<T> values() {
        return (List<T>) sharedList.clone();
    }

    /**
     * The method is used to make an item in the bag "unavailable" for
     * borrowing.  It is primarily used when wanting to operate on items
     * returned by the {@link Bag#values(int)} method.  Items that are
     * reserved can be removed from the bag via {@link Bag#remove(T)}
     * without the need to unreserve them.  Items that are not removed
     * from the bag can be make available for borrowing again by calling
     * the {@link Bag#unreserve(BagEntry)} method.
     *
     * @param bagEntry the item to reserve
     * @return true if the item was able to be reserved, false otherwise
     */
    public boolean reserve(T bagEntry) {
        return bagEntry.compareAndSet(BagEntry.STATE_NOT_IN_USE, BagEntry.STATE_RESERVED);
    }

    /**
     * This method is used to make an item reserved via {@link Bag#reserve(T)}
     * available again for borrowing.
     *
     * @param bagEntry the item to unreserve
     */
    @SuppressWarnings("SpellCheckingInspection")
    public void unreserve(final T bagEntry) {
        if (bagEntry.compareAndSet(BagEntry.STATE_RESERVED, BagEntry.STATE_NOT_IN_USE)) {
            // spin until a thread takes it or none are waiting
            while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
                Thread.yield();
            }
        } else {
            lastMessage = "attempt to relinquish an object to the bag that was not reserved: " + bagEntry;
            logger.warning(lastMessage);
        }
    }

    /**
     * Get the number of threads pending (waiting) for an item from the
     * bag to become available.
     *
     * @return the number of threads waiting for items from the bag
     */
    public int getWaitingThreadCount() {
        return waiters.get();
    }

    /**
     * Get a count of the number of items in the specified state at the time of this call.
     *
     * @param state the state of the items to count
     * @return a count of how many items in the bag are in the specified state
     */
    public int getCount(final int state) {
        int count = 0;
        for (BagEntry e : sharedList) {
            if (e.getState() == state) {
                count++;
            }
        }
        return count;
    }

    public int[] getStateCounts() {
        final int[] states = new int[6];
        for (BagEntry e : sharedList) {
            ++states[e.getState()];
        }
        states[4] = sharedList.size();
        states[5] = waiters.get();

        return states;
    }

    /**
     * Get the total number of items in the bag.
     *
     * @return the number of items in the bag
     */
    public int size() {
        return sharedList.size();
    }

    public void dumpState() {
        sharedList.forEach(entry -> logger.info(entry.toString()));
    }

    /**
     * Determine whether to use WeakReferences based on whether there is a
     * custom ClassLoader implementation sitting between this class and the
     * System ClassLoader.
     *
     * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
     */
    private boolean useWeakThreadLocals() {
        try {
            if (System.getProperty("pool.jdbc.useWeakReferences") != null) {
                return Boolean.getBoolean("pool.jdbc.useWeakReferences");
            }
            return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
        } catch (SecurityException se) {
            return true;
        }
    }
}
