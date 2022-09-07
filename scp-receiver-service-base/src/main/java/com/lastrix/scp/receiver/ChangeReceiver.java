package com.lastrix.scp.receiver;

import java.time.Duration;

/**
 * This interface describes change storing flow.
 * Expected usage goes as follows: sink may call multiple times {@link #receive(int, Duration)} method.
 * When sink decides that it's time to apply changes and all previously collected changes
 * are done - it calls {@link #commit(Object)} method. Receiver should track positions.
 *
 * @param <T>
 */
public interface ChangeReceiver<T> {
    /**
     * Receive maxSize entries from partition
     *
     * @param maxSize  the maximum entries to read
     * @param duration maximum poll time
     * @return the chunk of read entries
     */
    ChangeChunk<T> receive(int maxSize, Duration duration);

    /**
     * Commit state from last time receive called
     */
    void commit(Object slab);
}
