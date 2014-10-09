package com.jivesoftware.os.filer.queue.store;

import java.util.List;

/**
 * A specialization of Queue that helps solve fault tolerance.
 *
 * @param <V> Type of item in queue
 */
public interface PhasedQueue {

    /**
     *
     * @param phase
     * @return number of elements in queue that are at a particular phase. size doesn't need to be exact ie eventual size ok. Used to pick which queue should be
     * consumed next.
     * @throws Exception
     */
    public long getSize(long phase) throws Exception;

    /**
     *
     * @param phase the initial phase for the added value. if the value is in the queue with an newer phase then add is ignored!
     * @param timestamp used to reorder queues
     * @param value the value to be enqueued
     * @throws Exception throw exception if failed to add
     */
    public void add(long phase, long timestamp, byte[] value) throws Exception;

    /**
     *
     * @param currentPhase looking for item in the phase
     * @param phaseUponConsuming set the phase for the consumed item to this
     * @param batchSize
     * @param maxBytes the number of bytes a batch get should be less than. use Long.MAX_VALUE to ignore
     * @return
     * @throws Exception
     */
    public List<PhasedQueueEntry> consume(long currentPhase, long phaseUponConsuming, int batchSize, long maxBytes) throws Exception;

    /**
     *
     * @return the number of bytes this queue is roughly using
     */
    public long length();

    /**
     * remove all items from the queue!
     */
    public void clear();

    /**
     * closes the queue
     */
    public void close();
}
