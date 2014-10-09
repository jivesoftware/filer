package com.jivesoftware.os.filer.queue.store;

/**
 *
 */
public abstract class PhasedQueueFactory {

    /**
     * @param name
     * @return
     * @throws Exception
     */
    public abstract PhasedQueue queue(String name) throws Exception;
}
