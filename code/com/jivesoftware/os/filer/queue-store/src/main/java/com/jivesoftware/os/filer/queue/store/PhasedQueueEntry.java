/*
 * $Revision: 142270 $
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.store;

/**
 *
 * @author jonathan
 */
public interface PhasedQueueEntry {

    /**
     * call this when you are done processing the entry
     */
    public void processed();

    /**
     * call when you have failed to process entry and want to return the item to the queue.
     *
     * @param phase
     */
    public void failed(long phase);

    /**
     *
     * @return
     */
    public long getTimestamp();

    /**
     *
     * @return
     */
    public byte[] getEntry();

    /**
     *
     * @return
     */
    public long size();

    /**
     * removes resource associated with entry
     */
    public void clearEntry();
}
