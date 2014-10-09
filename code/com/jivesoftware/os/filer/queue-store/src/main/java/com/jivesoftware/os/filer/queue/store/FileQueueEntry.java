/*
 * $Revision$
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
abstract public class FileQueueEntry {

    abstract public void processed();

    abstract public void failed(long modeIfFailed);
    final private long fp;
    final private long timestamp;
    private byte[] entry;

    public FileQueueEntry(long fp, long timestamp, byte[] entry) {
        this.fp = fp;
        this.timestamp = timestamp;
        this.entry = entry;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return the fp
     */
    public long getFp() {
        return fp;
    }

    /**
     * @return the entry
     */
    public byte[] getEntry() {
        return entry;
    }

    /**
     *
     * @return
     */
    public long size() {
        if (entry == null) {
            return 0;
        }
        return entry.length;
    }

    /**
     *
     */
    public void clearEntry() {
        entry = null;
    }
}
