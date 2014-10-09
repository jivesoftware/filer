/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.filer.queue.store;

/**
 *
 * @author jonathan
 */
public class PhasedQueueEntryImpl implements PhasedQueueEntry {

    private final FileQueueEntry entry;

    public PhasedQueueEntryImpl(FileQueueEntry entry) {
        this.entry = entry;
    }

    @Override
    public void processed() {
        entry.processed();
    }

    /**
     * should only call this if you can process the message but just not at this time
     *
     * @param modeIfFailed
     */
    public void failed(long modeIfFailed) {
        entry.failed(modeIfFailed);
    }

    /**
     * @return the timestamp
     */
    @Override
    public long getTimestamp() {
        return entry.getTimestamp();
    }

    /**
     * @return the fp
     */
    public long getFp() {
        return entry.getFp();
    }

    /**
     * @return the entry
     */
    @Override
    public byte[] getEntry() {
        return entry.getEntry();
    }

    /**
     *
     * @return
     */
    @Override
    public long size() {
        if (entry == null) {
            return 0;
        }
        return entry.size();
    }

    /**
     *
     */
    @Override
    public void clearEntry() {
        entry.clearEntry();
    }
}
