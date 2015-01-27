package com.jivesoftware.os.filer.queue.store;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableLong;

public class TookPhasedFileQueue {

    private final static MetricLogger logger = MetricLoggerFactory.getLogger();

    public enum State {

        CONSUMEABLE,
        EMPTY,
        BUSY,
        FAILED
    }
    private final TwoPhasedFileQueueImpl tookFrom;
    private State state;
    private final Object stateLock = new Object();
    private final UniqueOrderableFileName queueId;
    private final FileQueue took;
    private final AtomicLong approximateCount;
    private final AtomicLong consumedCount;
    private final AtomicLong processedCount;
    private final List<PhasedQueueEntryImpl> failures = new LinkedList<>();
    private final MutableLong pending;

    TookPhasedFileQueue(TwoPhasedFileQueueImpl tookFrom, State state, UniqueOrderableFileName queueId, FileQueue took, AtomicLong approximateCount,
        MutableLong pending) {
        this.tookFrom = tookFrom;
        this.state = state;
        this.queueId = queueId;
        this.took = took;
        this.approximateCount = approximateCount;
        this.consumedCount = new AtomicLong();
        this.processedCount = new AtomicLong();
        this.pending = pending;
    }

    public boolean isState(State state) {
        synchronized (stateLock) {
            return this.state == state;
        }
    }

    public PhasedQueueEntryImpl consume() {
        synchronized (stateLock) {
            if (failures.size() > 0) {
                logger.warn("retrying failure message from " + tookFrom + "!");
                return failures.remove(0);
            }
            if (state == State.EMPTY) {
                return null;
            }
            FileQueueEntry next = null;
            try {
                next = took.readNext(FileQueue.ENQEUED, FileQueue.ENQEUED); // we can leave entry as is cause the remove is the main helper
            } catch (Exception x) {
                logger.error("failed to read from queue! look for corrupt files!", x);
                next = null;
            }
            if (next == null) {
                long processed = processedCount.get();
                long consumed = consumedCount.get();
                logger.debug(took + " isEmpty " + processed + " out of " + consumed);
                state = State.EMPTY;
                if (processed == consumed) { // we are all done with this page
                    tookFrom.processed(TookPhasedFileQueue.this, false);
                }
                return null;
            }
            long got = approximateCount.decrementAndGet();
            pending.setValue(got);
            consumedCount.incrementAndGet();
            return new PhasedQueueEntryImpl(next) {
                @Override
                public void processed() {
                    //don't mark individual records when we're processing at the granularity of a full file.
                    if (!tookFrom.isTakeFullQueuesOnly()) {
                        super.processed();
                    }
                    long processed = processedCount.incrementAndGet();
                    long consumed = consumedCount.get();
                    synchronized (stateLock) {
                        if (state == State.EMPTY) {
                            if (processed == consumed) { // we are all done with this page
                                tookFrom.processed(TookPhasedFileQueue.this, false);
                            }
                        }
                    }
                }

                @Override
                public void failed(long modeIfFailed) {
                    super.failed(modeIfFailed);
                    synchronized (stateLock) {
                        failures.add(this);
                    }
                }
            };
        }
    }

    FileQueue took() {
        return took;
    }

    @Override
    public String toString() {
        return state + " " + took;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TookPhasedFileQueue other = (TookPhasedFileQueue) obj;
        return !(this.queueId != other.queueId && (this.queueId == null || !this.queueId.equals(other.queueId)));
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + (this.queueId != null ? this.queueId.hashCode() : 0);
        return hash;
    }
}
