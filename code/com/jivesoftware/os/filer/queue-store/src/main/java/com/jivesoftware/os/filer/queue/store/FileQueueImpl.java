/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.store;

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author jonathan
 */
public class FileQueueImpl implements PhasedQueue {

    private static final MetricLogger logger = MetricLoggerFactory.getLogger();
    private TwoPhasedFileQueueImpl queue;
    private volatile TookPhasedFileQueue took;
    private final Object tookLock = new Object();
    private final long takableWhenCreationTimestampIsOlderThanXMillis;
    private final long takableWhenLastAppendedIsOlderThanXMillis;
    private final long takableWhenLargerThanXSize;

    public FileQueueImpl(
            String pathToQueueFiles,
            String queueName,
            long takableWhenLastAppendedIsOlderThanXMillis,
            long takableWhenLargerThanXSize,
            long maxQueueLength,
            int pushbackAtQueueSize,
            boolean deleteQueueFilesOnExit,
            MutableLong pending) throws IOException {
        this(pathToQueueFiles, queueName, takableWhenLastAppendedIsOlderThanXMillis, takableWhenLargerThanXSize, maxQueueLength,
                pushbackAtQueueSize, deleteQueueFilesOnExit, pending, false);
    }

    public FileQueueImpl(
            String pathToQueueFiles,
            String queueName,
            long takableWhenLastAppendedIsOlderThanXMillis,
            long takableWhenLargerThanXSize,
            long maxQueueLength,
            int pushbackAtQueueSize,
            boolean deleteQueueFilesOnExit,
            MutableLong pending,
            boolean takeFullQueuesOnly) throws IOException {
        this(pathToQueueFiles, queueName, Long.MAX_VALUE, takableWhenLastAppendedIsOlderThanXMillis,
                takableWhenLargerThanXSize, maxQueueLength, pushbackAtQueueSize, deleteQueueFilesOnExit, pending,
                takeFullQueuesOnly);
    }

    public FileQueueImpl(
            String pathToQueueFiles,
            String queueName,
            long takableWhenCreationTimestampIsOlderThanXMillis,
            long takableWhenLastAppendedIsOlderThanXMillis,
            long takableWhenLargerThanXSize,
            long maxQueueLength,
            int pushbackAtQueueSize,
            boolean deleteQueueFilesOnExit,
            MutableLong pending,
            boolean takeFullQueuesOnly) throws IOException {

        this.takableWhenCreationTimestampIsOlderThanXMillis = takableWhenCreationTimestampIsOlderThanXMillis;
        this.takableWhenLastAppendedIsOlderThanXMillis = takableWhenLastAppendedIsOlderThanXMillis;
        this.takableWhenLargerThanXSize = takableWhenLargerThanXSize;
        if (pushbackAtQueueSize != -1 && pushbackAtQueueSize < takableWhenLargerThanXSize) {
            throw new IllegalStateException("pushbackAtQueueSize must be -1 or larger than takableWhenLargerThanXSize");
        }
        this.queue = new TwoPhasedFileQueueImpl(pathToQueueFiles, queueName, maxQueueLength, pushbackAtQueueSize,
                deleteQueueFilesOnExit, pending, takeFullQueuesOnly);
        this.queue.init(0); // todo expose to constructor
        this.queue.open();
    }

    @Override
    public void close() {
        queue.close();
    }

    @Override
    public long length() {
        return queue.length();
    }

    @Override
    public long getSize(long phase) throws Exception {
        long c = queue.approximateCount.get();
        if (c < 0) {
            return 0;
        }
        return c;
    }

    /**
     * Results are interleaved with id,value,id,value,id,value
     *
     * @param currentPhase
     * @param phaseUponConsuming
     * @param batchSize
     * @return
     * @throws Exception
     */
    @Override
    public List<PhasedQueueEntry> consume(long currentPhase, long phaseUponConsuming, int batchSize, long maxBytes) throws Exception {
        List<PhasedQueueEntry> list = new ArrayList<>();
        synchronized (tookLock) {
            if (took == null) {
                long now = System.currentTimeMillis();
                long takeableIfLastAppendedIsOlderThanTimestampMillis = now - takableWhenLastAppendedIsOlderThanXMillis;
                if (queue.approximateCount.get() > takableWhenLargerThanXSize) {
                    takeableIfLastAppendedIsOlderThanTimestampMillis = Long.MAX_VALUE; // for when into the future which will force the page to be taken
                }
                long takeableIfCreationTimestampIsOlderThanTimestampMillis = now - takableWhenCreationTimestampIsOlderThanXMillis;

                took = queue.take(takeableIfCreationTimestampIsOlderThanTimestampMillis, takeableIfLastAppendedIsOlderThanTimestampMillis);
                logger.debug("taking from " + took);
            }
            if (took.isState(TookPhasedFileQueue.State.EMPTY)
                    || took.isState(TookPhasedFileQueue.State.BUSY)
                    || took.isState(TookPhasedFileQueue.State.FAILED)) {
                took = null;
                return list;
            }
            for (int i = 0; i < batchSize; i++) {
                PhasedQueueEntryImpl consumed = took.consume();
                if (consumed != null) {
                    list.add(consumed);
                    maxBytes -= consumed.size();
                    if (maxBytes < 0) {
                        break;
                    }
                } else {
                    logger.debug("processed " + took);
                    // if we call processed here we will remove the page before all entries have been confirmed as processed or failed
                    // so we won't!
                    //  queue.processed(took, false);
                    took = null;
                    break;
                }
            }
            return list;
        }
    }

    @Override
    public void add(long phase, long timestamp, byte[] value) throws Exception {
        queue.append(timestamp, value);
    }

    @Override
    public void clear() {
        synchronized (tookLock) {
            if (took != null) {
                // emtyp current took
                while (true) {
                    PhasedQueueEntryImpl consumed = took.consume();
                    if (consumed == null) {
                        break;
                    }
                    consumed.processed();
                }
                took = null;
            }
            queue.clear();
        }
    }
}
