/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.processor;

import com.jivesoftware.os.filer.queue.store.PhasedQueue;
import com.jivesoftware.os.filer.queue.store.PhasedQueueEntry;
import com.jivesoftware.os.jive.utils.base.util.UtilThread;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PhasedQueueProcessor implements Runnable {

    /**
     * @param process
     * @return return true if process was successful
     */
    static private final MetricLogger logger = MetricLoggerFactory.getLogger();
    static private final int OFF = 0;
    static private final int RUNNING = 1;
    static private final int STOPPING = 2;
    private final PhasedQueueProcessorConfig phasedQueueProcessorConfig;
    private final PhasedQueue queue;
    private final PhasedQueueBatchProcessor processor;
    private AtomicInteger state = new AtomicInteger(OFF);
    private long lastConsumeTimestamp = System.currentTimeMillis();

    public PhasedQueueProcessor(PhasedQueueProcessorConfig phasedQueueProcessorConfig, PhasedQueue queue, PhasedQueueBatchProcessor processor) {
        this.phasedQueueProcessorConfig = phasedQueueProcessorConfig;
        this.queue = queue;
        this.processor = processor;
    }

    public void stop() {
        state.set(STOPPING);
    }

    public boolean isRunning() {
        return state.get() == RUNNING || state.get() == STOPPING;
    }

    @Override
    public void run() {
        if (!state.compareAndSet(OFF, RUNNING)) {
            logger.error("Unexpected state while starting to run " + phasedQueueProcessorConfig.getName() + ".");
            return;
        }
        while (state.get() == RUNNING) {

            logger.startTimer(phasedQueueProcessorConfig.getName());

            List<PhasedQueueEntry> toBeProcessed = readNextBatch();
            List<PhasedQueueEntry> failures = processBatch(toBeProcessed);
            while (!failures.isEmpty()) {
                logger.incAtomic(phasedQueueProcessorConfig.getName() + ">failed", toBeProcessed.size());
                long retryAfterNMillis = phasedQueueProcessorConfig.getOnFailureToDeliverRetryAfterNMillis();
                logger.trace(phasedQueueProcessorConfig.getName()
                        + " failed to send! " + toBeProcessed.size() + " message/s. Will retry in " + retryAfterNMillis + "millis.");
                try {
                    Thread.sleep(retryAfterNMillis);
                } catch (InterruptedException ie) {
                    markAsFailed(toBeProcessed);
                    logger.info(this + " thread interrupted! Breaking out of ");
                    return;
                }
                failures = processBatch(failures);
            }
            logger.stopTimer(phasedQueueProcessorConfig.getName());
        }
        state.set(OFF);
    }

    private List<PhasedQueueEntry> processBatch(List<PhasedQueueEntry> toBeProcessed) {
        try {
            return processor.process(toBeProcessed);
        } catch (Exception x) {
            logger.warn(" Consume is having trouble processing batch. " + x.toString(), x);
            return toBeProcessed;
        }
    }

    private void markAsFailed(List<PhasedQueueEntry> consumed) {
        try {
            for (PhasedQueueEntry c : consumed) {
                c.failed(phasedQueueProcessorConfig.getInitialPhase());
            }
        } catch (Exception x) {
            logger.warn(" queue=" + phasedQueueProcessorConfig.getName() + " is having trouble marking as failed. " + x.toString(), x);
        }

    }

    private List<PhasedQueueEntry> readNextBatch() {
        List<PhasedQueueEntry> consumed = Collections.emptyList();
        while (state.get() == RUNNING && consumed.isEmpty()) {
            if (readyToConsume(queue)) {
                try {
                    consumed = queue.consume(phasedQueueProcessorConfig.getInitialPhase(),
                            phasedQueueProcessorConfig.getPhaseUponConsuming(),
                            phasedQueueProcessorConfig.getMaxBatchSize(),
                            phasedQueueProcessorConfig.getMaxBatchSizeInBytes());
                } catch (Exception x) {
                    logger.error(" Queue consumer for " + phasedQueueProcessorConfig.getName() + " is having trouble.", x);
                }
            }

            long backoff = phasedQueueProcessorConfig.getBackoffForNMillisWhenConsumerReturnsZero();
            if (consumed.isEmpty() && backoff > 0) {
                UtilThread.sleep(backoff);
                logger.debug("sleeping for " + backoff);
            }
        }
        if (!consumed.isEmpty()) {
            lastConsumeTimestamp = System.currentTimeMillis();
        }
        return consumed;

    }

    private boolean readyToConsume(PhasedQueue queue) {
        if (phasedQueueProcessorConfig.getIdealMinimumBatchSize() <= 0
                || phasedQueueProcessorConfig.getIdealMinimumBatchSizeMaxWaitMillis() <= 0
                || (System.currentTimeMillis() - lastConsumeTimestamp) > phasedQueueProcessorConfig.getIdealMinimumBatchSizeMaxWaitMillis()) {
            return true;
        }
        try {
            return queue.getSize(phasedQueueProcessorConfig.getInitialPhase()) >= phasedQueueProcessorConfig.getIdealMinimumBatchSize();
        } catch (Exception e) {
            logger.error("Problem getting size of the queue for ideal minimum check.  Allowing to consume. ", e);
            return true;
        }
    }
}
