/*
 * $Revision: 142270 $
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.processor;

import com.jivesoftware.os.filer.queue.store.PhasedQueueConstants;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jonathan
 */
final public class PhasedQueueProcessorConfig {

    final private String name;
    final private long backoffForNMillisWhenConsumerReturnsZero;
    final private long initialPhase;
    final private long phaseUponConsuming;
    final private int maxBatchSize;
    final private long maxBatchSizeInBytes;
    final private long onFailureToDeliverRetryAfterNMillis;
    // use the following to allow for larger batch processing with a trade-off of increased latency.  The Processor
    // will try to wait until there are at least idealMinimumBatchSize entries avalailable to consume, OR
    // idealMinimumBatchSizeMaxWaitMillis milliseconds elapsed since the last consume (whichever comes first).
    final private int idealMinimumBatchSize;
    final private long idealMinimumBatchSizeMaxWaitMillis;

    private PhasedQueueProcessorConfig(
            String name,
            long backoffForNMillisWhenConsumerReturnsZero,
            long initialPhase,
            long phaseUponConsuming,
            int maxBatchSize,
            long maxBatchSizeInBytes,
            long onFailureToDeliverRetryAfterNMillis,
            int idealMinimumBatchSize,
            long idealMinimumBatchSizeMaxWaitMillis) {
        this.name = name;
        this.backoffForNMillisWhenConsumerReturnsZero = backoffForNMillisWhenConsumerReturnsZero;
        this.initialPhase = initialPhase;
        this.phaseUponConsuming = phaseUponConsuming;
        this.maxBatchSize = maxBatchSize;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.onFailureToDeliverRetryAfterNMillis = onFailureToDeliverRetryAfterNMillis;
        this.idealMinimumBatchSize = idealMinimumBatchSize;
        this.idealMinimumBatchSizeMaxWaitMillis = idealMinimumBatchSizeMaxWaitMillis;
    }

    public long getBackoffForNMillisWhenConsumerReturnsZero() {
        return backoffForNMillisWhenConsumerReturnsZero;
    }

    public long getInitialPhase() {
        return initialPhase;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public long getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public String getName() {
        return name;
    }

    public long getPhaseUponConsuming() {
        return phaseUponConsuming;
    }

    public long getOnFailureToDeliverRetryAfterNMillis() {
        return onFailureToDeliverRetryAfterNMillis;
    }

    public int getIdealMinimumBatchSize() {
        return idealMinimumBatchSize;
    }

    public long getIdealMinimumBatchSizeMaxWaitMillis() {
        return idealMinimumBatchSizeMaxWaitMillis;
    }

    public static Builder newBuilder(String queueName) {
        return new Builder(queueName);
    }

    public static Builder newBuilder(PhasedQueueProcessorConfig config) {
        return new Builder(config);
    }

    final public static class Builder {

        private String queueName;
        private long backoffForNMillisWhenConsumerReturnsZero = TimeUnit.SECONDS.toMillis(1);
        private long initialPhase = PhasedQueueConstants.ENQUEUED;
        private long phaseUponConsuming = PhasedQueueConstants.CONSUMED;
        private int maxBatchSize = 1;
        private long maxBatchSizeInBytes = 10 * 1024 * 1024; // 10mb
        private long onFailureToDeliverRetryAfterNMillis = TimeUnit.SECONDS.toMillis(1);
        private int idealMinimumBatchSize = 0;
        private long idealMinimumBatchSizeMaxWaitMillis = 0;

        private Builder(String queueName) {
            this.queueName = queueName;
        }

        private Builder(PhasedQueueProcessorConfig config) {
            this.queueName = config.getName();
            this.backoffForNMillisWhenConsumerReturnsZero = config.getBackoffForNMillisWhenConsumerReturnsZero();
            this.initialPhase = config.getInitialPhase();
            this.phaseUponConsuming = config.getPhaseUponConsuming();
            this.maxBatchSize = config.getMaxBatchSize();
            this.maxBatchSizeInBytes = config.getMaxBatchSizeInBytes();
            this.idealMinimumBatchSize = config.getIdealMinimumBatchSize();
            this.idealMinimumBatchSizeMaxWaitMillis = config.getIdealMinimumBatchSizeMaxWaitMillis();
        }

        public Builder setQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder setBackoffForNMillisWhenConsumerReturnsZero(long backoffForNMillisWhenConsumerReturnsZero) {
            this.backoffForNMillisWhenConsumerReturnsZero = backoffForNMillisWhenConsumerReturnsZero;
            return this;
        }

        public Builder setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder setMaxBatchSizeInBytes(long maxBatchSizeInBytes) {
            this.maxBatchSizeInBytes = maxBatchSizeInBytes;
            return this;
        }

        public Builder setInitialPhase(long initialPhase) {
            this.initialPhase = initialPhase;
            return this;
        }

        public Builder setPhaseUponConsuming(long phaseUponConsuming) {
            this.phaseUponConsuming = phaseUponConsuming;
            return this;
        }

        public Builder setOnFailureToDeliverRetryAfterNMillis(long onFailureToDeliverRetryAfterNMillis) {
            this.onFailureToDeliverRetryAfterNMillis = onFailureToDeliverRetryAfterNMillis;
            return this;
        }

        public Builder setIdealMinimumBatchSize(int idealMinimumBatchSize) {
            this.idealMinimumBatchSize = idealMinimumBatchSize;
            return this;
        }

        public Builder setIdealMinimumBatchSizeMaxWaitMillis(long idealMinimumBatchSizeMaxWaitMillis) {
            this.idealMinimumBatchSizeMaxWaitMillis = idealMinimumBatchSizeMaxWaitMillis;
            return this;
        }

        public PhasedQueueProcessorConfig build() {
            return new PhasedQueueProcessorConfig(
                    queueName,
                    backoffForNMillisWhenConsumerReturnsZero,
                    initialPhase,
                    phaseUponConsuming,
                    maxBatchSize,
                    maxBatchSizeInBytes,
                    onFailureToDeliverRetryAfterNMillis,
                    idealMinimumBatchSize,
                    idealMinimumBatchSizeMaxWaitMillis);
        }
    }
}
