/*
 * $Revision: 142270 $
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.guaranteed.delivery;

import com.jivesoftware.os.filer.queue.processor.PhasedQueueProcessorConfig;


/**
 *
 * @author jonathan
 */
final public class FileQueueBackGuaranteedDeliveryServiceConfig {

    private final String pathToQueueFiles;
    private final String queueName;
    private final long takableWhenCreationTimestampIsOlderThanXMillis;
    private final long takableWhenLastAppendedIsOlderThanXMillis;
    private final long takableWhenLargerThanXEntries;
    private final int maxPageSizeInBytes;
    private final int pushbackAtEnqueuedSize;
    private final int numberOfConsumers;
    private final boolean deleteOnExit;
    private final PhasedQueueProcessorConfig queueProcessorConfig;
    private final boolean takeFullQueuesOnly;

    private FileQueueBackGuaranteedDeliveryServiceConfig(String pathToQueueFiles, String queueName, long takableWhenCreationTimestampIsOlderThanXMillis,
            long takableWhenLastAppendedIsOlderThanXMillis, long takableWhenLargerThanXEntries, int maxPageSizeInBytes,
            int pushbackAtEnqueuedSize, int numberOfConsumers, boolean deleteOnExit, PhasedQueueProcessorConfig queueProcessorConfig,
            boolean takeFullQueuesOnly) {
        this.pathToQueueFiles = pathToQueueFiles;
        this.queueName = queueName;
        this.takableWhenCreationTimestampIsOlderThanXMillis = takableWhenCreationTimestampIsOlderThanXMillis;
        this.takableWhenLastAppendedIsOlderThanXMillis = takableWhenLastAppendedIsOlderThanXMillis;
        this.takableWhenLargerThanXEntries = takableWhenLargerThanXEntries;
        this.maxPageSizeInBytes = maxPageSizeInBytes;
        this.pushbackAtEnqueuedSize = pushbackAtEnqueuedSize;
        this.numberOfConsumers = numberOfConsumers;
        this.deleteOnExit = deleteOnExit;
        this.queueProcessorConfig = queueProcessorConfig;
        this.takeFullQueuesOnly = takeFullQueuesOnly;
    }

    public int getMaxPageSizeInBytes() {
        return maxPageSizeInBytes;
    }

    public String getPathToQueueFiles() {
        return pathToQueueFiles;
    }

    public int getPushbackAtEnqueuedSize() {
        return pushbackAtEnqueuedSize;
    }

    public String getQueueName() {
        return queueName;
    }

    public long getTakableWhenLargerThanXEntries() {
        return takableWhenLargerThanXEntries;
    }

    public long getTakableWhenCreationTimestampIsOlderThanXMillis() {
        return takableWhenCreationTimestampIsOlderThanXMillis;
    }

    /**
     *
     * @return @deprecated use getTakableWhenLastAppendedIsOlderThanXMillis instead
     */
    @Deprecated
    public long getTakableWhenOlderThanXMillis() {
        return takableWhenLastAppendedIsOlderThanXMillis;
    }

    public long getTakableWhenLastAppendedIsOlderThanXMillis() {
        return takableWhenLastAppendedIsOlderThanXMillis;
    }

    public PhasedQueueProcessorConfig getQueueProcessorConfig() {
        return queueProcessorConfig;
    }

    public int getNumberOfConsumers() {
        return numberOfConsumers;
    }

    public boolean isDeleteOnExit() {
        return deleteOnExit;
    }

    public boolean isTakeFullQueuesOnly() {
        return takeFullQueuesOnly;
    }

    @Override
    public String toString() {
        return "FileQueueBackGuaranteedDeliveryServiceConfig{"
                + "pathToQueueFiles='" + pathToQueueFiles + '\''
                + ", queueName='" + queueName + '\''
                + ", takableWhenCreationTimestampIsOlderThanXMillis=" + takableWhenCreationTimestampIsOlderThanXMillis
                + ", takableWhenLastAppendedIsOlderThanXMillis=" + takableWhenLastAppendedIsOlderThanXMillis
                + ", takableWhenLargerThanXEntries=" + takableWhenLargerThanXEntries
                + ", maxPageSizeInBytes=" + maxPageSizeInBytes
                + ", pushbackAtEnqueuedSize=" + pushbackAtEnqueuedSize
                + ", numberOfConsumers=" + numberOfConsumers
                + ", deleteOnExit=" + deleteOnExit
                + ", queueProcessorConfig=" + queueProcessorConfig
                + ", takeFullQueuesOnly=" + takeFullQueuesOnly
                + '}';
    }

    public static Builder newBuilder(String pathToQueueFiles, String queueName, PhasedQueueProcessorConfig queueProcessorConfig) {
        return new Builder(pathToQueueFiles, queueName, queueProcessorConfig);
    }

    public static Builder newBuilder(FileQueueBackGuaranteedDeliveryServiceConfig config) {
        Builder builder = new Builder(config.pathToQueueFiles, config.queueName, config.queueProcessorConfig);
        builder.takableWhenCreationTimestampIsOlderThanXMillis = config.takableWhenCreationTimestampIsOlderThanXMillis;
        builder.takableWhenLastAppendedIsOlderThanXMillis = config.takableWhenLastAppendedIsOlderThanXMillis;
        builder.takableWhenLargerThanXEntries = config.takableWhenLargerThanXEntries;
        builder.maxPageSizeInBytes = config.maxPageSizeInBytes;
        builder.pushbackAtEnqueuedSize = config.pushbackAtEnqueuedSize;
        builder.numberOfConsumers = config.numberOfConsumers;
        builder.deleteOnExit = config.deleteOnExit;
        builder.takeFullQueuesOnly = config.takeFullQueuesOnly;
        return builder;
    }

    final public static class Builder {

        private String pathToQueueFiles;
        private String queueName;
        private long takableWhenCreationTimestampIsOlderThanXMillis = Long.MAX_VALUE;
        private long takableWhenLastAppendedIsOlderThanXMillis = 1000;
        private long takableWhenLargerThanXEntries = 0;
        private int maxPageSizeInBytes = 32 * 1024 * 1024; // 32mb
        private int pushbackAtEnqueuedSize = -1;
        private int numberOfConsumers = 1;
        private boolean deleteOnExit = false;
        private PhasedQueueProcessorConfig queueProcessorConfig;
        private boolean takeFullQueuesOnly = false;

        private Builder(String pathToQueueFiles, String queueName, PhasedQueueProcessorConfig queueProcessorConfig) {
            this.pathToQueueFiles = pathToQueueFiles;
            this.queueName = queueName;
            this.queueProcessorConfig = queueProcessorConfig;
        }

        public Builder setMaxPageSizeInBytes(int maxPageSizeInBytes) {
            this.maxPageSizeInBytes = maxPageSizeInBytes;
            return this;
        }

        public Builder setPathToQueueFiles(String pathToQueueFiles) {
            this.pathToQueueFiles = pathToQueueFiles;
            return this;
        }

        public Builder setPushbackAtEnqueuedSize(int pushbackAtEnqueuedSize) {
            this.pushbackAtEnqueuedSize = pushbackAtEnqueuedSize;
            return this;
        }

        public Builder setQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder setNumberOfConsumers(int numberOfConsumers) {
            this.numberOfConsumers = numberOfConsumers;
            return this;
        }

        public Builder setTakableWhenLargerThanXEntries(long takableWhenLargerThanXEntries) {
            this.takableWhenLargerThanXEntries = takableWhenLargerThanXEntries;
            return this;
        }

        public Builder setTakableWhenCreationTimestampIsOlderThanXMillis(long takableWhenCreationTimestampIsOlderThanXMillis) {
            this.takableWhenCreationTimestampIsOlderThanXMillis = takableWhenCreationTimestampIsOlderThanXMillis;
            return this;
        }

        /**
         *
         * @param takableWhenLastAppendedIsOlderThanXMillis
         * @return
         *
         * @deprecated use seTakableWhenLastAppendedIsOlderThanXMillis instead
         */
        @Deprecated
        public Builder setTakableWhenOlderThanXMillis(long takableWhenLastAppendedIsOlderThanXMillis) {
            this.takableWhenLastAppendedIsOlderThanXMillis = takableWhenLastAppendedIsOlderThanXMillis;
            return this;
        }

        public Builder seTakableWhenLastAppendedIsOlderThanXMillis(long takableWhenLastAppendedIsOlderThanXMillis) {
            this.takableWhenLastAppendedIsOlderThanXMillis = takableWhenLastAppendedIsOlderThanXMillis;
            return this;
        }

        public Builder setDeleteOnExit(boolean deleteOnExit) {
            this.deleteOnExit = deleteOnExit;
            return this;
        }

        public Builder setTakeFullQueuesOnly(boolean takeFullQueuesOnly) {
            this.takeFullQueuesOnly = takeFullQueuesOnly;
            return this;
        }

        public FileQueueBackGuaranteedDeliveryServiceConfig build() {
            return new FileQueueBackGuaranteedDeliveryServiceConfig(pathToQueueFiles, queueName, takableWhenCreationTimestampIsOlderThanXMillis,
                    takableWhenLastAppendedIsOlderThanXMillis, takableWhenLargerThanXEntries, maxPageSizeInBytes, pushbackAtEnqueuedSize,
                    numberOfConsumers, deleteOnExit, queueProcessorConfig, takeFullQueuesOnly);
        }
    }
}
