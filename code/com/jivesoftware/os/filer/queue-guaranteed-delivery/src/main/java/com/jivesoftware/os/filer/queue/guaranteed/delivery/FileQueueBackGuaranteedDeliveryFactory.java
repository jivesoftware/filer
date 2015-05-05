/*
 * $Revision: 142270 $
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.guaranteed.delivery;

import com.jivesoftware.os.filer.queue.processor.PhasedQueueBatchProcessor;
import com.jivesoftware.os.filer.queue.processor.PhasedQueueProcessor;
import com.jivesoftware.os.filer.queue.processor.PhasedQueueProcessorConfig;
import com.jivesoftware.os.filer.queue.store.FileQueueImpl;
import com.jivesoftware.os.filer.queue.store.PhasedQueue;
import com.jivesoftware.os.filer.queue.store.PhasedQueueConstants;
import com.jivesoftware.os.filer.queue.store.PhasedQueueEntry;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author jonathan
 */
public class FileQueueBackGuaranteedDeliveryFactory {

    /**
     *
     * @param serviceConfig
     * @param deliveryCallback
     * @return
     * @throws IOException
     */
    public static GuaranteedDeliveryService createService(FileQueueBackGuaranteedDeliveryServiceConfig serviceConfig,
        DeliveryCallback deliveryCallback) throws IOException {

        final MutableLong undelivered = new MutableLong(0);
        final MutableLong delivered = new MutableLong(0);
        final FileQueueImpl queue = new FileQueueImpl(serviceConfig.getPathToQueueFiles(),
            serviceConfig.getQueueName(),
            serviceConfig.getTakableWhenCreationTimestampIsOlderThanXMillis(),
            serviceConfig.getTakableWhenLastAppendedIsOlderThanXMillis(),
            serviceConfig.getTakableWhenLargerThanXEntries(),
            serviceConfig.getMaxPageSizeInBytes(),
            serviceConfig.getPushbackAtEnqueuedSize(),
            serviceConfig.isDeleteOnExit(),
            undelivered,
            serviceConfig.isTakeFullQueuesOnly());

        final GuaranteedDeliveryServiceStatus status = new GuaranteedDeliveryServiceStatus() {
            @Override
            public long undelivered() {
                return undelivered.longValue();
            }

            @Override
            public long delivered() {
                return delivered.longValue();
            }
        };

        final QueueProcessorPool processorPool = new QueueProcessorPool(
            queue,
            serviceConfig.getNumberOfConsumers(),
            serviceConfig.getQueueProcessorConfig(),
            delivered,
            deliveryCallback);

        GuaranteedDeliveryService service = new GuaranteedDeliveryService() {
            private final AtomicBoolean running = new AtomicBoolean(false);

            @Override
            public void add(List<byte[]> add) throws DeliveryServiceException {
                if (running.compareAndSet(false, true)) {
                    processorPool.start();
                }

                if (add == null) {
                    return;
                }
                for (int i = 0; i < add.size(); i++) {
                    byte[] value = add.get(i);
                    if (value == null) {
                        continue;
                    }
                    try {
                        queue.add(PhasedQueueConstants.ENQUEUED, System.currentTimeMillis(), value);
                    } catch (Exception ex) {
                        throw new DeliveryServiceException(add, "failed tp deliver the following items", ex);
                    }
                }
            }

            @Override
            public GuaranteedDeliveryServiceStatus getStatus() {
                return status;
            }

            @Override
            public void close() {
                if (running.compareAndSet(true, false)) {
                    processorPool.stop();
                }
            }
        };

        return service;
    }

    static class QueueProcessorPool {

        private final PhasedQueueProcessor[] queueProcessors;
        private final ExecutorService queueProcessorsThreads;

        public QueueProcessorPool(PhasedQueue queue,
            int numberOfConsumers,
            PhasedQueueProcessorConfig queueProcessorConfig,
            final MutableLong delivered,
            final DeliveryCallback deliveryCallback) {

            this.queueProcessorsThreads = Executors.newFixedThreadPool(numberOfConsumers);
            this.queueProcessors = new PhasedQueueProcessor[numberOfConsumers];

            PhasedQueueBatchProcessor batchProcessor = process -> {
                if (deliveryCallback.deliver(() -> {
                    final Iterator<PhasedQueueEntry> processablesIterator = process.iterator();
                    return new Iterator<byte[]>() {
                        @Override
                        public boolean hasNext() {
                            return processablesIterator.hasNext();
                        }

                        @Override
                        public byte[] next() {
                            return processablesIterator.next().getEntry();
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException("Not ever supported!");
                        }
                    };

                })) {
                    delivered.add(process.size());
                    for (PhasedQueueEntry p : process) {
                        p.processed();
                    }
                    return Collections.emptyList();
                } else {
                    return process;
                }
            };

            PhasedQueueProcessorConfig.Builder builder = PhasedQueueProcessorConfig.newBuilder(queueProcessorConfig);
            for (int i = 0; i < numberOfConsumers; i++) {
                builder.setQueueName(queueProcessorConfig.getName() + "-" + (i + 1));
                this.queueProcessors[i] = new PhasedQueueProcessor(builder.build(), queue, batchProcessor);
            }
        }

        synchronized public void start() {
            for (PhasedQueueProcessor processor : queueProcessors) {
                if (processor.isRunning()) {
                    continue;
                }
                queueProcessorsThreads.submit(processor);
            }
        }

        synchronized public void stop() {
            for (PhasedQueueProcessor processor : queueProcessors) {
                processor.stop();
            }

            this.queueProcessorsThreads.shutdownNow();
        }
    }
}
