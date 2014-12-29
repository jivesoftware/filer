/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class ResizableByteBuffer {

    private final ByteBufferProvider byteBufferProvider;
    private final AtomicReference<ByteBuffer> sharedByteBuffer;
    private final boolean autoResize;
    private final Semaphore semaphore;
    private final int semaphorePermits;
    private final ThreadLocal<BufferStack> reentrant = new ThreadLocal<BufferStack>() {
        @Override
        protected BufferStack initialValue() {
            return new BufferStack();
        }
    };

    private static class BufferStack {
        private final AtomicInteger semaphores = new AtomicInteger(0);
        private final List<SharedByteBufferBackedFiler> filers = new ArrayList<>();
    }

    public ResizableByteBuffer(long initialSize,
        ByteBufferProvider byteBufferProvider,
        boolean autoResize, Semaphore semaphore,
        int semaphorePermits) {
        this.byteBufferProvider = byteBufferProvider;
        this.autoResize = autoResize;
        ByteBuffer rootBuffer = byteBufferProvider.allocate(initialSize);
        this.sharedByteBuffer = new AtomicReference<>(rootBuffer);
        this.semaphore = semaphore;
        this.semaphorePermits = semaphorePermits;
    }

    private void acquire(int permits, String message) throws IOException {
        try {
            semaphore.acquire(permits);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IOException(message, e);
        }
    }

    private void release(int permits) {
        semaphore.release(permits);
    }

    public <R> R execute(long size, boolean greedy, FilerTransaction<Filer, R> filerTransaction) throws IOException {
        final int semaphoresToAcquire = greedy ? semaphorePermits : 1;
        BufferStack threadLocalBufferStack = reentrant.get();
        int currentSemaphores = threadLocalBufferStack.semaphores.getAndSet(semaphoresToAcquire);
        semaphore.release(currentSemaphores);
        try {
            acquire(semaphoresToAcquire, "Execute");
            int semaphores = semaphoresToAcquire;
            try {
                ByteBuffer buffer = sharedByteBuffer.get();
                if (buffer == null) {
                    throw new IOException("Filer has been closed");
                }
                int currentCapacity = buffer.capacity();
                if (currentCapacity < size) {
                    if (!autoResize) {
                        throw new IOException("Filer is at capacity");
                    }

                    release(semaphoresToAcquire);
                    semaphores = 0;

                    acquire(semaphorePermits, "Resizing");
                    semaphores = semaphorePermits;

                    // acquired a semaphore, need to get the latest buffer
                    buffer = sharedByteBuffer.get();
                    currentCapacity = buffer.capacity();
                    while (currentCapacity < size) {
                        if (sharedByteBuffer.compareAndSet(buffer, reallocateBuffer(size, buffer))) {
                            break;
                        }
                        buffer = sharedByteBuffer.get();
                        currentCapacity = buffer.capacity();
                    }

                    release(semaphorePermits);
                    semaphores = 0;

                    acquire(semaphoresToAcquire, "Resized");
                    semaphores = semaphoresToAcquire;

                    // acquired a semaphore, need to get the latest buffer
                    buffer = sharedByteBuffer.get();
                    for (SharedByteBufferBackedFiler filer : threadLocalBufferStack.filers) {
                        filer.sync(buffer.duplicate());
                    }
                }

                // commit() can potentially cause the sharedByteBuffer to change, so if we ever need to operate on the buffer
                // after the following commit() we would need to call sharedByteBuffer.get() again.
                SharedByteBufferBackedFiler filer = new SharedByteBufferBackedFiler(buffer.duplicate());
                threadLocalBufferStack.filers.add(filer);
                try {
                    return filerTransaction.commit(filer); // Root PITA dynamic code injection!
                } finally {
                    threadLocalBufferStack.filers.remove(filer);
                }
            } finally {
                if (semaphores > 0) {
                    release(semaphores);
                }
            }
        } finally {
            try {
                acquire(currentSemaphores, "Reacquire");
                threadLocalBufferStack.semaphores.set(currentSemaphores);
            } catch (IOException e) {
                threadLocalBufferStack.semaphores.set(0);
                throw e;
            }
        }
    }

    private ByteBuffer reallocateBuffer(long size, ByteBuffer buffer) {
        return byteBufferProvider.reallocate(buffer, FilerIO.chunkLength(FilerIO.chunkPower(size, 0)));
    }

    public void delete() throws IOException {
        acquire(semaphorePermits, "Delete");
        try {
            ByteBuffer bb = sharedByteBuffer.getAndSet(null);
            DirectBufferCleaner.clean(bb);
        } finally {
            release(semaphorePermits);
        }
    }
}
