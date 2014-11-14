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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class AutoResizingByteBufferBackedFiler implements ConcurrentFiler {

    private final Object lock;
    private final ByteBufferProvider byteBufferProvider;
    private final SharedByteBuffer sharedByteBuffer;
    private final Semaphore semaphore;
    private final int semaphorePermits;

    public AutoResizingByteBufferBackedFiler(Object lock,
        long initialSize,
        ByteBufferProvider byteBufferProvider,
        Semaphore semaphore,
        int semaphorePermits) {
        this.lock = lock;
        this.byteBufferProvider = byteBufferProvider;
        ByteBuffer rootBuffer = byteBufferProvider.allocate(initialSize);
        this.sharedByteBuffer = new SharedByteBuffer(new AtomicReference<>(rootBuffer), rootBuffer);
        this.semaphore = semaphore;
        this.semaphorePermits = semaphorePermits;
    }

    private AutoResizingByteBufferBackedFiler(Object lock,
        ByteBufferProvider byteBufferProvider,
        Semaphore semaphore,
        int semaphorePermits,
        SharedByteBuffer sharedByteBuffer) {
        this.lock = lock;
        this.byteBufferProvider = byteBufferProvider;
        this.semaphore = semaphore;
        this.semaphorePermits = semaphorePermits;
        this.sharedByteBuffer = sharedByteBuffer;
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

    @Override
    public Object lock() {
        return lock;
    }

    @Override
    public Filer asConcurrentReadWrite(Object suggestedLock) throws IOException {
        acquire(1, "As concurrent");
        try {
            return new AutoResizingByteBufferBackedFiler(suggestedLock, byteBufferProvider, semaphore, semaphorePermits,
                new SharedByteBuffer(sharedByteBuffer.bufferReference, sharedByteBuffer.clonedBuffer));
        } finally {
            release(1);
        }
    }

    @Override
    public void delete() throws Exception {
        acquire(semaphorePermits, "Delete");
        try {
            ByteBuffer bb = sharedByteBuffer.delete();
            DirectBufferCleaner.clean(bb);
        } finally {
            release(semaphorePermits);
        }
    }

    private void checkAllocation(int size) throws IOException {
        boolean needsResize;
        acquire(1, "Checking allocation");
        try {
            ByteBuffer buffer = sharedByteBuffer.get();
            if (buffer == null) {
                throw new IOException("Filer has been closed");
            }
            int currentCapacity = buffer.capacity();
            needsResize = (currentCapacity < size);
        } finally {
            release(1);
        }

        if (needsResize) {
            acquire(semaphorePermits, "Checking allocation");
            try {
                sharedByteBuffer.grow(byteBufferProvider, size);
            } finally {
                release(semaphorePermits);
            }
        }
    }

    @Override
    public void seek(long position) throws IOException {
        checkAllocation((int) position);
        acquire(1, "Seeking");
        try {
            sharedByteBuffer.get().position((int) position); // what a pain! limited to an int!
        } finally {
            release(1);
        }
    }

    @Override
    public long skip(long position) throws IOException {
        int p;
        acquire(1, "Before skip");
        try {
            p = sharedByteBuffer.get().position() + (int) position;
        } finally {
            release(1);
        }
        checkAllocation(p);
        acquire(1, "During skip");
        try {
            sharedByteBuffer.get().position(p);
        } finally {
            release(1);
        }
        return p;
    }

    @Override
    public long length() throws IOException {
        acquire(1, "Getting length");
        try {
            return sharedByteBuffer.get().capacity();
        } finally {
            release(1);
        }
    }

    @Override
    public void setLength(long len) throws IOException {
        checkAllocation((int) len); // what a pain! limited to an int!
    }

    @Override
    public long getFilePointer() throws IOException {
        return sharedByteBuffer.position();
    }

    @Override
    public void eof() throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public int read() throws IOException {
        acquire(1, "Reading byte");
        try {
            ByteBuffer buffer = sharedByteBuffer.get();
            int remaining = buffer.remaining();
            if (remaining == 0) {
                return -1;
            }
            return buffer.get();
        } finally {
            release(1);
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        acquire(1, "Reading byte array with offset");
        try {
            ByteBuffer byteBuffer = sharedByteBuffer.get();
            int remaining = byteBuffer.remaining();
            if (remaining == 0) {
                return -1;
            }
            int count = Math.min(_len, remaining);
            byteBuffer.get(b, _offset, count);
            return count;
        } finally {
            release(1);
        }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void write(int b) throws IOException {
        int size;
        acquire(1, "Before write byte");
        try {
            size = sharedByteBuffer.get().position() + 1;
        } finally {
            release(1);
        }
        checkAllocation(size);
        acquire(1, "During write byte");
        try {
            sharedByteBuffer.get().put((byte) b);
        } finally {
            release(1);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        int size;
        acquire(1, "Before write byte array");
        try {
            size = sharedByteBuffer.get().position() + b.length;
        } finally {
            release(1);
        }
        checkAllocation(size);
        acquire(1, "During write byte array");
        try {
            sharedByteBuffer.get().put(b);
        } finally {
            release(1);
        }
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        int size;
        acquire(1, "Before write byte array with offset");
        try {
            size = sharedByteBuffer.get().position() + _len;
        } finally {
            release(1);
        }
        checkAllocation(size);
        acquire(1, "During write byte array with offset");
        try {
            sharedByteBuffer.get().put(b, _offset, _len);
        } finally {
            release(1);
        }
    }

    @Override
    public long capacity() {
        return Integer.MAX_VALUE;
    }

    private static class SharedByteBuffer {

        private final AtomicReference<ByteBuffer> bufferReference;
        private ByteBuffer rootBuffer;
        private ByteBuffer clonedBuffer;

        private SharedByteBuffer(AtomicReference<ByteBuffer> bufferReference, ByteBuffer currentBuffer) {
            this.bufferReference = bufferReference;

            this.rootBuffer = bufferReference.get();
            this.clonedBuffer = currentBuffer.duplicate();
        }

        ByteBuffer get() {
            if (clonedBuffer != null) {
                ByteBuffer currentBuffer = bufferReference.get();
                if (rootBuffer != currentBuffer) {
                    rootBuffer = currentBuffer;
                    if (rootBuffer != null) {
                        int position = clonedBuffer.position();
                        clonedBuffer = rootBuffer.duplicate();
                        clonedBuffer.position(position);
                    } else {
                        clonedBuffer = null;
                    }
                }
            }
            return clonedBuffer;
        }

        void grow(ByteBufferProvider byteBufferProvider, int size) {
            ByteBuffer buffer = bufferReference.get();
            int currentCapacity = buffer.capacity();
            while (currentCapacity < size) {
                if (bufferReference.compareAndSet(buffer, byteBufferProvider.reallocate(buffer, FilerIO.chunkLength(FilerIO.chunkPower(size, 0))))) {
                    break;
                }
                buffer = bufferReference.get();
                currentCapacity = buffer.capacity();
            }
        }

        ByteBuffer delete() {
            return bufferReference.getAndSet(null);
        }

        long position() {
            return clonedBuffer.position();
        }
    }
}
