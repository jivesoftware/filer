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
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class AutoResizingByteBufferBackedFiler implements ConcurrentFiler {

    private final Object lock;
    private final ByteBufferProvider byteBufferProvider;
    private final AtomicReference<ByteBuffer> bufferReference;

    public AutoResizingByteBufferBackedFiler(Object lock, long initialSize, ByteBufferProvider byteBufferProvider) {
        this.lock = lock;
        this.byteBufferProvider = byteBufferProvider;
        this.bufferReference = new AtomicReference<>(byteBufferProvider.allocate(initialSize));
    }

    private AutoResizingByteBufferBackedFiler(Object lock, ByteBufferProvider byteBufferProvider, AtomicReference<ByteBuffer> bufferReference) {
        this.lock = lock;
        this.byteBufferProvider = byteBufferProvider;
        this.bufferReference = bufferReference;
    }

    @Override
    public Object lock() {
        return lock;
    }

    @Override
    public Filer asConcurrentReadWrite(Object suggestedLock) throws IOException {
        return this;
    }

    private void checkAllocation(int size) {
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

    @Override
    public void seek(long position) throws IOException {
        checkAllocation((int) position);
        bufferReference.get().position((int) position); // what a pain! limited to an int!
    }

    @Override
    public long skip(long position) throws IOException {
        int p = bufferReference.get().position();
        p += position;
        checkAllocation(p);
        bufferReference.get().position(p);
        return p;
    }

    @Override
    public long length() throws IOException {
        return bufferReference.get().capacity();
    }

    @Override
    public void setLength(long len) throws IOException {
        checkAllocation((int) len); // what a pain! limited to an int!
    }

    @Override
    public long getFilePointer() throws IOException {
        return bufferReference.get().position();
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
        return bufferReference.get().get();
    }

    @Override
    public int read(byte[] b) throws IOException {
        bufferReference.get().get(b);
        return b.length;
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        ByteBuffer byteBuffer = bufferReference.get();
        int remaining = byteBuffer.remaining();
        if (remaining == 0) {
            return -1;
        }
        int count = Math.min(_len, remaining);
        byteBuffer.get(b, _offset, count);
        return count;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void write(int b) throws IOException {
        checkAllocation(bufferReference.get().position() + 1);
        bufferReference.get().put((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        checkAllocation(bufferReference.get().position() + b.length);
        bufferReference.get().put(b);
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        checkAllocation(bufferReference.get().position() + _len);
        bufferReference.get().put(b, _offset, _len);
    }

    @Override
    public long capacity() {
        return Integer.MAX_VALUE;
    }

}
