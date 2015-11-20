package com.jivesoftware.os.filer.io.chunk;

import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;
import java.nio.ByteBuffer;

/*
 This class segments a single Filer into segment filers where
 each segment filer restates fp = 0. It only allows one segment filer
 at a time to be in control. It is the responsibility of the
 programmer to remove the segment filers as the become stale.

 @author jonathan.colt
 */
public class ChunkFiler implements Filer {

    private final ChunkStore chunkStore;
    private final AutoGrowingByteBufferBackedFiler filer;
    private final long chunkFP;
    private final long startOfFP;
    private final long endOfFP;

    ChunkFiler(ChunkStore chunkStore, AutoGrowingByteBufferBackedFiler filer, long chunkFP, long startOfFP, long endOfFP) {
        this.chunkStore = chunkStore;
        this.filer = filer;
        this.chunkFP = chunkFP;
        this.startOfFP = startOfFP;
        this.endOfFP = endOfFP;
    }

    public ChunkStore getChunkStore() {
        return chunkStore;
    }

    public long getChunkFP() {
        return chunkFP;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public String toString() {
        return "SOF=" + startOfFP + " EOF=" + endOfFP;
    }

    final public long getSize() throws IOException {
        return endOfFP - startOfFP;
    }

    final public long startOfFP() {
        return startOfFP;
    }

    final public long endOfFP() {
        return endOfFP;
    }

    @Override
    final public int read() throws IOException {
        long fp = filer.getFilePointer();
        if (fp < startOfFP || fp > endOfFP) {
            throw new IndexOutOfBoundsException("FP out of bounds " + startOfFP + " <= " + fp + " <= " + endOfFP);
        } else if (fp == endOfFP) {
            return -1;
        }
        return filer.read();
    }

    @Override
    final public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte b[], int _offset, int _len) throws IOException {
        long fp = filer.getFilePointer();
        if (fp < startOfFP || fp > endOfFP) {
            throw new IndexOutOfBoundsException("FP out of bounds " + fp + " " + this);
        } else if (fp == endOfFP) {
            return -1;
        }
        return filer.read(b, _offset, (int) Math.min(endOfFP - fp, _len));
    }

    @Override
    final public void write(int b) throws IOException {
        long fp = filer.getFilePointer();
        if (fp < startOfFP || fp >= endOfFP) {
            throw new IndexOutOfBoundsException("FP out of bounds " + fp + " " + this);
        }
        filer.write(b);
    }

    @Override
    final public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte b[], int _offset, int _len) throws IOException {
        long fp = filer.getFilePointer();
        if (fp < startOfFP || fp > (endOfFP - _len)) {
            throw new IndexOutOfBoundsException("A write starting at fp:" + fp + " with a len:" + _len + " will overflow  bounds. " + this);
        }
        filer.write(b, _offset, _len);
    }

    @Override
    final public void seek(long position) throws IOException {
        if (position > endOfFP - startOfFP) {
            throw new IOException("seek overflow " + position + " " + this);
        }
        filer.seek(startOfFP + position);
    }

    @Override
    final public long skip(long position) throws IOException {
        long fp = filer.getFilePointer() + position;
        if (fp < startOfFP || fp > endOfFP) {
            throw new IndexOutOfBoundsException("FP out of bounds " + fp + " " + this);
        }
        return filer.skip(position);
    }

    @Override
    final public long length() throws IOException {
        return endOfFP - startOfFP;
    }

    @Override
    final public void setLength(long len) throws IOException {
        throw new IOException("try to modified a fixed length filer");
    }

    @Override
    final public long getFilePointer() throws IOException {

        long fp = filer.getFilePointer();
        if (fp < startOfFP || fp > endOfFP) {
            throw new IndexOutOfBoundsException("FP out of bounds " + fp + " " + this);
        }
        return fp - startOfFP;
    }

    @Override
    final public void eof() throws IOException {
        filer.seek(endOfFP - startOfFP);
    }

    @Override
    final public void flush() throws IOException {
        filer.flush();
    }

    /**
     *
     * @return whether or not this chunk filer can leak an unsafe view of its backing byte buffer.
     */
    public boolean canLeakUnsafeByteBuffer() {
        return filer.canLeak(startOfFP, endOfFP);
    }

    /**
     *
     * @return an unsafe view of this chunk filer's backing byte buffer, or null if no byte buffer can be leaked.
     * @throws IOException
     */
    public ByteBuffer leakUnsafeByteBuffer() throws IOException {
        return filer.leak(startOfFP, endOfFP);
    }

}
