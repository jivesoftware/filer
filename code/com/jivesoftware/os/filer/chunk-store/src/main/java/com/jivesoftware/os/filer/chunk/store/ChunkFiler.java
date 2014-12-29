package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 This class segments a single Filer into segment filers where
 each segment filer restates fp = 0. It only allows one segment filer
 at a time to be in control. It is the responsibility of the
 programmer to remove the segment filers as the become stale.

 @author jonathan.colt
 */
public class ChunkFiler implements Filer {

    private final ChunkStore chunkStore;
    private final Filer filer;
    private final long chunkFP;
    private final long startOfFP;
    private final long endOfFP;
    private final AtomicBoolean recycle = new AtomicBoolean();

    ChunkFiler(ChunkStore chunkStore, Filer filer, long chunkFP, long startOfFP, long endOfFP) {
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

    void recycle() {
        recycle.set(true);
    }

    @Override
    public void close() throws IOException {
        if (isFileBacked()) {
            filer.close();
        }
        if (recycle.get()) {
            chunkStore.remove(chunkFP);
        }
    }

    @Override
    public String toString() {
        return "SOF=" + startOfFP + " EOF=" + endOfFP;
    }

    final public long getSize() throws IOException {
        if (isFileBacked()) {
            return length();
        }
        return endOfFP - startOfFP;
    }

    final public boolean isFileBacked() {
        return (endOfFP == Long.MAX_VALUE);
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
            throw new IndexOutOfBoundsException("A write starting at fp:" + fp + " with a  len:" + length() + " will overflow  bounds. " + this);
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

        if (isFileBacked()) {
            return filer.length();
        }
        return endOfFP - startOfFP;
    }

    @Override
    final public void setLength(long len) throws IOException {

        if (isFileBacked()) {
            filer.setLength(len);
        } else {
            throw new IOException("try to modified a fixed length filer");
        }
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
        if (isFileBacked()) {
            filer.eof();
        } else {
            filer.seek(endOfFP - startOfFP);
        }
    }

    @Override
    final public void flush() throws IOException {
        filer.flush();
    }

}
