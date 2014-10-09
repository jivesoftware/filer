package com.jivesoftware.os.filer.io;

import java.io.IOException;

/*
 This class segments a single Filer into segment filers where
 each segment filer restates fp = 0. It only allows one segment filer
 at a time to be in control. It is the responsibility of the
 programmer to remove the segment filers as the become stale.

 @author jonathan.colt
 */
public class SubsetableFiler implements Filer {

    private final Filer filer;
    private final long startOfFP;
    private final long endOfFP;
    private final long count;

    public SubsetableFiler(Filer filer, long startOfFP, long endOfFP, long count) {
        this.filer = filer;
        this.startOfFP = startOfFP;
        this.endOfFP = endOfFP;
        this.count = count;
    }

    @Override
    public Object lock() {
        return filer.lock();
    }

    @Override
    public String toString() {
        return "SOF=" + startOfFP + " EOF=" + endOfFP + " Count=" + count;
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

    final public long count() {
        return count;
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
            throw new IndexOutOfBoundsException("FP out of bounds " + fp + " " + this);
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
    final public void close() throws IOException {

        if (isFileBacked()) {
            filer.close();
        }
    }

    @Override
    final public void flush() throws IOException {

        filer.flush();
    }

}
