package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;

/**
 *
 *
 */
public class AutoResizingChunkFiler implements Filer {

    private final MultiChunkStore.ResizingChunkFilerProvider resizingChunkFilerProvider;

    public AutoResizingChunkFiler(MultiChunkStore.ResizingChunkFilerProvider resizingChunkFilerProvider) {
        this.resizingChunkFilerProvider = resizingChunkFilerProvider;
    }

    public void init(long initialChunkSize) throws IOException {
        resizingChunkFilerProvider.init(initialChunkSize);
    }

    public boolean open() throws IOException {
        return resizingChunkFilerProvider.open();
    }

    @Override
    public Object lock() throws IOException {
        return resizingChunkFilerProvider.lock();
    }

    @Override
    public void seek(long offset) throws IOException {
        resizingChunkFilerProvider.grow(offset).seek(offset);
    }

    @Override
    public long skip(long offset) throws IOException {
        ChunkFiler filer = resizingChunkFilerProvider.get();
        return resizingChunkFilerProvider.grow(filer.getFilePointer() + offset).skip(offset);
    }

    @Override
    public long length() throws IOException {
        return resizingChunkFilerProvider.get().length();
    }

    @Override
    public void setLength(long length) throws IOException {
        resizingChunkFilerProvider.grow(length); // TODO add shrinking support
    }

    @Override
    public long getFilePointer() throws IOException {
        return resizingChunkFilerProvider.get().getFilePointer();
    }

    @Override
    public void eof() throws IOException {
        resizingChunkFilerProvider.get().eof(); // TODO add shrinking support
    }

    @Override
    public void flush() throws IOException {
        resizingChunkFilerProvider.get().flush();
    }

    @Override
    public int read() throws IOException {
        return resizingChunkFilerProvider.get().read();
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        return resizingChunkFilerProvider.get().read(bytes);
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        return resizingChunkFilerProvider.get().read(bytes, offset, length);
    }

    @Override
    public void write(int i) throws IOException {
        ChunkFiler filer = resizingChunkFilerProvider.get();
        resizingChunkFilerProvider.grow(filer.getFilePointer() + 1).write(i);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        ChunkFiler filer = resizingChunkFilerProvider.get();
        resizingChunkFilerProvider.grow(filer.getFilePointer() + bytes.length).write(bytes);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        ChunkFiler filer = resizingChunkFilerProvider.get();
        resizingChunkFilerProvider.grow(filer.getFilePointer() + length).write(bytes, offset, length);
    }

    @Override
    public void close() throws IOException {
        resizingChunkFilerProvider.get().close();
    }

}