package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.RecycledFilerException;
import com.jivesoftware.os.filer.map.store.PartitionedMapChunkBackedMapStore;
import java.io.IOException;

/**
 *
 *
 */
public class AutoResizingChunkSwappableFiler<F extends ConcurrentFiler> implements SwappableFiler {

    private final AutoResizingChunkFiler filer;
    private final MultiChunkStore chunkStore;
    private final IBA key;
    private final PartitionedMapChunkBackedMapStore<F, IBA, IBA> mapStore;
    private final PartitionedMapChunkBackedMapStore<F, IBA, IBA> swapStore;

    public AutoResizingChunkSwappableFiler(AutoResizingChunkFiler filer, MultiChunkStore chunkStore, IBA key,
        PartitionedMapChunkBackedMapStore<F, IBA, IBA> mapStore, PartitionedMapChunkBackedMapStore<F, IBA, IBA> swapStore) {
        this.filer = filer;
        this.chunkStore = chunkStore;
        this.key = key;
        this.mapStore = mapStore;
        this.swapStore = swapStore;
    }

    public SwappingFiler swap(long initialChunkSize) throws Exception {
        AutoResizingChunkFiler<F> filer = new AutoResizingChunkFiler<>(swapStore, key, chunkStore);
        filer.init(initialChunkSize);
        return new AutoResizingChunkSwappingFiler(filer);
    }

    @Override
    public void sync() throws IOException {
        try {
            if (!filer.open()) {
                throw new IOException("Failed to reopen on sync");
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Object lock() throws RecycledFilerException {
        return filer.lock();
    }

    @Override
    public void seek(long offset) throws IOException {
        filer.seek(offset);
    }

    @Override
    public long skip(long offset) throws IOException {
        return filer.skip(offset);
    }

    @Override
    public long length() throws IOException {
        return filer.length();
    }

    @Override
    public void setLength(long length) throws IOException {
        filer.setLength(length);
    }

    @Override
    public long getFilePointer() throws IOException {
        return filer.getFilePointer();
    }

    @Override
    public void eof() throws IOException {
        filer.eof();
    }

    @Override
    public void flush() throws IOException {
        filer.flush();
    }

    @Override
    public int read() throws IOException {
        return filer.read();
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        return filer.read(bytes);
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        return filer.read(bytes, offset, length);
    }

    @Override
    public void write(int i) throws IOException {
        filer.write(i);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        filer.write(bytes);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        filer.write(bytes, offset, length);
    }

    @Override
    public void close() throws IOException {
        filer.close();
    }

    private class AutoResizingChunkSwappingFiler implements SwappingFiler {

        private final Filer filer;

        public AutoResizingChunkSwappingFiler(Filer filer) {
            this.filer = filer;
        }

        @Override
        public void commit() throws Exception {
            IBA oldChunk = mapStore.get(key);
            IBA newChunk = swapStore.get(key);
            mapStore.add(key, newChunk);
            swapStore.remove(key);
            chunkStore.remove(key.getBytes(), FilerIO.bytesLong(oldChunk.getBytes()));
        }

        @Override
        public Object lock() throws RecycledFilerException {
            return filer.lock();
        }

        @Override
        public void seek(long offset) throws IOException {
            filer.seek(offset);
        }

        @Override
        public long skip(long offset) throws IOException {
            return filer.skip(offset);
        }

        @Override
        public long length() throws IOException {
            return filer.length();
        }

        @Override
        public void setLength(long length) throws IOException {
            filer.setLength(length);
        }

        @Override
        public long getFilePointer() throws IOException {
            return filer.getFilePointer();
        }

        @Override
        public void eof() throws IOException {
            filer.eof();
        }

        @Override
        public void flush() throws IOException {
            filer.flush();
        }

        @Override
        public int read() throws IOException {
            return filer.read();
        }

        @Override
        public int read(byte[] bytes) throws IOException {
            return filer.read(bytes);
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            return filer.read(bytes, offset, length);
        }

        @Override
        public void write(int i) throws IOException {
            filer.write(i);
        }

        @Override
        public void write(byte[] bytes) throws IOException {
            filer.write(bytes);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            filer.write(bytes, offset, length);
        }

        @Override
        public void close() throws IOException {
            filer.close();
        }
    }
}
