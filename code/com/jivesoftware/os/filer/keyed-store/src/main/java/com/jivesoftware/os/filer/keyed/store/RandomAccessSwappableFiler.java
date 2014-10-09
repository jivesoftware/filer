package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.io.RandomAccessFiler;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class RandomAccessSwappableFiler implements SwappableFiler {

    private final File path;
    private final AtomicReference<RandomAccessFiler> filerReference;
    private final Random r = new Random();

    public RandomAccessSwappableFiler(File path) throws IOException {
        this.path = path;
        this.filerReference = new AtomicReference<>(new RandomAccessFiler(path, "rw"));
    }

    @Override
    public SwappingFiler swap(long initialChunkSize) throws Exception {
        File swapFile;
        do {
            swapFile = new File(path.getParentFile(), path.getName() + "." + r.nextInt());
        }
        while (swapFile.exists());

        return new RandomAccessSwappingFiler(swapFile);
    }

    @Override
    public void sync() throws IOException {
        filerReference.get().close();
        filerReference.set(new RandomAccessFiler(path, "rw"));
    }

    @Override
    public Object lock() {
        return filerReference;
    }

    @Override
    public void seek(long position) throws IOException {
        filerReference.get().seek(position);
    }

    @Override
    public long skip(long position) throws IOException {
        return filerReference.get().skip(position);
    }

    @Override
    public long length() throws IOException {
        return filerReference.get().length();
    }

    @Override
    public void setLength(long len) throws IOException {
        filerReference.get().setLength(len);
    }

    @Override
    public long getFilePointer() throws IOException {
        return filerReference.get().getFilePointer();
    }

    @Override
    public void eof() throws IOException {
        filerReference.get().eof();
    }

    @Override
    public void flush() throws IOException {
        filerReference.get().flush();
    }

    @Override
    public int read() throws IOException {
        return filerReference.get().read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return filerReference.get().read(b);
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        return filerReference.get().read(b, _offset, _len);
    }

    @Override
    public void write(int b) throws IOException {
        filerReference.get().write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        filerReference.get().write(b);
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        filerReference.get().write(b, _offset, _len);
    }

    @Override
    public void close() throws IOException {
        filerReference.get().close();
    }

    public class RandomAccessSwappingFiler implements SwappingFiler {

        private final File swapFile;
        private final RandomAccessFiler filer;

        public RandomAccessSwappingFiler(File swapFile) throws IOException {
            this.swapFile = swapFile;
            this.filer = new RandomAccessFiler(swapFile, "rw");
        }

        @Override
        public void commit() throws Exception {
            RandomAccessFiler filer = filerReference.get();
            filer.close();
            if (!path.delete()) {
                throw new RuntimeException("Could not delete old filer");
            }
            if (!swapFile.renameTo(path)) {
                throw new RuntimeException("Could not rename swap filer");
            }

            RandomAccessFiler swapFiler = new RandomAccessFiler(path, "rw");
            filerReference.set(swapFiler);
        }

        @Override
        public Object lock() {
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
