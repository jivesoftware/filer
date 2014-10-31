package com.jivesoftware.os.filer.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author jonathan.colt
 */
public class FileBackedMemMappedByteBufferFactory implements ByteBufferFactory {

    private final File directory;

    public FileBackedMemMappedByteBufferFactory(File directory) {
        this.directory = directory;
    }

    public MappedByteBuffer open(String key) {
        try {
            ensureDirectory(directory);
            File file = new File(directory, key);
            MappedByteBuffer buf;
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.seek(0);
                FileChannel channel = raf.getChannel();
                buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, (int) channel.size());
            }
            return buf;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer allocate(String key, long length) {
        try {
            ensureDirectory(directory);
            File file = new File(directory, key);
            MappedByteBuffer buf;
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(length);
            raf.write(0);
            raf.seek(0);
            FileChannel channel = raf.getChannel();
            return channel.map(FileChannel.MapMode.READ_WRITE, 0, (int) channel.size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer reallocate(String key, ByteBuffer oldBuffer, long newSize) {
        return allocate(key, newSize);
    }

    private void ensureDirectory(File directory) {
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                if (!directory.exists()) {
                    throw new RuntimeException("Failed to create directory: " + directory);
                }
            }
        }
    }
}
