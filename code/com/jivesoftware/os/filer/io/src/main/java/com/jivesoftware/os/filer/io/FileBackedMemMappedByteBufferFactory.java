package com.jivesoftware.os.filer.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * TODO this implementation of ByteBufferFactory is inherently unsafe because its allocate() method is only capable of
 *      growing an existing buffer rather than handing out a new one. Eventually we need to extend ByteBufferFactory
 *      to formalize notions of create(), open(), copy(), resize().
 *
 * @author jonathan.colt
 */
public class FileBackedMemMappedByteBufferFactory implements ByteBufferFactory {

    private final File[] directories;

    public FileBackedMemMappedByteBufferFactory(File... directories) {
        this.directories = directories;
    }

    private File getDirectory(String key) {
        return directories[Math.abs(key.hashCode()) % directories.length];
    }

    public MappedByteBuffer open(String key) {
        try {
            //System.out.println(String.format("Open key=%s for directories=%s", key, Arrays.toString(directories)));
            File directory = getDirectory(key);
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
            //System.out.println(String.format("Allocate key=%s length=%s for directories=%s", key, length, Arrays.toString(directories)));
            File directory = getDirectory(key);
            ensureDirectory(directory);
            File file = new File(directory, key);
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
        //System.out.println(String.format("Reallocate key=%s newSize=%s for directories=%s", key, newSize, Arrays.toString(directories)));
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
