package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ByteBufferBackedConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import java.nio.file.Files;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class BytesObjectMapStoreTest {

    @Test
    public void testCopyTo_bufferToFile() throws Exception {
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> from = new BytesObjectMapStore<>("4",
            4,
            null,
            new ConcurrentFilerProviderBackedMapChunkFactory<>(4, false, 0, false, 8,
                new ConcurrentFilerProvider<>("booya".getBytes(Charsets.UTF_8), new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory()))),
            PassThroughKeyMarshaller.INSTANCE);
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> to = new BytesObjectMapStore<>("4",
            4,
            null,
            new FileBackedMapChunkFactory(4, false, 0, false, 8, new String[] {
                Files.createTempDirectory("copy").toFile().getAbsolutePath(),
                Files.createTempDirectory("copy").toFile().getAbsolutePath()
            }),
            PassThroughKeyMarshaller.INSTANCE);

        assertCopyTo(from, to);
    }

    @Test
    public void testCopyTo_fileToFile() throws Exception {
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> from = new BytesObjectMapStore<>("4",
            4,
            null,
            new FileBackedMapChunkFactory(4, false, 0, false, 8, new String[] {
                Files.createTempDirectory("copy").toFile().getAbsolutePath(),
                Files.createTempDirectory("copy").toFile().getAbsolutePath()
            }),
            PassThroughKeyMarshaller.INSTANCE);
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> to = new BytesObjectMapStore<>("4",
            4,
            null,
            new FileBackedMapChunkFactory(4, false, 0, false, 8, new String[] {
                Files.createTempDirectory("copy").toFile().getAbsolutePath(),
                Files.createTempDirectory("copy").toFile().getAbsolutePath()
            }),
            PassThroughKeyMarshaller.INSTANCE);

        assertCopyTo(from, to);
    }

    @Test
    public void testCopyTo_bufferToBuffer() throws Exception {
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> from = new BytesObjectMapStore<>("4",
            4,
            null,
            new ConcurrentFilerProviderBackedMapChunkFactory<>(4, false, 0, false, 8,
                new ConcurrentFilerProvider<>("booya".getBytes(Charsets.UTF_8), new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory()))),
            PassThroughKeyMarshaller.INSTANCE);
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> to = new BytesObjectMapStore<>("4",
            4,
            null,
            new ConcurrentFilerProviderBackedMapChunkFactory<>(4, false, 0, false, 8,
                new ConcurrentFilerProvider<>("booya".getBytes(Charsets.UTF_8), new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory()))),
            PassThroughKeyMarshaller.INSTANCE);

        assertCopyTo(from, to);
    }

    private void assertCopyTo(BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> from, BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> to)
        throws Exception {

        final int numEntries = 100;
        Object[] objects = new Object[numEntries * 2];
        for (int i = 0; i < numEntries * 2; i++) {
            objects[i] = new Object();
        }

        for (int i = 0; i < numEntries; i++) {
            from.add(FilerIO.intBytes(i), objects[i]);
        }

        for (int i = 0; i < numEntries; i++) {
            assertEquals(from.get(FilerIO.intBytes(i)), objects[i]);
        }

        from.copyTo(to);

        for (int i = 0; i < numEntries; i++) {
            assertEquals(to.get(FilerIO.intBytes(i)), objects[i]);
        }

        for (int i = 0; i < numEntries; i++) {
            to.add(FilerIO.intBytes(numEntries + i), objects[numEntries + i]);
        }

        for (int i = 0; i < numEntries; i++) {
            assertEquals(to.get(FilerIO.intBytes(numEntries + i)), objects[numEntries + i]);
        }
    }
}