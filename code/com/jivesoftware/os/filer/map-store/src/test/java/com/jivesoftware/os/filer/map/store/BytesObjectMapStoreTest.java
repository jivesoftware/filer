package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import java.nio.file.Files;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class BytesObjectMapStoreTest {

    @Test
    public void testCopyTo_bufferToFile() throws Exception {
        BytesObjectMapStore<byte[], Object> from = new BytesObjectMapStore<>("4",
            4,
            null,
            new ByteBufferProviderBackedMapChunkFactory(4, false, 0, false, 8, new ByteBufferProvider("booya", new HeapByteBufferFactory())),
            PassThroughKeyMarshaller.INSTANCE);
        BytesObjectMapStore<byte[], Object> to = new BytesObjectMapStore<>("4",
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
        BytesObjectMapStore<byte[], Object> from = new BytesObjectMapStore<>("4",
            4,
            null,
            new FileBackedMapChunkFactory(4, false, 0, false, 8, new String[] {
                Files.createTempDirectory("copy").toFile().getAbsolutePath(),
                Files.createTempDirectory("copy").toFile().getAbsolutePath()
            }),
            PassThroughKeyMarshaller.INSTANCE);
        BytesObjectMapStore<byte[], Object> to = new BytesObjectMapStore<>("4",
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
        BytesObjectMapStore<byte[], Object> from = new BytesObjectMapStore<>("4",
            4,
            null,
            new ByteBufferProviderBackedMapChunkFactory(4, false, 0, false, 8, new ByteBufferProvider("booya", new HeapByteBufferFactory())),
            PassThroughKeyMarshaller.INSTANCE);
        BytesObjectMapStore<byte[], Object> to = new BytesObjectMapStore<>("4",
            4,
            null,
            new ByteBufferProviderBackedMapChunkFactory(4, false, 0, false, 8, new ByteBufferProvider("booya", new HeapByteBufferFactory())),
            PassThroughKeyMarshaller.INSTANCE);

        assertCopyTo(from, to);
    }

    private void assertCopyTo(BytesObjectMapStore<byte[], Object> from, BytesObjectMapStore<byte[], Object> to) throws Exception {
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