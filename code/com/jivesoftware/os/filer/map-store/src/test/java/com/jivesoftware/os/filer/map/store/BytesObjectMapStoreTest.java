package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import java.nio.file.Files;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class BytesObjectMapStoreTest {

    @Test
    public void testCopyTo() throws Exception {
        BytesObjectMapStore<byte[], Object> from = new BytesObjectMapStore<>(4, false, 8, null,
            new ByteBufferProvider("booya", new HeapByteBufferFactory()),
            PassThroughKeyMarshaller.INSTANCE);
        BytesObjectMapStore<byte[], Object> to = new BytesObjectMapStore<>(4, false, 8, null,
            new ByteBufferProvider("yooba", new FileBackedMemMappedByteBufferFactory(Files.createTempDirectory("copy").toFile())),
            PassThroughKeyMarshaller.INSTANCE);

        final int numEntries = 100;
        Object[] objects = new Object[numEntries];
        for (int i = 0; i < numEntries; i++) {
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
    }
}