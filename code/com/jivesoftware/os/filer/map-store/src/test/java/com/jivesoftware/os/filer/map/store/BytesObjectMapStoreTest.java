package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ByteBufferBackedConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;
import java.nio.file.Files;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class BytesObjectMapStoreTest {

    @Test(expectedExceptions = UnsupportedOperationException.class, description = "Out of scope")
    public void testCopyTo_bufferToFile() throws Exception {
        @SuppressWarnings("unchecked")
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> from =
            new BytesObjectMapStore<>(
                4,
                null,
                new ConcurrentFilerProviderBackedMapChunkProvider<ByteBufferBackedFiler>(
                    4, false, 0, false, 8,
                    new ConcurrentFilerProvider[] {
                        new ConcurrentFilerProvider<>(
                            "booya".getBytes(Charsets.UTF_8),
                            new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory()))
                    }),
                PassThroughKeyMarshaller.INSTANCE);
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> to =
            new BytesObjectMapStore<>(
                4,
                null,
                new FileBackedMapChunkProvider(
                    4, false, 0, false, 8,
                    new String[] {
                        Files.createTempDirectory("copy").toFile().getAbsolutePath(),
                        Files.createTempDirectory("copy").toFile().getAbsolutePath()
                    },
                    1),
                PassThroughKeyMarshaller.INSTANCE);

        assertCopyTo(from, to);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, description = "Out of scope")
    public void testCopyTo_fileToFile() throws Exception {
        @SuppressWarnings("unchecked")
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> from =
            new BytesObjectMapStore<>(
                4,
                null,
                new FileBackedMapChunkProvider(4, false, 0, false, 8,
                    new String[] {
                        Files.createTempDirectory("copy").toFile().getAbsolutePath(),
                        Files.createTempDirectory("copy").toFile().getAbsolutePath()
                    },
                    1),
                PassThroughKeyMarshaller.INSTANCE);
        @SuppressWarnings("unchecked")
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> to =
            new BytesObjectMapStore<>(
                4,
                null,
                new FileBackedMapChunkProvider(4, false, 0, false, 8,
                    new String[] {
                        Files.createTempDirectory("copy").toFile().getAbsolutePath(),
                        Files.createTempDirectory("copy").toFile().getAbsolutePath()
                    },
                    1),
                PassThroughKeyMarshaller.INSTANCE);

        assertCopyTo(from, to);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, description = "Out of scope")
    public void testCopyTo_bufferToBuffer() throws Exception {
        @SuppressWarnings("unchecked")
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> from =
            new BytesObjectMapStore<>(
                4,
                null,
                new ConcurrentFilerProviderBackedMapChunkProvider<ByteBufferBackedFiler>(4, false, 0, false, 8,
                    new ConcurrentFilerProvider[] {
                        new ConcurrentFilerProvider<>(
                            "booya".getBytes(Charsets.UTF_8),
                            new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory()))
                    }),
                PassThroughKeyMarshaller.INSTANCE);
        @SuppressWarnings("unchecked")
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> to =
            new BytesObjectMapStore<>(
                4,
                null,
                new ConcurrentFilerProviderBackedMapChunkProvider<ByteBufferBackedFiler>(4, false, 0, false, 8,
                    new ConcurrentFilerProvider[] {
                        new ConcurrentFilerProvider<>(
                            "booya".getBytes(Charsets.UTF_8),
                            new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory()))
                    }),
                PassThroughKeyMarshaller.INSTANCE);

        assertCopyTo(from, to);
    }

    private void assertCopyTo(
        BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> from,
        final BytesObjectMapStore<ByteBufferBackedFiler, byte[], Object> to)
        throws Exception {

        final int numEntries = 100;
        Object[] objects = new Object[numEntries * 2];
        for (int i = 0; i < numEntries * 2; i++) {
            objects[i] = new Object();
        }

        for (int i = 0; i < numEntries; i++) {
            final Object value = objects[i];
            from.execute(FilerIO.intBytes(i), true, new KeyValueTransaction<Object, Void>() {
                @Override
                public Void commit(KeyValueContext<Object> context) throws IOException {
                    context.set(value);
                    return null;
                }
            });
        }

        for (int i = 0; i < numEntries; i++) {
            final Object expected = objects[i];
            from.execute(FilerIO.intBytes(i), false, new KeyValueTransaction<Object, Void>() {
                @Override
                public Void commit(KeyValueContext<Object> context) throws IOException {
                    assertSame(context.get(), expected);
                    return null;
                }
            });
        }

        //TODO this is terrible copying
        from.stream(new KeyValueStore.EntryStream<byte[], Object>() {
            @Override
            public boolean stream(byte[] key, final Object value) throws IOException {
                to.execute(key, true, new KeyValueTransaction<Object, Void>() {
                    @Override
                    public Void commit(KeyValueContext<Object> context) throws IOException {
                        context.set(value);
                        return null;
                    }
                });
                return true;
            }
        });

        for (int i = 0; i < numEntries; i++) {
            final Object expected = objects[i];
            to.execute(FilerIO.intBytes(i), false, new KeyValueTransaction<Object, Void>() {
                @Override
                public Void commit(KeyValueContext<Object> context) throws IOException {
                    assertSame(context.get(), expected);
                    return null;
                }
            });
        }

        for (int i = 0; i < numEntries; i++) {
            final Object value = objects[numEntries + i];
            to.execute(FilerIO.intBytes(i), true, new KeyValueTransaction<Object, Void>() {
                @Override
                public Void commit(KeyValueContext<Object> context) throws IOException {
                    context.set(value);
                    return null;
                }
            });
        }

        for (int i = 0; i < numEntries; i++) {
            final Object expected = objects[numEntries + i];
            to.execute(FilerIO.intBytes(numEntries + i), false, new KeyValueTransaction<Object, Void>() {
                @Override
                public Void commit(KeyValueContext<Object> context) throws IOException {
                    assertSame(context.get(), expected);
                    return null;
                }
            });
            from.execute(FilerIO.intBytes(numEntries + i), false, new KeyValueTransaction<Object, Void>() {
                @Override
                public Void commit(KeyValueContext<Object> context) throws IOException {
                    assertNull(context.get());
                    return null;
                }
            });
        }
    }
}