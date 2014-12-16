/*
 * Copyright 2014 Jive Software.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ByteBufferBackedConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.MonkeyFilerTransaction;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class FileBackedMapChunkProvider implements MapChunkProvider<ByteBufferBackedFiler> {

    private static final MapStore mapStore = MapStore.INSTANCE;

    private final int keySize;
    private final boolean variableKeySizes;
    private final int payloadSize;
    private final boolean variablePayloadSizes;
    private final int initialPageCapacity;
    private final String[] pathsToPartitions;
    private final String[] pageIds;
    private final ConcurrentHashMap<String, MapChunk> pageChunks = new ConcurrentHashMap<>();

    private final NoOpOpenFiler<ByteBufferBackedFiler> openFiler = new NoOpOpenFiler<>();
    private final NoOpCreateFiler<ByteBufferBackedFiler> createFiler = new NoOpCreateFiler<>();

    public FileBackedMapChunkProvider(int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes,
        int initialPageCapacity,
        String[] pathsToPartitions,
        int numPages) {

        this.keySize = keySize;
        this.variableKeySizes = variableKeySizes;
        this.payloadSize = payloadSize;
        this.variablePayloadSizes = variablePayloadSizes;
        this.initialPageCapacity = initialPageCapacity;
        this.pathsToPartitions = pathsToPartitions;
        this.pageIds = new String[numPages];
        for (int i = 0; i < numPages; i++) {
            this.pageIds[i] = String.valueOf(i);
        }
    }

    @Override
    public <R> R get(byte[] pageKey, MapTransaction<ByteBufferBackedFiler, R> chunkTransaction) throws IOException {
        String pageId = pageId(pageKey);
        synchronized (pageId) {
            MapChunk chunk = getMapChunk(pageId, false);
            return chunkTransaction.commit(chunk.context, chunk.filer);
        }
    }

    @Override
    public <R> R getOrCreate(byte[] pageKey, MapTransaction<ByteBufferBackedFiler, R> chunkTransaction) throws IOException {
        String pageId = pageId(pageKey);
        synchronized (pageId) {
            MapChunk chunk = getMapChunk(pageId, true);
            return chunkTransaction.commit(chunk.context, chunk.filer);
        }
    }

    @Override
    public <R> R grow(byte[] pageKey,
        int newSize,
        MapStore.CopyToStream copyToStream,
        MapTransaction<ByteBufferBackedFiler, R> chunkTransaction)
        throws IOException {

        String pageId = pageId(pageKey);
        synchronized (pageId) {
            File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
            MapChunk oldChunk = getMapChunk(pageId, true);
            MapChunk newChunk = mmap(temporaryNewKeyIndexPartition, newSize);
            mapStore.copyTo(oldChunk.filer, oldChunk.context, newChunk.filer, newChunk.context, copyToStream);
            File createIndexSetFile = createIndexSetFile(pageId);
            FileUtils.forceDelete(createIndexSetFile);
            FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
            FileUtils.forceDelete(temporaryNewKeyIndexPartition);

            newChunk = mmap(createIndexSetFile, newSize);
            pageChunks.put(pageId, newChunk);

            return chunkTransaction.commit(newChunk.context, newChunk.filer);
        }
    }

    @Override
    public <R> R copy(byte[] pageKey,
        final MapContext fromContext,
        final ByteBufferBackedFiler fromFiler,
        final MapTransaction<ByteBufferBackedFiler, R> chunkTransaction)
        throws IOException {

        final String pageId = pageId(pageKey);
        synchronized (pageId) {
            final File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
            final int newSize = fromContext.capacity;
            int filerSize = mapStore.computeFilerSize(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
            return getPageProvider(temporaryNewKeyIndexPartition).getOrAllocate(filerSize, openFiler, createFiler,
                new MonkeyFilerTransaction<Void, ByteBufferBackedFiler, R>() {
                    @Override
                    public R commit(Void monkey, ByteBufferBackedFiler newFiler) throws IOException {
                        fromFiler.seek(0);
                        newFiler.seek(0);
                        FilerIO.copy(fromFiler, newFiler, -1); // TODO add copy to onto Filer interface
                        File createIndexSetFile = createIndexSetFile(pageId);
                        FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
                        FileUtils.forceDelete(temporaryNewKeyIndexPartition);

                        MapChunk newChunk = mmap(createIndexSetFile, newSize);
                        pageChunks.put(pageId, newChunk);

                        return chunkTransaction.commit(newChunk.context, newChunk.filer);
                    }
                });
        }
    }

    @Override
    public boolean stream(MapTransaction<ByteBufferBackedFiler, Boolean> chunkTransaction) throws IOException {
        for (String pageId : pageIds) {
            synchronized (pageId) {
                MapChunk chunk = getMapChunk(pageId, false);
                if (!chunkTransaction.commit(chunk.context, chunk.filer)) {
                    return false;
                }
            }
        }
        return true;
    }

    private MapChunk getMapChunk(String pageId, boolean createIfAbsent) throws IOException {
        MapChunk chunk = pageChunks.get(pageId);
        if (chunk == null) {
            File file = createIndexSetFile(pageId);
            if (file.exists()) {
                chunk = mmap(file, initialPageCapacity);
                pageChunks.put(pageId, chunk);
            } else if (createIfAbsent) {
                // initializing in a temporary file prevents accidental corruption if the thread dies during mmap
                File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
                mmap(temporaryNewKeyIndexPartition, initialPageCapacity);

                File createIndexSetFile = createIndexSetFile(pageId);
                FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
                FileUtils.forceDelete(temporaryNewKeyIndexPartition);
                chunk = mmap(file, initialPageCapacity);
                pageChunks.put(pageId, chunk);
            }
        }
        return chunk;
    }

    @Override
    public <F2 extends Filer> void copyTo(MapChunkProvider<F2> toProvider) throws IOException {
        throw new UnsupportedOperationException("Out of scope");
    }

    @Override
    public void delete(byte[] pageKey) throws IOException {
        String pageId = pageId(pageKey);
        synchronized (pageId) {
            File file = createIndexSetFile(pageId);
            if (!file.delete()) {
                if (file.exists()) {
                    throw new RuntimeException("Failed to delete existing chunk file: " + file.getAbsolutePath());
                }
            }
        }
    }

    private FileBackedMemMappedByteBufferFactory getPageFactory(File file) {
        return new FileBackedMemMappedByteBufferFactory(file.getParentFile());
    }

    private ConcurrentFilerProvider<ByteBufferBackedFiler> getPageProvider(File file) {
        return new ConcurrentFilerProvider<>(file.getName().getBytes(Charsets.UTF_8), new ByteBufferBackedConcurrentFilerFactory(getPageFactory(file)));
    }

    private MapChunk mmap(final File file, final int maxCapacity) throws IOException {
        if (file.exists()) {
            MappedByteBuffer buffer = getPageFactory(file).open(file.getName());
            ByteBufferBackedFiler filer = new ByteBufferBackedFiler(file, buffer);
            return new MapChunk(mapStore.open(filer), filer);
        } else {
            ConcurrentFilerProvider<ByteBufferBackedFiler> concurrentFilerProvider = getPageProvider(file);
            int filerSize = mapStore.computeFilerSize(maxCapacity, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
            return concurrentFilerProvider.getOrAllocate(filerSize, openFiler, createFiler,
                new MonkeyFilerTransaction<Void, ByteBufferBackedFiler, MapChunk>() {
                    @Override
                    public MapChunk commit(Void monkey, ByteBufferBackedFiler filer) throws IOException {
                        return new MapChunk(mapStore.create(initialPageCapacity, keySize, variableKeySizes, payloadSize, variablePayloadSizes, filer), filer);
                    }
                });
        }
    }

    private String pageId(byte[] pageKey) {
        return pageIds[Math.abs(Arrays.hashCode(pageKey)) % pageIds.length];
    }

    private File createIndexSetFile(String pageId) {
        return createIndexFilePostfixed(pageId, ".set");
    }

    private File createIndexTempFile(String pageId) {
        return createIndexFilePostfixed(pageId + "-" + UUID.randomUUID().toString(), ".tmp");
    }

    private File createIndexFilePostfixed(String partition, String postfix) {
        String pathToPartitions = pathsToPartitions[Math.abs(partition.hashCode()) % pathsToPartitions.length];
        String newIndexFilename = partition + (postfix == null ? "" : postfix);
        return new File(pathToPartitions, newIndexFilename);
    }

    private static class MapChunk {
        private final MapContext context;
        private final ByteBufferBackedFiler filer;

        private MapChunk(MapContext context, ByteBufferBackedFiler filer) {
            this.context = context;
            this.filer = filer;
        }
    }
}
