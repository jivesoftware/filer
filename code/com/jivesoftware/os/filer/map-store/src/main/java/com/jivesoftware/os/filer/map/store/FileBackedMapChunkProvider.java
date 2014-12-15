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
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.MonkeyFilerTransaction;
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

    private final int keySize;
    private final boolean variableKeySizes;
    private final int payloadSize;
    private final boolean variablePayloadSizes;
    private final int initialPageCapacity;
    private final String[] pathsToPartitions;
    private final String[] pageIds;
    private final ConcurrentHashMap<String, MapContext<ByteBufferBackedFiler>> pageChunks = new ConcurrentHashMap<>();

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
            MapContext<ByteBufferBackedFiler> chunk = getMapChunk(mapStore, pageId, false);
            return chunkTransaction.commit(chunk);
        }
    }

    @Override
    public <R> R getOrCreate(byte[] pageKey, MapTransaction<ByteBufferBackedFiler, R> chunkTransaction) throws IOException {
        String pageId = pageId(pageKey);
        synchronized (pageId) {
            MapContext<ByteBufferBackedFiler> chunk = getMapChunk(mapStore, pageId, true);
            return chunkTransaction.commit(chunk);
        }
    }

    @Override
    public <R> R grow(byte[] pageKey, MapStore mapStore, MapContext<ByteBufferBackedFiler> oldChunk, int newSize, MapStore.CopyToStream copyToStream,
        MapTransaction<ByteBufferBackedFiler, R> chunkTransaction) throws IOException {

        String pageId = pageId(pageKey);
        synchronized (pageId) {
            File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
            MapContext<ByteBufferBackedFiler> newChunk = mmap(mapStore, temporaryNewKeyIndexPartition, newSize);
            mapStore.copyTo(oldChunk, newChunk, copyToStream);
            File createIndexSetFile = createIndexSetFile(pageId);
            FileUtils.forceDelete(createIndexSetFile);
            FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
            FileUtils.forceDelete(temporaryNewKeyIndexPartition);

            newChunk =  mmap(mapStore, createIndexSetFile, newSize);
            pageChunks.put(pageId, newChunk);

            return chunkTransaction.commit(newChunk);
        }
    }

    @Override
    public <R> R copy(byte[] pageKey,
        final MapStore mapStore,
        final MapContext<ByteBufferBackedFiler> oldChunk,
        final MapTransaction<ByteBufferBackedFiler, R> chunkTransaction)
        throws IOException {

        final String pageId = pageId(pageKey);
        synchronized (pageId) {
            final File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
            final int newSize = oldChunk.capacity;
            int filerSize = mapStore.computeFilerSize(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
            return getPageProvider(temporaryNewKeyIndexPartition).getOrAllocate(filerSize, openFiler, createFiler, new MonkeyFilerTransaction<ByteBufferBackedFiler, R>() {
                @Override
                public R commit(ByteBufferBackedFiler newFiler, boolean isNew) throws IOException {
                    oldChunk.filer.seek(0);
                    newFiler.seek(0);
                    FilerIO.copy(oldChunk.filer, newFiler, -1); // TODO add copy to onto Filer interface
                    File createIndexSetFile = createIndexSetFile(pageId);
                    FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
                    FileUtils.forceDelete(temporaryNewKeyIndexPartition);

                    MapContext<ByteBufferBackedFiler> newChunk = mmap(mapStore, createIndexSetFile, newSize);
                    pageChunks.put(pageId, newChunk);

                    return chunkTransaction.commit(newChunk);
                }
            });
        }
    }

    @Override
    public void stream(MapStore mapStore, MapTransaction<ByteBufferBackedFiler, Void> chunkTransaction) throws IOException {
        for (String pageId : pageIds) {
            synchronized (pageId) {
                MapContext<ByteBufferBackedFiler> chunk = getMapChunk(mapStore, pageId, false);
                chunkTransaction.commit(chunk);
            }
        }

        chunkTransaction.commit(null);
    }

    private MapContext<ByteBufferBackedFiler> getMapChunk(MapStore mapStore, String pageId,  boolean createIfAbsent) throws IOException {
        MapContext<ByteBufferBackedFiler> chunk = pageChunks.get(pageId);
        if (chunk == null) {
            File file = createIndexSetFile(pageId);
            if (file.exists()) {
                chunk = mmap(mapStore, file, initialPageCapacity);
                pageChunks.put(pageId, chunk);
            } else if (createIfAbsent) {
                // initializing in a temporary file prevents accidental corruption if the thread dies during mmap
                File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
                mmap(mapStore, temporaryNewKeyIndexPartition, initialPageCapacity);

                File createIndexSetFile = createIndexSetFile(pageId);
                FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
                FileUtils.forceDelete(temporaryNewKeyIndexPartition);
                chunk = mmap(mapStore, file, initialPageCapacity);
                pageChunks.put(pageId, chunk);
            }
        }
        return chunk;
    }

    @Override
    public <F2 extends ConcurrentFiler> void copyTo(final MapStore mapStore, MapChunkProvider<F2> toProvider) throws IOException {
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

    private MapContext<ByteBufferBackedFiler> mmap(final MapStore mapStore, final File file, final int maxCapacity) throws IOException {
        if (file.exists()) {
            MappedByteBuffer buffer = getPageFactory(file).open(file.getName());
            ByteBufferBackedFiler filer = new ByteBufferBackedFiler(file, buffer);
            MapContext<ByteBufferBackedFiler> page = new MapContext<>(filer);
            page.init(mapStore);
            return page;
        } else {
            ConcurrentFilerProvider<ByteBufferBackedFiler> concurrentFilerProvider = getPageProvider(file);
            int filerSize = mapStore.computeFilerSize(maxCapacity, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
            return concurrentFilerProvider.getOrAllocate(filerSize, openFiler, createFiler, new MonkeyFilerTransaction<ByteBufferBackedFiler, MapContext<ByteBufferBackedFiler>>() {
                @Override
                public MapContext<ByteBufferBackedFiler> commit(ByteBufferBackedFiler filer, boolean isNew) throws IOException {
                    MapContext<ByteBufferBackedFiler> page = new MapContext<>(filer);
                    mapStore.create(maxCapacity, keySize, variableKeySizes, payloadSize, variablePayloadSizes, filer);
                    page.init(mapStore);
                    return page;
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

}
