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
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.UUID;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class FileBackedMapChunkFactory implements MapChunkFactory<ByteBufferBackedFiler> {

    private final int keySize;
    private final boolean variableKeySizes;
    private final int payloadSize;
    private final boolean variablePayloadSizes;
    private final int initialPageCapacity;
    private final String[] pathsToPartitions;

    public FileBackedMapChunkFactory(int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes,
        int initialPageCapacity,
        String[] pathsToPartitions) {

        this.keySize = keySize;
        this.variableKeySizes = variableKeySizes;
        this.payloadSize = payloadSize;
        this.variablePayloadSizes = variablePayloadSizes;
        this.initialPageCapacity = initialPageCapacity;
        this.pathsToPartitions = pathsToPartitions;
    }

    @Override
    public MapChunk<ByteBufferBackedFiler> get(MapStore mapStore, String pageId) throws IOException {
        File file = createIndexSetFile(pageId);
        return (file.exists()) ? mmap(mapStore, file, initialPageCapacity) : null;
    }

    @Override
    public MapChunk<ByteBufferBackedFiler> getOrCreate(MapStore mapStore, String pageId) throws IOException {

        File file = createIndexSetFile(pageId);
        if (!file.exists()) {
            // initializing in a temporary file prevents accidental corruption if the thread dies during mmap
            File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
            mmap(mapStore, temporaryNewKeyIndexPartition, initialPageCapacity);

            File createIndexSetFile = createIndexSetFile(pageId);
            FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
            FileUtils.forceDelete(temporaryNewKeyIndexPartition);
        }

        return mmap(mapStore, file, initialPageCapacity);

    }

    @Override
    public MapChunk<ByteBufferBackedFiler> resize(MapStore mapStore, MapChunk chunk, String pageId, int newSize, MapStore.CopyToStream copyToStream)
        throws IOException {

        File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
        MapChunk newIndex = mmap(mapStore, temporaryNewKeyIndexPartition, newSize);
        mapStore.copyTo(chunk, newIndex, copyToStream);
        // TODO: implement to clean up
        //index.close();
        //newIndex.close();
        File createIndexSetFile = createIndexSetFile(pageId);
        FileUtils.forceDelete(createIndexSetFile);
        FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
        FileUtils.forceDelete(temporaryNewKeyIndexPartition);

        return mmap(mapStore, createIndexSetFile, newSize);
    }

    @Override
    public MapChunk<ByteBufferBackedFiler> copy(MapStore mapStore, MapChunk chunk, String pageId, int newSize) throws IOException {

        File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
        int filerSize = mapStore.computeFilerSize(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
        ByteBufferBackedFiler newFiler = getPageProvider(temporaryNewKeyIndexPartition).allocate(filerSize);
        chunk.filer.seek(0);
        newFiler.seek(0);
        FilerIO.copy(chunk.filer, newFiler, -1); // TODO add copy to onto Filer interface
        File createIndexSetFile = createIndexSetFile(pageId);
        FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
        FileUtils.forceDelete(temporaryNewKeyIndexPartition);

        return mmap(mapStore, createIndexSetFile, newSize);
    }

    @Override
    public void delete(String pageId) throws IOException {
        File file = createIndexSetFile(pageId);
        if (!file.delete()) {
            if (file.exists()) {
                throw new RuntimeException("Failed to delete existing chunk file: " + file.getAbsolutePath());
            }
        }
    }

    private FileBackedMemMappedByteBufferFactory getPageFactory(File file) {
        return new FileBackedMemMappedByteBufferFactory(file.getParentFile());
    }

    private ConcurrentFilerProvider<ByteBufferBackedFiler> getPageProvider(File file) {
        return new ConcurrentFilerProvider<>(file.getName().getBytes(Charsets.UTF_8), new ByteBufferBackedConcurrentFilerFactory(getPageFactory(file)));
    }

    private MapChunk<ByteBufferBackedFiler> mmap(MapStore mapStore, final File file, int maxCapacity) throws IOException {
        if (file.exists()) {
            MappedByteBuffer buffer = getPageFactory(file).open(file.getName());
            ByteBufferBackedFiler filer = new ByteBufferBackedFiler(file, buffer);
            MapChunk<ByteBufferBackedFiler> page = new MapChunk<>(filer);
            page.init(mapStore);
            return page;
        } else {
            ConcurrentFilerProvider<ByteBufferBackedFiler> concurrentFilerProvider = getPageProvider(file);
            int filerSize = mapStore.computeFilerSize(maxCapacity, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
            ByteBufferBackedFiler filer = concurrentFilerProvider.allocate(filerSize);
            return mapStore.bootstrapAllocatedFiler(maxCapacity,
                keySize,
                variableKeySizes,
                payloadSize,
                variablePayloadSizes,
                filer);
        }
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
