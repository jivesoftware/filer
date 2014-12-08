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
import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.UUID;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class FileBackedMapChunkFactory implements MapChunkFactory {

    private static final byte[] EMPTY_ID = new byte[16];

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
    public MapChunk get(MapStore mapStore, String pageId) throws Exception {
        File file = createIndexSetFile(pageId);
        return (file.exists()) ? mmap(mapStore, file, initialPageCapacity) : null;
    }

    @Override
    public MapChunk getOrCreate(MapStore mapStore, String pageId) throws Exception {

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
    public MapChunk resize(MapStore mapStore, MapChunk chunk, String pageId, int newSize, MapStore.CopyToStream copyToStream) throws Exception {

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
    public MapChunk copy(MapStore mapStore, MapChunk chunk, String pageId, int newSize) throws Exception {

        File temporaryNewKeyIndexPartition = createIndexTempFile(pageId);
        ConcurrentFiler newFiler = mapStore.allocateFiler(newSize,
            keySize, variableKeySizes, payloadSize, variablePayloadSizes,
            getPageProvider(temporaryNewKeyIndexPartition));
        chunk.filer.seek(0);
        newFiler.seek(0);
        FilerIO.copy(chunk.filer, newFiler, -1); // TODO add copy to onto Filer interface
        File createIndexSetFile = createIndexSetFile(pageId);
        FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
        FileUtils.forceDelete(temporaryNewKeyIndexPartition);

        return mmap(mapStore, createIndexSetFile, newSize);
    }

    @Override
    public void delete(String pageId) throws Exception {
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

    private ConcurrentFilerProvider getPageProvider(File file) {
        return new ConcurrentFilerProvider(file.getName().getBytes(Charsets.UTF_8), new ByteBufferBackedConcurrentFilerFactory(getPageFactory(file)));
    }

    private MapChunk mmap(MapStore mapStore, final File file, int maxCapacity) throws Exception {
        if (file.exists()) {
            MappedByteBuffer buffer = getPageFactory(file).open(file.getName());
            ByteBufferBackedFiler filer = new ByteBufferBackedFiler(file, buffer);
            MapChunk page = new MapChunk(filer);
            page.init(mapStore);
            return page;
        } else {
            ConcurrentFilerProvider concurrentFilerProvider = getPageProvider(file);
            ConcurrentFiler filer = mapStore.allocateFiler(maxCapacity,
                keySize,
                variableKeySizes,
                payloadSize,
                variablePayloadSizes,
                concurrentFilerProvider);
            return mapStore.bootstrapAllocatedFiler((byte) 0,
                (byte) 0,
                EMPTY_ID,
                0,
                maxCapacity,
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
