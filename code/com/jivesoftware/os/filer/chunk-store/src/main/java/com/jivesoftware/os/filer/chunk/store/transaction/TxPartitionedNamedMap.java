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
package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkTransaction;
import com.jivesoftware.os.filer.io.PartitionFunction;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class TxPartitionedNamedMap {

    private final TxNamedMap[] namedMaps;
    private final PartitionFunction<byte[]> partitionFunction;

    public TxPartitionedNamedMap(PartitionFunction<byte[]> partitionFunction, TxNamedMap[] namedMaps) {
        this.namedMaps = namedMaps;
        this.partitionFunction = partitionFunction;
    }

    public boolean[] contains(byte[][] keysBytes, byte[] mapName) throws IOException {
        byte[][][] partitionedKeysBytes = new byte[namedMaps.length][keysBytes.length][];
        boolean[][] partitionedContains = new boolean[namedMaps.length][keysBytes.length];
        for (int i = 0; i < keysBytes.length; i++) {
            int p = partitionFunction.partition(namedMaps.length, keysBytes[i]);
            partitionedKeysBytes[p][i] = keysBytes[i];
        }

        for (int p = 0; p < namedMaps.length; p++) {
            final byte[][] currentKeysBytes = partitionedKeysBytes[p];
            final boolean[] currentContains = partitionedContains[p];
            namedMaps[p].read(mapName, new ChunkTransaction<MapContext, Void>() {
                @Override
                public Void commit(MapContext context, ChunkFiler filer, Object lock) throws IOException {
                    synchronized (lock) {
                        for (int i = 0; i < currentKeysBytes.length; i++) {
                            byte[] keyBytes = currentKeysBytes[i];
                            currentContains[i] = (keyBytes != null && MapStore.INSTANCE.contains(filer, context, keyBytes));
                        }
                    }
                    return null;
                }
            });
        }

        boolean[] result = new boolean[keysBytes.length];
        for (int i = 0; i < result.length; i++) {
            for (int p = 0; p < namedMaps.length; p++) {
                result[i] |= partitionedContains[p][i];
            }
        }
        return result;
    }

    public <R> R write(byte[] partitionKey, final byte[] mapName, final ChunkTransaction<MapContext, R> mapTransaction) throws IOException {
        int i = partitionFunction.partition(namedMaps.length, partitionKey);
        final TxNamedMap namedMap = namedMaps[i];
        return namedMap.write(mapName, mapTransaction);
    }

    public <R> R read(byte[] partitionKey, final byte[] mapName, final ChunkTransaction<MapContext, R> mapTransaction) throws IOException {
        int i = partitionFunction.partition(namedMaps.length, partitionKey);
        final TxNamedMap namedMap = namedMaps[i];
        return namedMap.read(mapName, mapTransaction);
    }

    public Boolean stream(final byte[] mapName, final TxStream<byte[], MapContext, ChunkFiler> stream) throws IOException {
        for (TxNamedMap namedMap : namedMaps) {
            boolean result = namedMap.stream(mapName, stream);
            if (!result) {
                return false;
            }
        }
        return true;
    }
}
