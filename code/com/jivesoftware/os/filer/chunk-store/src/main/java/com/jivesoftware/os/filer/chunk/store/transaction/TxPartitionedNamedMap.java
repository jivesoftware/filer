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
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class TxPartitionedNamedMap {

    private final TxNamedMap[] namedMaps;
    private final PartitionFunction<byte[]> partitionFunction;

    public TxPartitionedNamedMap(TxNamedMap[] namedMaps, PartitionFunction<byte[]> partitionFunction) {
        this.namedMaps = namedMaps;
        this.partitionFunction = partitionFunction;
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
        for (final TxNamedMap namedMap : namedMaps) {
            boolean result = namedMap.stream(mapName, stream);
            if (!result) {
                break;
            }
        }
        return true;
    }
}
