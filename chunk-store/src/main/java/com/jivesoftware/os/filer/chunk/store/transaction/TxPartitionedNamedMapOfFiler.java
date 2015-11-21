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

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.PartitionFunction;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import java.io.IOException;
import java.util.List;

/**
 * @author jonathan.colt
 * @param <N>
 * @param <H>
 * @param <M>
 */
public class TxPartitionedNamedMapOfFiler<N extends FPIndex<byte[], N>, H, M> {

    private final TxNamedMapOfFiler<N, H, M>[] stores;
    private final PartitionFunction<byte[]> partitionFunction;

    public TxPartitionedNamedMapOfFiler(PartitionFunction<byte[]> partitionFunction, TxNamedMapOfFiler<N, H, M>[] stores) {
        this.stores = stores;
        this.partitionFunction = partitionFunction;
    }

    public <R> R readWriteAutoGrow(byte[] partitionKey,
        byte[] mapName,
        byte[] filerKey,
        H sizeHint,
        ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException {
        return stores[partitionFunction.partition(stores.length, partitionKey)]
            .readWriteAutoGrow(mapName, filerKey, sizeHint, filerTransaction, stackBuffer);
    }

    public <R> R writeNewReplace(byte[] partitionKey,
        byte[] mapName,
        byte[] filerKey,
        H sizeHint,
        ChunkTransaction<M, R> chunkTransaction,
        StackBuffer stackBuffer) throws IOException {
        return stores[partitionFunction.partition(stores.length, partitionKey)].writeNewReplace(mapName, filerKey, sizeHint, chunkTransaction, stackBuffer);
    }

    public <R> R read(byte[] partitionKey, final byte[] mapName, final byte[] filerKey, final ChunkTransaction<M, R> filerTransaction, StackBuffer stackBuffer)
        throws IOException {
        return stores[partitionFunction.partition(stores.length, partitionKey)].read(mapName, filerKey, filerTransaction, stackBuffer);
    }

    public <R> List<R> readEach(byte[][] partitionKeys, byte[] mapName, byte[][] filerKeys, ChunkTransaction<M, R> filerTransaction, StackBuffer stackBuffer)
        throws IOException {
        byte[][][] partitionedFilerKeys = new byte[stores.length][][];
        for (int i = 0; i < partitionKeys.length; i++) {
            byte[] partitionKey = partitionKeys[i];
            byte[] filerKey = filerKeys[i];
            if (partitionKey != null && filerKey != null) {
                int p = partitionFunction.partition(stores.length, partitionKey);
                if (partitionedFilerKeys[p] == null) {
                    partitionedFilerKeys[p] = new byte[filerKeys.length][];
                }
                partitionedFilerKeys[p][i] = filerKey;
            }
        }
        List<R> result = Lists.newArrayList();
        for (int p = 0; p < stores.length; p++) {
            if (partitionedFilerKeys[p] != null) {
                result.addAll(stores[p].readEach(mapName, partitionedFilerKeys[p], filerTransaction, stackBuffer));
            }
        }
        return result;
    }

    public Boolean stream(final byte[] mapName, final List<KeyRange> ranges, final TxStream<byte[], M, ChunkFiler> stream, StackBuffer stackBuffer) throws
        IOException {
        for (final TxNamedMapOfFiler<N, H, M> store : stores) {
            if (!store.stream(mapName, ranges, stream, stackBuffer)) {
                return false;
            }
        }
        return true;
    }

    public Boolean streamKeys(final byte[] mapName, final List<KeyRange> ranges, final TxStreamKeys<byte[]> stream, StackBuffer stackBuffer) throws IOException {
        for (final TxNamedMapOfFiler<N, H, M> store : stores) {
            if (!store.streamKeys(mapName, ranges, stream, stackBuffer)) {
                return false;
            }
        }
        return true;
    }
}
