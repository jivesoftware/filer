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
import com.jivesoftware.os.filer.chunk.store.RewriteChunkTransaction;
import com.jivesoftware.os.filer.io.PartitionFunction;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class TxPartitionedNamedMapOfFiler<M> {

    private final TxNamedMapOfFiler<M>[] stores;
    private final PartitionFunction<byte[]> partitionFunction;

    public TxPartitionedNamedMapOfFiler(PartitionFunction<byte[]> partitionFunction, TxNamedMapOfFiler<M>[] stores) {
        this.stores = stores;
        this.partitionFunction = partitionFunction;
    }

    public <R> R overwrite(byte[] partitionKey,
        byte[] mapName,
        byte[] filerKey,
        Long sizeHint,
        ChunkTransaction<M, R> filerTransaction) throws IOException {
        return stores[partitionFunction.partition(stores.length, partitionKey)].overwrite(mapName, filerKey, sizeHint, filerTransaction);
    }

    public <R> R rewrite(byte[] partitionKey,
        byte[] mapName,
        byte[] filerKey,
        Long sizeHint,
        RewriteChunkTransaction<M, R> rewriteChunkTransaction) throws IOException {
        return stores[partitionFunction.partition(stores.length, partitionKey)].rewrite(mapName, filerKey, sizeHint, rewriteChunkTransaction);
    }

    public <R> R read(byte[] partitionKey, final byte[] mapName, final byte[] filerKey, final ChunkTransaction<M, R> filerTransaction) throws IOException {
        return stores[partitionFunction.partition(stores.length, partitionKey)].read(mapName, filerKey, filerTransaction);
    }

    public Boolean stream(final byte[] mapName, final TxStream<byte[], M, ChunkFiler> stream) throws IOException {
        for (final TxNamedMapOfFiler<M> store : stores) {
            if (!store.stream(mapName, stream)) {
                return false;
            }
        }
        return true;
    }

    public Boolean streamKeys(final byte[] mapName, final TxStreamKeys<byte[]> stream) throws IOException {
        for (final TxNamedMapOfFiler<M> store : stores) {
            if (!store.streamKeys(mapName, stream)) {
                return false;
            }
        }
        return true;
    }
}
