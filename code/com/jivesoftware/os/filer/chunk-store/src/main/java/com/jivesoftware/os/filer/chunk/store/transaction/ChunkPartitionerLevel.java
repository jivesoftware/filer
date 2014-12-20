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

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.io.PartitionFunction;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class ChunkPartitionerLevel<K> implements LevelProvider<ChunkStore[], Void, ChunkStore, K, Void> {

    private final PartitionFunction<K> partitionFunction;

    public ChunkPartitionerLevel(PartitionFunction<K> partitionFunction) {
        this.partitionFunction = partitionFunction;
    }

    @Override
    public <R> R enter(ChunkStore[] backingStorage,
        Void parentLevel,
        K levelKey,
        StoreTransaction<R, ChunkStore, Void> storeTransaction) throws IOException {

        int i = partitionFunction.partition(backingStorage.length, levelKey);
        return storeTransaction.commit(backingStorage[i], null);
    }

}
