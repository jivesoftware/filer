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
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class KeyedPayloadsReadLevel implements LevelProvider<KeyedFP<byte[]>, byte[], MapContextFiler> {

    private final ChunkStore chunkStore;

    public KeyedPayloadsReadLevel(ChunkStore chunkStore) {
        this.chunkStore = chunkStore;
    }

    @Override
    public <R> R acquire(final KeyedFP<byte[]> parentStore,
        final byte[] key,
        final StoreTransaction<R, MapContextFiler> storeTransaction) throws IOException {
        return MapTransactionUtil.INSTANCE.read(chunkStore, parentStore, key, storeTransaction);
    }

    @Override
    public void release(KeyedFP<byte[]> parentStore, byte[] keySize, MapContextFiler store) throws IOException {
    }

}
