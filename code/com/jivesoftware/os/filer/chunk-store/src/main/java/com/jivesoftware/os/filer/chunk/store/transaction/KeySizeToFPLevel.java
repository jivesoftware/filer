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
public class KeySizeToFPLevel implements LevelProvider<KeyedFP<Integer>, Integer, MapContextFiler> {

    private final ChunkStore chunkStore;
    private final boolean variableSizedKeys;

    public KeySizeToFPLevel(ChunkStore chunkStore, boolean variableSizedKeys) {
        this.chunkStore = chunkStore;
        this.variableSizedKeys = variableSizedKeys;
    }

    @Override
    public <R> R acquire(final KeyedFP<Integer> parentStore,
        final Integer key,
        final StoreTransaction<R, MapContextFiler> storeTransaction) throws IOException {
        return MapTransactionUtil.INSTANCE.write(chunkStore, key, variableSizedKeys, 8, false, 1, parentStore, key, storeTransaction);

    }

    @Override
    public void release(KeyedFP<Integer> parentStore, Integer keySize, MapContextFiler store) throws IOException {
    }

}
