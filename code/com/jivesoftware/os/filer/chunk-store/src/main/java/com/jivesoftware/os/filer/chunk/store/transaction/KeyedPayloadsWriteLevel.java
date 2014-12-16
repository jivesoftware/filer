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
public class KeyedPayloadsWriteLevel implements LevelProvider<KeyedFP<byte[]>, byte[], MapContextFiler> {

    private final ChunkStore chunkStore;
    private final int keySize;
    private final boolean variableKeySize;
    private final int payloadSize;
    private final boolean variablePayloadSize;
    private final int alwaysRoomForNMoreKeys;

    public KeyedPayloadsWriteLevel(ChunkStore chunkStore,
        int keySize, boolean variableKeySize,
        int payloadSize,
        boolean variablePayloadSize,
        int alwaysRoomForNMoreKeys) {
        this.chunkStore = chunkStore;
        this.keySize = keySize;
        this.variableKeySize = variableKeySize;
        this.payloadSize = payloadSize;
        this.variablePayloadSize = variablePayloadSize;
        this.alwaysRoomForNMoreKeys = alwaysRoomForNMoreKeys;
    }

    @Override
    public <R> R acquire(KeyedFP<byte[]> parentStore,
        byte[] key,
        StoreTransaction<R, MapContextFiler> storeTransaction) throws IOException {

        return MapTransactionUtil.INSTANCE.write(chunkStore,
            keySize, variableKeySize,
            payloadSize, variablePayloadSize,
            alwaysRoomForNMoreKeys, parentStore, key, storeTransaction);

    }

    @Override
    public void release(KeyedFP<byte[]> parentStore, byte[] keySize, MapContextFiler store) throws IOException {
    }

}
