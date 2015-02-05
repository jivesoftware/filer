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
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.map.store.LexSkipListComparator;
import com.jivesoftware.os.filer.map.store.SkipListMapContext;
import com.jivesoftware.os.filer.map.store.SkipListMapStore;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author jonathan.colt
 */
public class SkipListMapBackedKeyedFPIndexCreator implements CreateFiler<Integer, SkipListMapBackedKeyedFPIndex, ChunkFiler> {

    private final int initialCapacity;
    private final int keySize;
    private final boolean variableKeySize;
    private final int payloadSize;
    private final SkipListMapBackedKeyedFPIndexOpener opener;
    private final LocksProvider<byte[]> keyLocks;
    private final SemaphoreProvider<byte[]> keySemaphores;

    public SkipListMapBackedKeyedFPIndexCreator(int initialCapacity,
        int keySize,
        boolean variableKeySize,
        int payloadSize,
        SkipListMapBackedKeyedFPIndexOpener opener,
        LocksProvider<byte[]> keyLocks,
        SemaphoreProvider<byte[]> keySemaphores) {

        this.initialCapacity = initialCapacity < 2 ? 2 : initialCapacity;
        this.keySize = keySize;
        this.variableKeySize = variableKeySize;
        this.payloadSize = payloadSize;
        this.opener = opener;
        this.keyLocks = keyLocks;
        this.keySemaphores = keySemaphores;
    }

    @Override
    public SkipListMapBackedKeyedFPIndex create(Integer hint, ChunkFiler filer) throws IOException {
        hint += initialCapacity;
        hint = hint < 2 ? 2 : hint;
        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext context = SkipListMapStore.INSTANCE.create(hint,
            headKey,
            keySize, variableKeySize, payloadSize,
            (byte) 9,
            LexSkipListComparator.cSingleton, filer);
        return new SkipListMapBackedKeyedFPIndex(filer.getChunkStore(), filer.getChunkFP(), context, opener, keyLocks, keySemaphores);
    }

    @Override
    public long sizeInBytes(Integer hint) throws IOException {
        hint += initialCapacity;
        hint = hint < 2 ? 2 : hint;
        return SkipListMapStore.INSTANCE.computeFilerSize(hint, keySize, variableKeySize, payloadSize, (byte) 9);
    }
}
