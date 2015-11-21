/*
 * Copyright 2015 Jive Software.
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

import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author jonathan.colt
 * @param <K>
 * @param <I>
 */
public interface FPIndex<K, I extends FPIndex<K, I>> {

    long get(K key, StackBuffer stackBuffer) throws IOException;

    void set(K key, long fp, StackBuffer stackBuffer) throws IOException;

    long getAndSet(K key, long fp, StackBuffer stackBuffer) throws IOException;

    boolean acquire(int alwaysRoomForNMoreKeys);

    int nextGrowSize(int alwaysRoomForNMoreKeys) throws IOException;

    void copyTo(Filer curentFiler, FPIndex<K, I> newMonkey, Filer newFiler, StackBuffer stackBuffer) throws IOException;

    void release(int alwayRoomForNMoreKeys);

    boolean stream(List<KeyRange> ranges, KeysStream<K> stream, StackBuffer stackBuffer) throws IOException;

    <H, M, R> R read(
        ChunkStore chunkStore,
        K key,
        OpenFiler<M, ChunkFiler> opener,
        ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException;

    <H, M, R> R writeNewReplace(
        ChunkStore chunkStore,
        K key,
        H hint,
        CreateFiler<H, M, ChunkFiler> creator,
        OpenFiler<M, ChunkFiler> opener,
        GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException;

    <H, M, R> R readWriteAutoGrow(
        ChunkStore chunkStore,
        K key,
        H hint,
        CreateFiler<H, M, ChunkFiler> creator,
        OpenFiler<M, ChunkFiler> opener,
        GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException;

}
