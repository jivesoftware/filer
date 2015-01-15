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
package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class ChunkCache {

    private final byte[] name;
    private final ByteBufferFactory bufferFactory;

    private MapContext mapContext;
    private ByteBufferBackedFiler mapFiler;
    private Object[] monkeys;

    public ChunkCache(byte[] name, ByteBufferFactory bufferFactory) {
        this.name = name;
        this.bufferFactory = bufferFactory;
    }

    public long approxSize() throws IOException {
        MapContext _mapContext = mapContext;
        if (_mapContext == null) {
            return 0;
        }
        return MapStore.INSTANCE.getApproxCount(mapContext);
    }

    public <M> void set(long chunkFP, long startOfFP, long endOfFP, M monkey) throws IOException {
        ensureCapacity();
        int ai = MapStore.INSTANCE.add(mapFiler, mapContext, (byte) 1, FilerIO.longBytes(chunkFP), FilerIO.longsBytes(new long[] { startOfFP, endOfFP }));
        monkeys[ai] = monkey;
    }

    public boolean contains(long chunkFP) throws IOException {
        ensureCapacity();
        long ai = MapStore.INSTANCE.get(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
        return ai > -1;
    }

    public <M> Chunk<M> get(long chunkFP) throws IOException {
        ensureCapacity();
        long ai = MapStore.INSTANCE.get(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
        if (ai == -1) {
            return null;
        }
        long[] startAndEnd = FilerIO.bytesLongs(MapStore.INSTANCE.getPayload(mapFiler, mapContext, ai));
        return new Chunk<>((M) monkeys[(int) ai], chunkFP, startAndEnd[0], startAndEnd[1]);

    }

    public void remove(long chunkFP) throws IOException {
        ensureCapacity();
        long ai = MapStore.INSTANCE.remove(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
        if (ai > -1) {
            monkeys[(int) ai] = null;
        }
    }

    void ensureCapacity() throws IOException {
        if (mapContext == null) {
            int size = MapStore.INSTANCE.computeFilerSize(2, 8, false, 16, false);
            mapFiler = new ByteBufferBackedFiler(bufferFactory.allocate(name, size));
            mapContext = MapStore.INSTANCE.create(2, 8, false, 16, false, mapFiler);
            monkeys = new Object[mapContext.capacity];
        } else {
            if (MapStore.INSTANCE.isFull(mapContext)) {
                int nextGrowSize = MapStore.INSTANCE.nextGrowSize(mapContext);
                int newSize = MapStore.INSTANCE.computeFilerSize(nextGrowSize, 8, false, 16, false);
                ByteBufferBackedFiler newMapFiler = new ByteBufferBackedFiler(bufferFactory.allocate(name, newSize));
                MapContext newMapContext = MapStore.INSTANCE.create(nextGrowSize, 8, false, 16, false, newMapFiler);
                final Object[] newMonkeys = new Object[newMapContext.capacity];
                MapStore.INSTANCE.copyTo(mapFiler, mapContext, newMapFiler, newMapContext, new MapStore.CopyToStream() {
                    @Override
                    public void copied(int fromIndex, int toIndex) {
                        newMonkeys[toIndex] = monkeys[fromIndex];
                    }
                });
                mapFiler = newMapFiler;
                mapContext = newMapContext;
                monkeys = newMonkeys;

            }
        }
    }

}
