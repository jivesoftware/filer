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
import com.jivesoftware.os.filer.io.FilerLock;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

/**
 *
 * @author jonathan.colt
 */
public class ChunkLocks {

    private static final byte[] NAME = new byte[0];

    private final ChunkLockMap locks;
    private final ArrayBlockingQueue<FilerLock> reusableLocks = new ArrayBlockingQueue<>(1024);

    public ChunkLocks(ByteBufferFactory bufferFactory) {
        this.locks = new ChunkLockMap(NAME, bufferFactory);
    }

    public FilerLock acquire(long chunkFp) throws IOException {
        synchronized (locks) {
            FilerLock lock = locks.get(chunkFp);
            if (lock == null) {
                lock = reusableLocks.poll();
                if (lock == null) {
                    lock = new FilerLock();
                }
                locks.set(chunkFp, lock);

            }
            lock.incrementAndGet();
            return lock;
        }
    }

    public void release(long chunkFp) throws IOException {
        synchronized (locks) {
            FilerLock lock = locks.get(chunkFp);
            if (lock == null) {
                throw new IllegalStateException("The should be impossible. You should figure out how this happened.");
            } else {
                int got = lock.decrementAndGet();
                if (got == 0) {
                    locks.remove(chunkFp);
                }
                reusableLocks.offer(lock);
            }
        }
    }

    static class ChunkLockMap {

        private static final byte[] EMPTY = new byte[0];
        private final byte[] name;
        private final ByteBufferFactory bufferFactory;

        private MapContext mapContext;
        private ByteBufferBackedFiler mapFiler;
        private FilerLock[] locks;

        public ChunkLockMap(byte[] name, ByteBufferFactory bufferFactory) {
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

        public <M> void set(long chunkFP, FilerLock lock) throws IOException {
            ensureCapacity();
            int ai = MapStore.INSTANCE.add(mapFiler, mapContext, (byte) 1, FilerIO.longBytes(chunkFP), EMPTY);
            locks[ai] = lock;
        }

        public boolean contains(long chunkFP) throws IOException {
            ensureCapacity();
            long ai = MapStore.INSTANCE.get(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
            return ai > -1;
        }

        public FilerLock get(long chunkFP) throws IOException {
            ensureCapacity();
            long ai = MapStore.INSTANCE.get(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
            if (ai == -1) {
                return null;
            }
            return locks[(int) ai];

        }

        public void remove(long chunkFP) throws IOException {
            ensureCapacity();
            long ai = MapStore.INSTANCE.remove(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
            if (ai > -1) {
                locks[(int) ai] = null;
            }
        }

        void ensureCapacity() throws IOException {
            if (mapContext == null) {
                int size = MapStore.INSTANCE.computeFilerSize(2, 8, false, 0, false);
                mapFiler = new ByteBufferBackedFiler(bufferFactory.allocate(name, size));
                mapContext = MapStore.INSTANCE.create(2, 8, false, 0, false, mapFiler);
                locks = new FilerLock[mapContext.capacity];
            } else {
                if (MapStore.INSTANCE.isFull(mapContext)) {
                    int nextGrowSize = MapStore.INSTANCE.nextGrowSize(mapContext);
                    int newSize = MapStore.INSTANCE.computeFilerSize(nextGrowSize, 8, false, 0, false);
                    ByteBufferBackedFiler newMapFiler = new ByteBufferBackedFiler(bufferFactory.allocate(name, newSize));
                    MapContext newMapContext = MapStore.INSTANCE.create(nextGrowSize, 8, false, 0, false, newMapFiler);
                    final FilerLock[] newLocks = new FilerLock[newMapContext.capacity];
                    MapStore.INSTANCE.copyTo(mapFiler, mapContext, newMapFiler, newMapContext, new MapStore.CopyToStream() {
                        @Override
                        public void copied(int fromIndex, int toIndex) {
                            newLocks[toIndex] = locks[fromIndex];
                        }
                    });
                    mapFiler = newMapFiler;
                    mapContext = newMapContext;
                    locks = newLocks;

                }
            }
        }
    }
}
