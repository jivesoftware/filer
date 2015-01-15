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
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class MapBackedKeyedFPIndexGrower implements GrowFiler<Integer, MapBackedKeyedFPIndex, ChunkFiler> {

    private final int alwaysRoomForNMoreKeys;

    public MapBackedKeyedFPIndexGrower(int alwaysRoomForNMoreKeys) {
        this.alwaysRoomForNMoreKeys = alwaysRoomForNMoreKeys;
    }

    @Override
    public Integer acquire(MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
        synchronized (lock) {
            if (MapStore.INSTANCE.acquire(monkey.getMapContext(), alwaysRoomForNMoreKeys)) {
                // there is definitely room for N more
                return null;
            } else {
                // there might not be room for N more
                return MapStore.INSTANCE.nextGrowSize(monkey.getMapContext(), alwaysRoomForNMoreKeys);
            }
        }
    }

    @Override
    public void growAndAcquire(MapBackedKeyedFPIndex currentMonkey,
        ChunkFiler currentFiler,
        MapBackedKeyedFPIndex newMonkey,
        ChunkFiler newFiler,
        Object currentLock,
        Object newLock) throws IOException {

        synchronized (currentLock) {
            synchronized (newLock) {
                if (MapStore.INSTANCE.acquire(newMonkey.getMapContext(), alwaysRoomForNMoreKeys)) {
                    MapStore.INSTANCE.copyTo(currentFiler, currentMonkey.getMapContext(), newFiler, newMonkey.getMapContext(), null);
                } else {
                    throw new RuntimeException("Newly allocated MapBackedKeyedFPIndexGrower context does not have necessary capacity!");
                }
            }
        }
    }

    @Override
    public void release(MapBackedKeyedFPIndex monkey, Object lock) {
        synchronized (lock) {
            MapStore.INSTANCE.release(monkey.getMapContext(), alwaysRoomForNMoreKeys);
        }
    }
}
