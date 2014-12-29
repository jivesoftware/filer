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
 *
 * @author jonathan.colt
 */
public class MapBackedKeyedFPIndexGrower implements GrowFiler<Integer, MapBackedKeyedFPIndex, ChunkFiler> {

    private final int alwaysRoomForNMoreKeys;

    public MapBackedKeyedFPIndexGrower(int alwaysRoomForNMoreKeys) {
        this.alwaysRoomForNMoreKeys = alwaysRoomForNMoreKeys;
    }

    @Override
    public Integer grow(MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
        synchronized (monkey) {
            if (MapStore.INSTANCE.isFullWithNMore(filer, monkey.getMapContext(), alwaysRoomForNMoreKeys)) {
                return MapStore.INSTANCE.nextGrowSize(monkey.getMapContext(), alwaysRoomForNMoreKeys);
            }
        }
        return null;
    }

    @Override
    public void grow(MapBackedKeyedFPIndex currentMonkey, ChunkFiler currentFiler, MapBackedKeyedFPIndex newMonkey, ChunkFiler newFiler) throws IOException {
        synchronized (currentMonkey) {
            synchronized (newMonkey) {
                MapStore.INSTANCE.copyTo(currentFiler, currentMonkey.getMapContext(), newFiler, newMonkey.getMapContext(), null);
            }
        }
    }
}
