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
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class MapContextFiler implements KeyedFP<byte[]> {

    MapContext mapContext;
    ChunkFiler filer;

    public MapContextFiler(MapContext mapContext, ChunkFiler filer) {
        this.mapContext = mapContext;
        this.filer = filer;
    }

    @Override
    public long getFP(byte[] key) throws IOException {
        long ai = MapStore.INSTANCE.get(filer, mapContext, key);
        if (ai < 0) {
            return -1;
        }
        return FilerIO.bytesLong(MapStore.INSTANCE.getPayload(filer, mapContext, ai));
    }

    @Override
    public void update(byte[] key, long fp) throws IOException {
        MapStore.INSTANCE.add(filer, mapContext, (byte) 1, key, FilerIO.longBytes(fp));
    }

}
