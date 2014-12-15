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
package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public interface MapChunkProvider<F extends Filer> {

    <R> R get(byte[] pageKey, MapTransaction<F, R> chunkTransaction) throws IOException;

    <R> R getOrCreate(byte[] pageKey, MapTransaction<F, R> chunkTransaction) throws IOException;

    <R> R grow(byte[] pageKey, MapStore mapStore, MapContext chunk, int newSize, MapStore.CopyToStream copyToStream,
        MapTransaction<F, R> chunkTransaction) throws IOException;

    <R> R copy(byte[] pageKey, MapStore mapStore, MapContext chunk, MapTransaction<F, R> chunkTransaction) throws IOException;

    void delete(byte[] pageKey) throws IOException;

    void stream(MapStore mapStore, MapTransaction<F, Boolean> chunkTransaction) throws IOException;

    <F2 extends ConcurrentFiler> void copyTo(MapStore mapStore, MapChunkProvider<F2> toProvider) throws IOException;

}
