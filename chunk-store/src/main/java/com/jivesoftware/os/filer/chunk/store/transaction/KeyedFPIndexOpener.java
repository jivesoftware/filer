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

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class KeyedFPIndexOpener implements OpenFiler<PowerKeyedFPIndex, ChunkFiler> {

    private final int seed;
    private final long magicHeader; // = 5583112375L;
    private final int skyHookMaxKeySizePower; // = 16;
    private final IntIndexSemaphore keySemaphores;

    public KeyedFPIndexOpener(int seed, long magicHeader, int skyHookMaxKeySizePower, IntIndexSemaphore keySemaphores) {
        this.seed = seed;
        this.magicHeader = magicHeader;
        this.skyHookMaxKeySizePower = skyHookMaxKeySizePower;
        this.keySemaphores = keySemaphores;
    }

    @Override
    public PowerKeyedFPIndex open(ChunkFiler filer, StackBuffer stackBuffer) throws IOException {
        filer.seek(0);
        long magicNumber = FilerIO.readLong(filer, "magicHeader", stackBuffer);
        if (magicNumber != magicHeader) {
            throw new IllegalStateException("Expected magicHeader:" + magicHeader + " but found:" + magicNumber);
        }
        byte[] rawFPS = new byte[8 * skyHookMaxKeySizePower];
        FilerIO.read(filer, rawFPS);
        long[] fps = FilerIO.bytesLongs(rawFPS);
        return new PowerKeyedFPIndex(seed, filer.getChunkStore(), filer.getChunkFP(), fps, keySemaphores);
    }
}
