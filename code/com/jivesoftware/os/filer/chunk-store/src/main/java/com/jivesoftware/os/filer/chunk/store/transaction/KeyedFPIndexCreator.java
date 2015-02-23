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

import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class KeyedFPIndexCreator implements CreateFiler<Void, PowerKeyedFPIndex, ChunkFiler> {

    public static final long DEFAULT_MAGIC_HEADER = 5583112375L;
    public static final int DEFAULT_MAX_KEY_SIZE_POWER = 16;
    public static KeyedFPIndexCreator DEFAULT = new KeyedFPIndexCreator(DEFAULT_MAGIC_HEADER, DEFAULT_MAX_KEY_SIZE_POWER);

    private final long magicHeader;
    private final int skyHookMaxKeySizePower;

    public KeyedFPIndexCreator(long magicHeader, int skyHookMaxKeySizePower) {
        this.magicHeader = magicHeader;
        this.skyHookMaxKeySizePower = skyHookMaxKeySizePower;
    }

    @Override
    public PowerKeyedFPIndex create(Void hint, ChunkFiler filer) throws IOException {
        filer.seek(0);
        FilerIO.writeLong(filer, magicHeader, "magicHeader");
        for (int i = 0; i < skyHookMaxKeySizePower; i++) {
            FilerIO.writeLong(filer, -1, "powerFP");
        }
        long[] fps = new long[skyHookMaxKeySizePower];
        Arrays.fill(fps, -1L);
        return new PowerKeyedFPIndex(filer.getChunkStore(), filer.getChunkFP(), fps);
    }

    @Override
    public long sizeInBytes(Void hint) {
        return 8 + (8 * skyHookMaxKeySizePower);
    }
}
