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

import com.jivesoftware.os.filer.io.FilerLock;
import com.jivesoftware.os.filer.io.OpenFiler;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class OpenChunkFilerStripedLock implements OpenFiler<FilerLock, ChunkFiler> {

    private final FPStripingLocksProvider filerLocks;

    public OpenChunkFilerStripedLock(FPStripingLocksProvider filerLocks) {
        this.filerLocks = filerLocks;
    }

    @Override
    public FilerLock open(ChunkFiler filer) throws IOException {
        return filerLocks.lock(filer.getChunkFP());
    }
}
