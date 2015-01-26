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
package com.jivesoftware.os.filer.map.store.api;

import com.google.common.primitives.UnsignedBytes;
import java.util.Comparator;

/**
 *
 * @author jonathan.colt
 */
public class KeyRange {
    static final Comparator<byte[]> lexicographicalComparator = UnsignedBytes.lexicographicalComparator();
    private final byte[] startInclusiveKey;
    private final byte[] stopExclusiveKey;

    public KeyRange(byte[] startInclusiveKey, byte[] stopExclusiveKey) {
        this.startInclusiveKey = startInclusiveKey;
        this.stopExclusiveKey = stopExclusiveKey;
    }

    public byte[] getStartInclusiveKey() {
        return startInclusiveKey;
    }

    public byte[] getStopExclusiveKey() {
        return stopExclusiveKey;
    }

    public boolean contains(byte[] key) {
        int compare = lexicographicalComparator.compare(startInclusiveKey, key);
        if (compare <= 0) {
            return lexicographicalComparator.compare(key, stopExclusiveKey) < 0;
        }
        return false;
    }

}
