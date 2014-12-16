/*
 * Copyright 2014 jonathan.
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

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;

/**
 *
 * @author jonathan
 */
public class LongSkipListComparator implements SkipListComparator {

    static final public LongSkipListComparator cSingleton = new LongSkipListComparator();

    @Override
    public int compare(Filer a, int astart, Filer b, int bstart, int length) {
        try {
            long al = MapStore.INSTANCE.readLong(a, astart);
            long bl = MapStore.INSTANCE.readLong(b, bstart);
            return (al < bl ? -1 : (al == bl ? 0 : 1));
        } catch (Exception x) {
            throw new RuntimeException("failed reading long while trying to compare.", x);
        }

    }

    @Override
    public long range(byte[] a, byte[] b) {
        long al = FilerIO.bytesLong(a);
        long bl = FilerIO.bytesLong(b);
        return Math.max(al, bl) - Math.min(al, bl);
    }

}
