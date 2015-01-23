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
import java.io.IOException;

/**
 *
 * @author jonathan
 */
public class LongSkipListComparator implements SkipListComparator {

    static final public LongSkipListComparator cSingleton = new LongSkipListComparator();

    @Override
    public int compare(Filer a, int astart, Filer b, int bstart, int length) throws IOException {

        a.seek(astart);
        long al = FilerIO.readLong(a, "a");
        b.seek(bstart);
        long bl = FilerIO.readLong(b, "b");

        return (al < bl ? -1 : (al == bl ? 0 : 1));

    }

    @Override
    public long range(byte[] a, byte[] b) {
        long al = FilerIO.bytesLong(a);
        long bl = FilerIO.bytesLong(b);
        return Math.max(al, bl) - Math.min(al, bl);
    }

}
