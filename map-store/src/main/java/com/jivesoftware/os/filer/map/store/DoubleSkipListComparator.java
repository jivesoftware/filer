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
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.map.SkipListComparator;
import java.io.IOException;

/**
 *
 * @author jonathan
 */
public class DoubleSkipListComparator implements SkipListComparator {

    static final public DoubleSkipListComparator cSingleton = new DoubleSkipListComparator();

    public int compare(Filer a, int astart, Filer b, int bstart, int length, StackBuffer stackBuffer) throws IOException {
        a.seek(astart);
        double ad = FilerIO.readDouble(a, "a", stackBuffer);
        b.seek(bstart);
        double bd = FilerIO.readDouble(b, "b", stackBuffer);
        return Double.compare(ad, bd);
    }

    @Override
    public int compare(byte[] a, byte[] b) throws IOException {
        return Double.compare(FilerIO.byteDouble(a), FilerIO.byteDouble(b));
    }

}
