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

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.filer.io.map.SkipListComparator;
import java.io.IOException;
import java.util.Comparator;

/**
 *
 * @author jonathan
 */
public class LexSkipListComparator implements SkipListComparator {

    final static Comparator<byte[]> lexicographicalComparator = UnsignedBytes.lexicographicalComparator();

    static final public LexSkipListComparator cSingleton = new LexSkipListComparator();

    @Override
    public int compare(byte[] a, byte[] b) throws IOException {
        return lexicographicalComparator.compare(a, b);
    }

}
