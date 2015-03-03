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
package com.jivesoftware.os.filer.io.chunk;

import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class StripedFiler {

    private final AutoGrowingByteBufferBackedFiler root;
    private final Object[] locks;
    private final AutoGrowingByteBufferBackedFiler[] stripes;
    private final byte[] name;
    private final ByteBufferFactory bufferFactory;
    private final ChunkCache[] stripesChunkCaches;

    public StripedFiler(AutoGrowingByteBufferBackedFiler root,
        byte[] name,
        ByteBufferFactory bufferFactory,
        int numberOfStripes) {
        this.root = root;
        this.name = name;
        this.bufferFactory = bufferFactory;
        this.locks = new Object[numberOfStripes];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
        this.stripes = new AutoGrowingByteBufferBackedFiler[numberOfStripes];
        this.stripesChunkCaches = new ChunkCache[numberOfStripes];
    }

    public long length() throws IOException {
        return root.length();
    }

    public <R> R rootTx(long fp, StripeTx<R> stripeTx) throws IOException {
        synchronized (root) {
            return stripeTx.tx(fp, null, root);
        }
    }

    private static final long multiplier = 0x5DEECE66DL;
    private static final long addend = 0xBL;
    private static final long mask = (1L << 48) - 1;

    static int hashFP(long fp) {
        long seed = (fp ^ multiplier) & mask;
        seed = (seed * multiplier + addend) & mask;
        return (int) (seed >>> (48 - 32));
    }

    public <R> R tx(long fp, StripeTx<R> stripeTx) throws IOException {
        int stripe = (int) Math.abs(hashFP(fp) % stripes.length);
        synchronized (locks[stripe]) {
            if (stripes[stripe] == null) {
                stripes[stripe] = root.duplicateAll();
                stripesChunkCaches[stripe] = new ChunkCache(join(name, FilerIO.intBytes(stripe)), bufferFactory);
            } else if (root.length() > stripes[stripe].length()) {
                stripes[stripe] = root.duplicateNew(stripes[stripe]);
            }
            return stripeTx.tx(fp, stripesChunkCaches[stripe], stripes[stripe]);
        }
    }

    public static interface StripeTx<R> {

        R tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler filer) throws IOException;
    }

    static byte[] join(byte[] array1, byte[] array2) {
        final byte[] joinedArray = new byte[array1.length + array2.length];
        System.arraycopy(array1, 0, joinedArray, 0, array1.length);
        System.arraycopy(array2, 0, joinedArray, array1.length, array2.length);
        return joinedArray;
    }

}
