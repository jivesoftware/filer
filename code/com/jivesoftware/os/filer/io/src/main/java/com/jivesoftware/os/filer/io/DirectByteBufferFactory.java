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
package com.jivesoftware.os.filer.io;

import java.nio.ByteBuffer;

/**
 * @author jonathan.colt
 */
public class DirectByteBufferFactory implements ByteBufferFactory {

    @Override
    public ByteBuffer allocate(byte[] key, long _size) {
        return ByteBuffer.allocateDirect((int) _size);
    }

    @Override
    public ByteBuffer reallocate(byte[] key, ByteBuffer oldBuffer, long newSize) {
        ByteBuffer newBuffer = allocate(key, newSize);
        if (oldBuffer != null) {
            oldBuffer.position(0);
            newBuffer.put(oldBuffer); // this assume we only grow. Blame Kevin :)
            newBuffer.position(0);
        }
        return newBuffer;
    }

    @Override
    public boolean exists(byte[] key) {
        return false;
    }
}
