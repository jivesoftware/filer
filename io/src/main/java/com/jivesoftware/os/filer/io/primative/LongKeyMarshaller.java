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
package com.jivesoftware.os.filer.io.primative;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyMarshaller;

/**
 *
 * @author jonathan.colt
 */
public class LongKeyMarshaller implements KeyMarshaller<Long> {

    @Override
    public byte[] keyBytes(Long key) {
        return FilerIO.longBytes(key);
    }

    @Override
    public Long bytesKey(byte[] keyBytes, int offset) {
        return FilerIO.bytesLong(keyBytes, offset);
    }
}
