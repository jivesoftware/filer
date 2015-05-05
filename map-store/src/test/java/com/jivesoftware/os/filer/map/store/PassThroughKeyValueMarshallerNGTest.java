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
package com.jivesoftware.os.filer.map.store;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class PassThroughKeyValueMarshallerNGTest {

    public PassThroughKeyValueMarshallerNGTest() {
    }

    @Test
    public void testKeyBytes() {
        Assert.assertEquals(new byte[]{1, 2, 3, 4}, PassThroughKeyValueMarshaller.INSTANCE.keyBytes(new byte[]{1, 2, 3, 4}));
    }

    @Test
    public void testBytesKey() {
        Assert.assertEquals(new byte[]{1, 2, 3, 4}, PassThroughKeyValueMarshaller.INSTANCE.bytesKey(new byte[]{1, 2, 3, 4}, 0));
    }

    @Test
    public void testValueBytes() {
        Assert.assertEquals(new byte[]{1, 2, 3, 4}, PassThroughKeyValueMarshaller.INSTANCE.valueBytes(new byte[]{1, 2, 3, 4}));
    }

    @Test
    public void testBytesValue() {
        Assert.assertEquals(new byte[]{1, 2, 3, 4}, PassThroughKeyValueMarshaller.INSTANCE.bytesValue(new byte[]{1, 2, 3, 4}, new byte[]{1, 2, 3, 4}, 0));
    }

}
