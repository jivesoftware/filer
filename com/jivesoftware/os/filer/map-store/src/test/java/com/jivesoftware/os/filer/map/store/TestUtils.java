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

import java.util.Random;

/**
 *
 * @author jonathan
 */
public class TestUtils {

    static byte[] randomLowerCaseAlphaBytes(Random random, int _length) {
        byte[] name = new byte[_length];
        fill(random, name, 0, _length, 97, 122); // 97 122 lowercase a to z ascii
        return name;
    }

    static void fill(Random random, byte[] _fill, int _offset, int _length, int _min, int _max) {
        for (int i = _offset; i < _offset + _length; i++) {
            _fill[i] = (byte) (_min + random.nextInt(_max - _min));
        }
    }

}
