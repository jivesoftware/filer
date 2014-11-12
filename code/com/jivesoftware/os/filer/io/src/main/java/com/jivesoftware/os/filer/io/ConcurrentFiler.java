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

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface ConcurrentFiler extends Filer {

    /**
     *
     * When a filer is backed by memory it is possible to concurrently modify non overlapping chunks of memory however when a filer is backed by disk you have
     * only a single seek head. This method can return itself or a pointer to itself that is in effect like having multiple seek heads.
     *
     * @param suggestedLock
     * @return @throws IOException
     */
    Filer asConcurrentReadWrite(Object suggestedLock) throws IOException;

    long capacity();

    void delete() throws Exception;
}
