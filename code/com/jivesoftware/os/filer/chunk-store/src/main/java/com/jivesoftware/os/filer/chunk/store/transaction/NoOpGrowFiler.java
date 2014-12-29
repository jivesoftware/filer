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
package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class NoOpGrowFiler<H, M, F extends Filer> implements GrowFiler<H, M, F> {

    @Override
    public H grow(M monkey, F filer) throws IOException {
        return null;
    }

    @Override
    public void grow(M currentMonkey, F currentFiler, M newMonkey, F newFiler) throws IOException {
    }

}
