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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class TransactionBuilder<C> {

    private final List keys;
    private final List<LevelProvider> levelProviders;

    public TransactionBuilder() {
        this.keys = null;
        this.levelProviders = null;
    }

    public TransactionBuilder(List keys, List<LevelProvider> levelProviders) {
        this.keys = keys;
        this.levelProviders = levelProviders;
    }

    public <B, P, S, L> TransactionBuilder<C> add(L key, LevelProvider<B, P, S, L, C> levelProvider) {
        List copyOfKeys = keys == null ? new ArrayList<>() : new ArrayList<>(keys);
        List copyOfLevels = levelProviders == null ? new ArrayList<>() : new ArrayList<>(levelProviders);
        copyOfKeys.add(key);
        copyOfLevels.add(levelProvider);
        return new TransactionBuilder<>(copyOfKeys, copyOfLevels);
    }

    public <B, R, S> R commit(B backingStorage, StoreTransaction<R, S, C> transaction) throws IOException {
        return new TransactionStore<>(backingStorage).commit(keys, levelProviders, transaction);
    }
}
