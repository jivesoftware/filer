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
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class TransactionStore<B> {

    private final B backingStorage;

    public TransactionStore(B backingStorage) {
        this.backingStorage = backingStorage;
    }

    public <R, S> R commit(List keys,
        List<LevelProvider> levelProviders,
        StoreTransaction<R, S, ? extends Object> transaction) throws IOException {

        return exec(0, backingStorage, null, keys, levelProviders, transaction);
    }

    public <R, S, C> R exec(final int level,
        final Object backingStorage,
        final Object parent,
        final List keys,
        final List<LevelProvider> levelProviders,
        final StoreTransaction<R, S, C> transaction) throws IOException {

        final Object key = keys.get(level);
        final LevelProvider levelProvider = levelProviders.get(level);
        R r = (R) levelProvider.enter(backingStorage, parent, key, new StoreTransaction<R, S, C>() {

            @Override
            public R commit(S storage, C child) throws IOException {
                if (level + 1 == keys.size()) {
                    return transaction.commit(storage, child);
                } else {
                   return exec(level + 1, storage, child, keys, levelProviders, transaction); // recursion
                }
            }
        });
        return r;
    }

}
