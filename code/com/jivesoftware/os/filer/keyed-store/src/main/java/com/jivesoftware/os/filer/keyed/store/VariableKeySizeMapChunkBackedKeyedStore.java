package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VariableKeySizeMapChunkBackedKeyedStore<F extends Filer> implements KeyedFilerStore {

    private final PartitionSizedByKeyStore<F>[] keyedStores;

    private VariableKeySizeMapChunkBackedKeyedStore(PartitionSizedByKeyStore<F>[] keyedStores)
        throws Exception {
        this.keyedStores = keyedStores;
    }

    private PartitionSizedByKeyStore<F> get(byte[] key) {

        for (PartitionSizedByKeyStore<F> keyedStore : keyedStores) {
            if (keyedStore.keySize >= key.length) {
                return keyedStore;
            }
        }
        throw new IndexOutOfBoundsException("Key is too long");
    }

    @Override
    public <R> R execute(byte[] keyBytes, long newFilerInitialCapacity, FilerTransaction<Filer, R> transaction) throws IOException {
        return get(keyBytes).store.execute(keyBytes, newFilerInitialCapacity, transaction);
    }

    @Override
    public <R> R executeRewrite(byte[] keyBytes, long newFilerInitialCapacity, RewriteFilerTransaction<Filer, R> transaction) throws IOException {
        return null;
    }

    @Override
    public void close() {
        for (PartitionSizedByKeyStore<F> keyedStore : keyedStores) {
            keyedStore.store.close();
        }
    }

    @Override
    public boolean stream(KeyValueStore.EntryStream<IBA, Filer> stream) throws IOException {
        for (PartitionSizedByKeyStore<F> keyedStore : keyedStores) {
            if (!keyedStore.store.stream(stream)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean streamKeys(KeyValueStore.KeyStream<IBA> stream) throws IOException {
        for (PartitionSizedByKeyStore<F> keyedStore : keyedStores) {
            if (!keyedStore.store.streamKeys(stream)) {
                return false;
            }
        }
        return true;
    }

    public static class Builder<F extends Filer> {

        private final List<PartitionSizedByKeyStore<F>> stores = new ArrayList<>();

        public Builder add(int keySize, PartitionedMapChunkBackedKeyedStore<F> partitionedMapChunkBackedKeyedStore) {
            stores.add(new PartitionSizedByKeyStore<>(keySize, partitionedMapChunkBackedKeyedStore));
            return this;
        }

        public VariableKeySizeMapChunkBackedKeyedStore<F> build() throws Exception {
            Collections.sort(stores);
            @SuppressWarnings("unchecked")
            PartitionSizedByKeyStore<F>[] storesArray = stores.toArray(new PartitionSizedByKeyStore[stores.size()]);
            return new VariableKeySizeMapChunkBackedKeyedStore<F>(storesArray);

        }

    }

    private static class PartitionSizedByKeyStore<F extends Filer> implements Comparable<PartitionSizedByKeyStore<F>> {

        final int keySize;
        final PartitionedMapChunkBackedKeyedStore<F> store;

        public PartitionSizedByKeyStore(int keySize, PartitionedMapChunkBackedKeyedStore<F> store) {
            this.keySize = keySize;
            this.store = store;
        }

        @Override
        public int compareTo(PartitionSizedByKeyStore<F> o) {
            return Integer.compare(keySize, o.keySize);
        }

    }
}
