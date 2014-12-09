package com.jivesoftware.os.filer.keyed.store;

import com.google.common.collect.Iterators;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class VariableKeySizeMapChunkBackedKeyedStore<F extends ConcurrentFiler> implements KeyedFilerStore, Iterable<KeyValueStore.Entry<IBA, SwappableFiler>> {

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
    public SwappableFiler get(byte[] keyBytes, long newFilerInitialCapacity) throws Exception {
        return get(keyBytes).store.get(keyBytes, newFilerInitialCapacity);
    }

    @Override
    public void close() {
        for (PartitionSizedByKeyStore<F> keyedStore : keyedStores) {
            keyedStore.store.close();
        }
    }

    public void copyTo(VariableKeySizeMapChunkBackedKeyedStore<F> to) throws Exception {
        for (int i = 0; i < keyedStores.length; i++) {
            keyedStores[i].store.copyTo(to.keyedStores[i].store);
        }
    }

    @Override
    public Iterator<KeyValueStore.Entry<IBA, SwappableFiler>> iterator() {
        List<Iterator<KeyValueStore.Entry<IBA, SwappableFiler>>> iterators = new ArrayList<>();
        for (PartitionSizedByKeyStore<F> keyStore : keyedStores) {
            iterators.add(keyStore.store.iterator());
        }
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<IBA> keysIterator() {
        List<Iterator<IBA>> iterators = new ArrayList<>();
        for (PartitionSizedByKeyStore<F> keyStore : keyedStores) {
            iterators.add(keyStore.store.keysIterator());
        }
        return Iterators.concat(iterators.iterator());
    }

    public static class Builder<F extends ConcurrentFiler> {

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

    private static class PartitionSizedByKeyStore<F extends ConcurrentFiler> implements Comparable<PartitionSizedByKeyStore<F>> {

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
