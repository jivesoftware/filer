package com.jivesoftware.os.filer.keyed.store;

import com.google.common.collect.Iterators;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class VariableKeySizeMapChunkBackedKeyedStore implements KeyedFilerStore, Iterable<KeyValueStore.Entry<IBA, SwappableFiler>> {

    private final PartitionSizedByKeyStore[] keyedStores;

    private VariableKeySizeMapChunkBackedKeyedStore(PartitionSizedByKeyStore[] keyedStores)
        throws Exception {
        this.keyedStores = keyedStores;
    }

    private PartitionSizedByKeyStore get(byte[] key) {

        for (PartitionSizedByKeyStore keyedStore : keyedStores) {
            if (keyedStore.keySize >= key.length) {
                return keyedStore;
            }
        }
        throw new IndexOutOfBoundsException("Key is too long");
    }

    private byte[] pad(byte[] key) {
        int keySize = get(key).keySize;
        byte[] padded = new byte[keySize];
        System.arraycopy(key, 0, padded, 0, key.length);
        return padded;
    }

    @Override
    public SwappableFiler get(byte[] keyBytes, long newFilerInitialCapacity) throws Exception {
        return get(keyBytes).store.get(pad(keyBytes), newFilerInitialCapacity);
    }

    @Override
    public long sizeInBytes() throws Exception {
        long sizeInBytes = 0;
        for (PartitionSizedByKeyStore keyedStore : keyedStores) {
            sizeInBytes += keyedStore.store.mapStoreSizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public void close() {
        for (PartitionSizedByKeyStore keyedStore : keyedStores) {
            keyedStore.store.close();
        }
    }

    public void copyTo(VariableKeySizeMapChunkBackedKeyedStore to) throws Exception {
        for (int i = 0; i < keyedStores.length; i++) {
            keyedStores[i].store.copyTo(to.keyedStores[i].store);
        }
    }

    @Override
    public Iterator<KeyValueStore.Entry<IBA, SwappableFiler>> iterator() {
        List<Iterator<KeyValueStore.Entry<IBA, SwappableFiler>>> iterators = new ArrayList<>();
        for (PartitionSizedByKeyStore keyStore : keyedStores) {
            iterators.add(keyStore.store.iterator());
        }
        return Iterators.concat(iterators.iterator());
    }

    public static class Builder {

        private final List<PartitionSizedByKeyStore> stores = new ArrayList<>();

        public Builder add(int keySize, PartitionedMapChunkBackedKeyedStore partitionedMapChunkBackedKeyedStore) {
            stores.add(new PartitionSizedByKeyStore(keySize, partitionedMapChunkBackedKeyedStore));
            return this;
        }

        public VariableKeySizeMapChunkBackedKeyedStore build() throws Exception {
            Collections.sort(stores);
            return new VariableKeySizeMapChunkBackedKeyedStore(stores.toArray(new PartitionSizedByKeyStore[stores.size()]));

        }

    }

    private static class PartitionSizedByKeyStore implements Comparable<PartitionSizedByKeyStore> {

        final int keySize;
        final PartitionedMapChunkBackedKeyedStore store;

        public PartitionSizedByKeyStore(int keySize, PartitionedMapChunkBackedKeyedStore store) {
            this.keySize = keySize;
            this.store = store;
        }

        @Override
        public int compareTo(PartitionSizedByKeyStore o) {
            return Integer.compare(keySize, o.keySize);
        }

    }
}
