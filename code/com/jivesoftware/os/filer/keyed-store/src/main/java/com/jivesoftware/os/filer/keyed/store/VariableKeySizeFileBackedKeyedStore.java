package com.jivesoftware.os.filer.keyed.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VariableKeySizeFileBackedKeyedStore implements KeyedFilerStore {


    public static class Builder {

        private final List<ParitionSizedByKeyStore> stores = new ArrayList<>();

        public Builder add(int keySize, FileBackedKeyedStore fileBackedKeyedStore) {
            stores.add(new ParitionSizedByKeyStore(keySize, fileBackedKeyedStore));
            return this;
        }

        public VariableKeySizeFileBackedKeyedStore build() throws Exception {
            Collections.sort(stores);
            return new VariableKeySizeFileBackedKeyedStore(stores.toArray(new ParitionSizedByKeyStore[stores.size()]));

        }

    }

    private static class ParitionSizedByKeyStore implements Comparable<ParitionSizedByKeyStore> {

        final int keySize;
        final FileBackedKeyedStore store;

        public ParitionSizedByKeyStore(int keySize, FileBackedKeyedStore store) {
            this.keySize = keySize;
            this.store = store;
        }

        @Override
        public int compareTo(ParitionSizedByKeyStore o) {
            return Integer.compare(keySize, o.keySize);
        }

    }

    private final ParitionSizedByKeyStore[] keyedStores;

    private VariableKeySizeFileBackedKeyedStore(ParitionSizedByKeyStore[] keyedStores)
            throws Exception {
        this.keyedStores = keyedStores;
    }

    private ParitionSizedByKeyStore get(byte[] key) {

        for (ParitionSizedByKeyStore keyedStore : keyedStores) {
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
        for (ParitionSizedByKeyStore keyedStore : keyedStores) {
            sizeInBytes += keyedStore.store.mapStoreSizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public void close() {
        for (ParitionSizedByKeyStore keyedStore : keyedStores) {
            keyedStore.store.close();
        }
    }
}
