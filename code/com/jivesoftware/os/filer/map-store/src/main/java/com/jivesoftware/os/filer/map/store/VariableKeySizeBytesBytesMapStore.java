package com.jivesoftware.os.filer.map.store;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.Iterator;
import java.util.List;

public abstract class VariableKeySizeBytesBytesMapStore<K, V> implements KeyValueStore<K, V> {

    private final BytesBytesMapStore<K, V>[] mapStores;

    @SuppressWarnings("unchecked")
    public VariableKeySizeBytesBytesMapStore(BytesBytesMapStore<K, V>[] mapStores) {
        this.mapStores = mapStores;

        /*
        for (int i = 0; i < keySizeThresholds.length; i++) {
            Preconditions.checkArgument(i == 0 || keySizeThresholds[i] > keySizeThresholds[i - 1], "Thresholds must be monotonically increasing");

            final int keySize = keySizeThresholds[i];
            mapStores[i] = new BytesBytesMapStore<>(String.valueOf(keySize), returnWhenGetReturnsNull, mapChunkFactory, keyValueMarshaller);
        }
        */
    }

    private BytesBytesMapStore<K, V> getMapStore(int keyLength) {
        for (int i = 0; i < mapStores.length; i++) {
            if (mapStores[i].keySize >= keyLength) {
                return mapStores[i];
            }
        }
        throw new IndexOutOfBoundsException("Key is too long");
    }

    protected abstract int keyLength(K key);

    @Override
    public void add(K key, V value) throws Exception {
        getMapStore(keyLength(key)).add(key, value);
    }

    @Override
    public void remove(K key) throws Exception {
        getMapStore(keyLength(key)).remove(key);
    }

    @Override
    public V get(K key) throws Exception {
        return getMapStore(keyLength(key)).get(key);
    }

    @Override
    public V getUnsafe(K key) throws Exception {
        return getMapStore(keyLength(key)).getUnsafe(key);
    }

    @Override
    public long estimateSizeInBytes() throws Exception {
        long estimate = 0;
        for (BytesBytesMapStore<K, V> mapStore : mapStores) {
            estimate += mapStore.estimateSizeInBytes();
        }
        return estimate;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayListWithCapacity(mapStores.length);
        for (BytesBytesMapStore<K, V> mapStore : mapStores) {
            iterators.add(mapStore.iterator());
        }
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<K> keysIterator() {
        List<Iterator<K>> iterators = Lists.newArrayListWithCapacity(mapStores.length);
        for (BytesBytesMapStore<K, V> mapStore : mapStores) {
            iterators.add(mapStore.keysIterator());
        }
        return Iterators.concat(iterators.iterator());
    }
}
