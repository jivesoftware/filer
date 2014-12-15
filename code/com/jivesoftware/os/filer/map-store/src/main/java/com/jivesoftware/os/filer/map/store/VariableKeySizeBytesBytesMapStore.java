package com.jivesoftware.os.filer.map.store;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public abstract class VariableKeySizeBytesBytesMapStore<F extends ConcurrentFiler, K, V> implements KeyValueStore<K, V> {

    private final BytesBytesMapStore<F, K, V>[] mapStores;

    @SuppressWarnings("unchecked")
    public VariableKeySizeBytesBytesMapStore(BytesBytesMapStore<F, K, V>[] mapStores) {
        this.mapStores = mapStores;
    }

    private BytesBytesMapStore<F, K, V> getMapStore(int keyLength) {
        for (int i = 0; i < mapStores.length; i++) {
            if (mapStores[i].keySize >= keyLength) {
                return mapStores[i];
            }
        }
        throw new IndexOutOfBoundsException("Key is too long");
    }

    protected abstract int keyLength(K key);

    @Override
    public void add(K key, V value) throws IOException {
        getMapStore(keyLength(key)).add(key, value);
    }

    @Override
    public void remove(K key) throws IOException {
        getMapStore(keyLength(key)).remove(key);
    }

    @Override
    public V get(K key) throws IOException {
        return getMapStore(keyLength(key)).get(key);
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayListWithCapacity(mapStores.length);
        for (BytesBytesMapStore<F, K, V> mapStore : mapStores) {
            iterators.add(mapStore.iterator());
        }
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<K> keysIterator() {
        List<Iterator<K>> iterators = Lists.newArrayListWithCapacity(mapStores.length);
        for (BytesBytesMapStore<F, K, V> mapStore : mapStores) {
            iterators.add(mapStore.keysIterator());
        }
        return Iterators.concat(iterators.iterator());
    }
}
