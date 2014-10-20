package com.jivesoftware.os.filer.map.store.api;

import com.jivesoftware.os.filer.map.store.api.KeyValueStore.Entry;

/**
 *
 * @author jonathan.colt
 * @param <K>
 * @param <V>
 */
public interface KeyValueStore<K, V> extends Iterable<Entry<K, V>> {

    void add(K key, V value) throws KeyValueStoreException;

    void remove(K key) throws KeyValueStoreException;

    V get(K key) throws KeyValueStoreException;

    V getUnsafe(K key) throws KeyValueStoreException;

    long estimateSizeInBytes() throws Exception;

    interface Entry<K, V> {

        K getKey();

        V getValue();
    }
}
