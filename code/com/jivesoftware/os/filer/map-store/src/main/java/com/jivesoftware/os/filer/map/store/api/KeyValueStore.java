package com.jivesoftware.os.filer.map.store.api;

import com.jivesoftware.os.filer.map.store.api.KeyValueStore.Entry;
import java.util.Iterator;

/**
 *
 * @author jonathan.colt
 * @param <K>
 * @param <V>
 */
public interface KeyValueStore<K, V> extends Iterable<Entry<K, V>> {

    void add(K key, V value) throws Exception;

    void remove(K key) throws Exception;

    V get(K key) throws Exception;

    V getUnsafe(K key) throws Exception;

    long estimateSizeInBytes() throws Exception;

    Iterator<K> keysIterator();

    interface Entry<K, V> {

        K getKey();

        V getValue();
    }
}
