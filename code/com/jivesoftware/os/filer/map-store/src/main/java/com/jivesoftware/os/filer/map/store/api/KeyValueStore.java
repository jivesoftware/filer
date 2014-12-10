package com.jivesoftware.os.filer.map.store.api;

import com.jivesoftware.os.filer.map.store.api.KeyValueStore.Entry;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 * @author jonathan.colt
 * @param <K>
 * @param <V>
 */
public interface KeyValueStore<K, V> extends Iterable<Entry<K, V>> {

    void add(K key, V value) throws IOException;

    void remove(K key) throws IOException;

    V get(K key) throws IOException;

    V getUnsafe(K key) throws IOException;

    Iterator<K> keysIterator();

    interface Entry<K, V> {

        K getKey();

        V getValue();
    }
}
