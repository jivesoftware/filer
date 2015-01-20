package com.jivesoftware.os.filer.map.store.api;

import java.io.IOException;

/**
 * @param <K>
 * @param <V>
 * @author jonathan.colt
 */
public interface KeyValueStore<K, V> {

    boolean[] contains(K[] keys) throws IOException;

    <R> R execute(K key, boolean createIfAbsent, KeyValueTransaction<V, R> keyValueTransaction) throws IOException;

    boolean stream(EntryStream<K, V> stream) throws IOException;

    boolean streamKeys(KeyStream<K> stream) throws IOException;

    interface EntryStream<K, V> {
        boolean stream(K key, V value) throws IOException;
    }

    interface KeyStream<K> {
        boolean stream(K key) throws IOException;
    }
}
