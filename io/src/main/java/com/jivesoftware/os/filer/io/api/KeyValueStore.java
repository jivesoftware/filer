package com.jivesoftware.os.filer.io.api;

import java.io.IOException;
import java.util.List;

/**
 * @param <K>
 * @param <V>
 * @author jonathan.colt
 */
public interface KeyValueStore<K, V> {

    boolean[] contains(List<K> keys) throws IOException;

    void multiExecute(K[] keys, IndexAlignedKeyValueTransaction<V> indexAlignedKeyValueTransaction) throws IOException;

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
