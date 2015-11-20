package com.jivesoftware.os.filer.io.api;

import java.io.IOException;
import java.util.List;

/**
 * @param <K>
 * @param <V>
 * @author jonathan.colt
 */
public interface KeyValueStore<K, V> {

    boolean[] contains(List<K> keys, byte[] primitiveBuffer) throws IOException;

    void multiExecute(K[] keys, IndexAlignedKeyValueTransaction<V> indexAlignedKeyValueTransaction, byte[] primitiveBuffer) throws IOException;

    <R> R execute(K key, boolean createIfAbsent, KeyValueTransaction<V, R> keyValueTransaction, byte[] primitiveBuffer) throws IOException;

    boolean stream(EntryStream<K, V> stream, byte[] primitiveBuffer) throws IOException;

    boolean streamKeys(KeyStream<K> stream, byte[] primitiveBuffer) throws IOException;

    interface EntryStream<K, V> {

        boolean stream(K key, V value) throws IOException;
    }

    interface KeyStream<K> {

        boolean stream(K key) throws IOException;
    }
}
