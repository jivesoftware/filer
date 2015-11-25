package com.jivesoftware.os.filer.io.api;

import java.io.IOException;
import java.util.List;

/**
 * @param <K>
 * @param <V>
 * @author jonathan.colt
 */
public interface KeyValueStore<K, V> {

    boolean[] contains(List<K> keys, StackBuffer stackBuffer) throws IOException, InterruptedException;

    void multiExecute(K[] keys, IndexAlignedKeyValueTransaction<V> indexAlignedKeyValueTransaction, StackBuffer stackBuffer) throws IOException,
        InterruptedException;

    <R> R execute(K key, boolean createIfAbsent, KeyValueTransaction<V, R> keyValueTransaction, StackBuffer stackBuffer) throws IOException,
        InterruptedException;

    boolean stream(EntryStream<K, V> stream, StackBuffer stackBuffer) throws IOException, InterruptedException;

    boolean streamKeys(KeyStream<K> stream, StackBuffer stackBuffer) throws IOException, InterruptedException;

    interface EntryStream<K, V> {

        boolean stream(K key, V value) throws IOException, InterruptedException;
    }

    interface KeyStream<K> {

        boolean stream(K key) throws IOException, InterruptedException;
    }
}
