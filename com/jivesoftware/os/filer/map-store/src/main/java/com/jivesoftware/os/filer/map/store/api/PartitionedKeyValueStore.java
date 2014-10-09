package com.jivesoftware.os.filer.map.store.api;

public interface PartitionedKeyValueStore<K, V> extends KeyValueStore<K, V> {

    String keyPartition(K key);
}
