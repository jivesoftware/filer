package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.KeyMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;

public class VariableKeySizeBytesObjectMapStore<F extends Filer, K, V> implements KeyValueStore<K, V> {

    private final BytesObjectMapStore<F, byte[], V>[] mapStores;
    private final KeyMarshaller<K> keyMarshaller;

    public VariableKeySizeBytesObjectMapStore(BytesObjectMapStore<F, byte[], V>[] mapStores, KeyMarshaller<K> keyMarshaller) {
        this.mapStores = mapStores;
        this.keyMarshaller = keyMarshaller;
    }

    private BytesObjectMapStore<F, byte[], V> getMapStore(int keyLength) {
        for (int i = 0; i < mapStores.length; i++) {
            if (mapStores[i].keySize >= keyLength) {
                return mapStores[i];
            }
        }
        throw new IndexOutOfBoundsException("Key is too long");
    }

    @Override
    public <R> R execute(K key, boolean createIfAbsent, KeyValueTransaction<V, R> keyValueTransaction) throws IOException {
        byte[] keyBytes = keyMarshaller.keyBytes(key);
        return getMapStore(keyBytes.length).execute(keyBytes, createIfAbsent, keyValueTransaction);
    }

    @Override
    public boolean stream(final EntryStream<K, V> stream) throws IOException {
        EntryStream<byte[], V> byteStream = new EntryStream<byte[], V>() {
            @Override
            public boolean stream(byte[] key, V value) throws IOException {
                return stream.stream(keyMarshaller.bytesKey(key, 0), value);
            }
        };
        for (BytesObjectMapStore<F, byte[], V> mapStore : mapStores) {
            if (!mapStore.stream(byteStream)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean streamKeys(final KeyStream<K> stream) throws IOException {
        KeyStream<byte[]> byteStream = new KeyStream<byte[]>() {
            @Override
            public boolean stream(byte[] key) throws IOException {
                return stream.stream(keyMarshaller.bytesKey(key, 0));
            }
        };
        for (BytesObjectMapStore<F, byte[], V> mapStore : mapStores) {
            if (!mapStore.streamKeys(byteStream)) {
                return false;
            }
        }
        return true;
    }
}