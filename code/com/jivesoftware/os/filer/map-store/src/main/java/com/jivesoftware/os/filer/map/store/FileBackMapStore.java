package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.filer.map.store.api.PartitionedKeyValueStore;
import com.jivesoftware.os.filer.map.store.extractors.ExtractIndex;
import com.jivesoftware.os.filer.map.store.extractors.ExtractKey;
import com.jivesoftware.os.filer.map.store.extractors.ExtractPayload;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * @author jonathan
 * @param <K>
 * @param <V>
 */
public abstract class FileBackMapStore<K, V> implements PartitionedKeyValueStore<K, V> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger(true);

    private static final byte[] EMPTY_ID = new byte[16];

    private final MapStore mapStore = new MapStore(ExtractIndex.SINGLETON, ExtractKey.SINGLETON, ExtractPayload.SINGLETON);
    private final String[] pathsToPartitions;
    private final int keySize;
    private final int payloadSize;
    private final int initialPageCapacity;
    private final StripingLocksProvider<String> keyLocksProvider;
    private final Map<String, MapChunk> indexPages;
    private final V returnWhenGetReturnsNull;

    public FileBackMapStore(String[] pathsToPartitions,
        int keySize,
        int payloadSize,
        int initialPageCapacity,
        int concurrency,
        V returnWhenGetReturnsNull) {
        this.pathsToPartitions = pathsToPartitions;
        this.keySize = keySize;
        this.payloadSize = payloadSize;
        this.initialPageCapacity = initialPageCapacity;
        this.keyLocksProvider = new StripingLocksProvider<>(concurrency);
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.indexPages = new ConcurrentSkipListMap<>();
    }

    @Override
    public void add(K key, V value) throws KeyValueStoreException {
        if (key == null || value == null) {
            return;
        }

        byte[] keyBytes = keyBytes(key);
        byte[] rawChildActivity = valueBytes(value);
        if (rawChildActivity == null) {
            return;
        }
        synchronized (keyLocksProvider.lock(keyPartition(key))) {
            MapChunk index = index(key);

            try {
                // grow the set if needed;
                if (mapStore.getCount(index) >= index.maxCount) {
                    int newSize = index.maxCount * 2;

                    File temporaryNewKeyIndexPartition = createIndexTempFile(key);
                    MapChunk newIndex = mmap(temporaryNewKeyIndexPartition, newSize);
                    mapStore.copyTo(index, newIndex, null);
                    // TODO: implement to clean up
                    //index.close();
                    //newIndex.close();
                    File createIndexSetFile = createIndexSetFile(key);
                    FileUtils.forceDelete(createIndexSetFile);
                    FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
                    FileUtils.forceDelete(temporaryNewKeyIndexPartition);

                    index = mmap(createIndexSetFile(key), newSize);

                    indexPages.put(keyPartition(key), index);
                }
            } catch (IOException e) {
                throw new KeyValueStoreException("Error when expanding size of partition!", e);
            }

            byte[] payload = mapStore.get(index, keyBytes, ExtractPayload.SINGLETON);
            if (payload == null) {
                payload = new byte[(payloadSize)];
            }
            System.arraycopy(rawChildActivity, 0, payload, 0, rawChildActivity.length);
            mapStore.add(index, (byte) 1, keyBytes, payload);
        }

    }

    @Override
    public void remove(K key) throws KeyValueStoreException {
        if (key == null) {
            return;
        }

        byte[] keyBytes = keyBytes(key);
        synchronized (keyLocksProvider.lock(keyPartition(key))) {
            MapChunk index = index(key);
            mapStore.remove(index, keyBytes);
        }
    }

    private File createIndexSetFile(K key) {
        return createIndexFilePostfixed(keyPartition(key), ".set");
    }

    private File createIndexTempFile(K key) {
        return createIndexFilePostfixed(keyPartition(key) + "-" + UUID.randomUUID().toString(), ".tmp");
    }

    private File createIndexFilePostfixed(String partition, String postfix) {
        String pathToPartitions = pathsToPartitions[Math.abs(partition.hashCode()) % pathsToPartitions.length];
        String newIndexFilename = partition + (postfix == null ? "" : postfix);
        return new File(pathToPartitions, newIndexFilename);
    }

    @Override
    public V getUnsafe(K key) throws KeyValueStoreException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        MapChunk index = index(key);
        byte[] keyBytes = keyBytes(key);
        byte[] payload = mapStore.get(index.duplicate(), keyBytes, ExtractPayload.SINGLETON);

        if (payload == null) {
            return returnWhenGetReturnsNull;
        }
        return bytesValue(key, payload, 0);
    }

    @Override
    public V get(K key) throws KeyValueStoreException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyBytes(key);
        byte[] payload;
        synchronized (keyLocksProvider.lock(keyPartition(key))) {
            MapChunk index = index(key);
            payload = mapStore.get(index, keyBytes, ExtractPayload.SINGLETON);
        }
        if (payload == null) {
            return returnWhenGetReturnsNull;
        }
        return bytesValue(key, payload, 0);
    }

    private MapChunk index(K key) throws KeyValueStoreException {
        try {
            String pageId = keyPartition(key);
            MapChunk got = indexPages.get(pageId);
            if (got != null) {
                return got;
            }

            synchronized (keyLocksProvider.lock(pageId)) {
                got = indexPages.get(pageId);
                if (got != null) {
                    return got;
                }

                File file = createIndexSetFile(key);
                if (!file.exists()) {
                    // initializing in a temporary file prevents accidental corruption if the thread dies during mmap
                    File temporaryNewKeyIndexPartition = createIndexTempFile(key);
                    mmap(temporaryNewKeyIndexPartition, initialPageCapacity);

                    File createIndexSetFile = createIndexSetFile(key);
                    FileUtils.copyFile(temporaryNewKeyIndexPartition, createIndexSetFile);
                    FileUtils.forceDelete(temporaryNewKeyIndexPartition);
                }

                got = mmap(file, initialPageCapacity);
                indexPages.put(pageId, got);
            }
            return got;
        } catch (FileNotFoundException fnfx) {
            throw new KeyValueStoreException("Page file could not be found", fnfx);
        } catch (IOException iox) {
            throw new KeyValueStoreException("Failed to map page from disk", iox);
        }
    }

    private MapChunk mmap(final File file, int maxCapacity) throws FileNotFoundException, IOException {
        final FileBackedMemMappedByteBufferFactory pageFactory = new FileBackedMemMappedByteBufferFactory(file,
                FileBackedMemMappedByteBufferFactory.DEFAULT_64BIT_MAX_BUFF);
        if (file.exists()) {
            MappedByteBuffer buffer = pageFactory.open();
            MapChunk page = new MapChunk(mapStore, buffer);
            page.init();
            return page;
        } else {
            MapChunk set = mapStore.allocate((byte) 0, (byte) 0, EMPTY_ID, 0, maxCapacity, keySize,
                payloadSize,
                new ByteBufferFactory() {

                    @Override
                    public ByteBuffer allocate(long _size) {
                        return pageFactory.allocate(_size);
                    }
                });
            return set;
        }
    }

    @Override
    public long estimatedMaxNumberOfKeys() {
        return mapStore.absoluteMaxCount(keySize, payloadSize);
    }

    @Override
    public long estimateSizeInBytes() throws Exception {
        final MutableLong size = new MutableLong(0);
        for (String pathToPartitions : pathsToPartitions) {
            File partitionsDir = new File(pathToPartitions);
            if (!partitionsDir.exists()) {
                continue;
            }

            // TODO - Cache this value and only recalculate when add or remove is called?
            Files.walkFileTree(partitionsDir.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    size.add(attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    LOG.warn("Unable to calculate size of file: " + file, exc);
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        return size.longValue();
    }

    protected abstract Iterable<String> keyPartitions();

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        for (String pageId : keyPartitions()) {
            MapChunk got = indexPages.get(pageId);
            if (got != null) {
                iterators.add(Iterators.transform(mapStore.iterator(got), new Function<MapStore.Entry, Entry<K, V>>() {
                    @Override
                    public Entry<K, V> apply(final MapStore.Entry input) {
                        final K key = bytesKey(input.key, 0);
                        final V value = bytesValue(key, input.payload, 0);

                        return new Entry<K, V>() {
                            @Override
                            public K getKey() {
                                return key;
                            }

                            @Override
                            public V getValue() {
                                return value;
                            }
                        };
                    }
                }));
            }
        }
        return Iterators.concat(iterators.iterator());
    }
}
