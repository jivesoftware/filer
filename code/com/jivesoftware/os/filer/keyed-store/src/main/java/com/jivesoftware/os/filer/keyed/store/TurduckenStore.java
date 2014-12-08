package com.jivesoftware.os.filer.keyed.store;

/**
 *
 * @author jonathan.colt
 */
public interface TurduckenStore {

    // Table (IndexName) -> SuperIndex(Usages) -> Index(Fields, Chunk) -> ChunkStore
    // ChunkStore first fp -> Table FP
    // Table FP -> MapStore<byte[](variable key size or not), Chunk FP Long> (aka Row Store)
    // RowSore -> MapStore<byte[](variable key size or not), Chunk FP Long>  (aka Value Store)
    // Value Store MapStore<byte[](variable key size or not), byte[](variable key size or not)>

    byte[] get(String table,
        RowType rowType, byte[] row,
        ColumnType columnType, byte[] column,
        KeyValueType keyType, byte[] key) throws Exception;

    void set(String table,
        RowType rowType, byte[] row,
        ColumnType columnType, byte[] column,
        KeyValueType keyType, byte[] key,
        byte[] value) throws Exception;

    void remove(String table,
        RowType rowType, byte[] row,
        ColumnType columnType, byte[] column,
        KeyValueType keyType, byte[] key) throws Exception;


    /**
     *
     */
    interface RowType extends Type {
    }

    interface ColumnType extends Type {
    }

    interface KeyValueType extends Type {

        int[] valueVariableSized();
    }


    interface Type<K> {

        String name();

        int[] keySize();

        Partitioner<K> paritioner();

    }

    interface Partitioner<K> {

        int partition(K k);
    }

}
