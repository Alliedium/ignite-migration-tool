package org.alliedium.ignite.migration;

public interface IDataWriter<T> extends AutoCloseable {

    void write(T data);

    /**
     * Optional autocloseable
     * @throws Exception
     */
    @Override
    default void close() throws Exception {
        // do nothing, optional autocloseable
    }
}
