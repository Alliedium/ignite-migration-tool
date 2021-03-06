package io.github.alliedium.ignite.migration;

public interface IDataWriter<T> extends AutoCloseable {

    void write(T data);

    /**
     * Optional autocloseable
     */
    @Override
    default void close() throws Exception {
        // do nothing, optional autocloseable
    }
}
