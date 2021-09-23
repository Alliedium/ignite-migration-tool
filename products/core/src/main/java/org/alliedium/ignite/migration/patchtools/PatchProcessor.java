package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.Dispatcher;
import org.alliedium.ignite.migration.IDataWriter;
import org.alliedium.ignite.migration.TasksExecutor;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;
import org.alliedium.ignite.migration.serializer.AvroDeserializer;
import org.alliedium.ignite.migration.serializer.AvroSerializer;
import org.alliedium.ignite.migration.serializer.IAvroDeserializer;

import java.nio.file.Path;

public class PatchProcessor implements IPatchProcessor {

    private final IPatch patch;
    private final IAvroDeserializer deserializer;
    private final IDataWriter<ICacheMetaData> cacheMetaDataSerializer;
    private final IDataWriter<ICacheData> cacheDataSerializer;

    public PatchProcessor(Path sourcePath, Path destinationPath, IPatch patch) {
        this.patch = patch;
        // prepare for read
        deserializer = new AvroDeserializer(sourcePath);
        AvroSerializer serializer = new AvroSerializer(destinationPath);
        cacheMetaDataSerializer = serializer.getCacheMetaDataSerializer();
        cacheDataSerializer = serializer.getCacheDataSerializer();
    }

    @Override
    public void process() {
        // patch
        IDataWriter<ICacheMetaData> cacheMetaDataWriter = this::patchCacheMetaData;
        IDataWriter<ICacheData> cacheDataWriter = this::patchCacheData;

        // setup start of patch
        Dispatcher<ICacheMetaData> cacheMetaDataDispatcher = new Dispatcher<>();
        cacheMetaDataDispatcher.subscribe(cacheMetaDataWriter);

        Dispatcher<ICacheData> cacheDataDispatcher = new Dispatcher<>();
        cacheDataDispatcher.subscribe(cacheDataWriter);
        Runnable deserialize = ()-> deserializer.deserializeCaches(cacheMetaDataDispatcher, cacheDataDispatcher);

        TasksExecutor.execute(deserialize, cacheMetaDataDispatcher, cacheDataDispatcher)
                .waitForCompletion();
        closeResources(cacheMetaDataSerializer, cacheDataSerializer);
    }

    private void patchCacheMetaData(ICacheMetaData metaData) {
        try {
            // patch metadata
            ICacheMetaData resultMetaData = patch.transformMetaData(metaData);
            // writing back to files
            cacheMetaDataSerializer.write(resultMetaData);
        } catch (Throwable t) {
            throw new IllegalStateException(
                    String.format("Exception occurred while patching meta data of cache: %s",
                            metaData.getCacheName()), t);
        }
    }

    private void patchCacheData(ICacheData cacheData) {
        try {
            // patch data
            ICacheData resultCacheData = patch.transformData(cacheData);
            // writing back to files
            cacheDataSerializer.write(resultCacheData);
        } catch (Throwable t) {
            throw new IllegalStateException(
                    String.format("Exception occurred while patching cache data of cache: %s",
                            cacheData.getCacheName()), t);
        }
    }

    private void closeResources(AutoCloseable... autoCloseables) {
        try {
            for (AutoCloseable autoCloseable : autoCloseables) {
                autoCloseable.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
