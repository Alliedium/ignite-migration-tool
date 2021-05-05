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
    private final Path sourcePath;
    private final Path destinationPath;

    public PatchProcessor(Path sourcePath, Path destinationPath, IPatch patch) {
        this.patch = patch;
        this.sourcePath = sourcePath;
        this.destinationPath = destinationPath;
    }

    @Override
    public void process() {
        // prepare for read
        IAvroDeserializer deserializer = new AvroDeserializer(sourcePath);

        AvroSerializer serializer = new AvroSerializer(destinationPath);
        IDataWriter<ICacheMetaData> cacheMetaDataSerializer = serializer.getCacheMetaDataSerializer();
        IDataWriter<ICacheData> cacheDataSerializer = serializer.getCacheDataSerializer();

        // patch
        IDataWriter<ICacheMetaData> cacheMetaDataWriter = metaData -> {
            // patch metadata
            ICacheMetaData resultMetaData = patch.transformMetaData(metaData);
            // writing back to files
            cacheMetaDataSerializer.write(resultMetaData);
        };

        IDataWriter<ICacheData> cacheDataWriter = cacheData -> {
            // patch data
            ICacheData resultCacheData = patch.transformData(cacheData);
            // writing back to files
            cacheDataSerializer.write(resultCacheData);
        };

        // setup start of patch
        Dispatcher<ICacheMetaData> cacheMetaDataDispatcher = new Dispatcher<>();
        cacheMetaDataDispatcher.subscribe(cacheMetaDataWriter);

        Dispatcher<ICacheData> cacheDataDispatcher = new Dispatcher<>();
        cacheDataDispatcher.subscribe(cacheDataWriter);

        TasksExecutor executor = new TasksExecutor(cacheMetaDataDispatcher, cacheDataDispatcher);
        executor.execute();

        deserializer.deserializeCaches(cacheMetaDataDispatcher, cacheDataDispatcher);

        executor.waitForCompletion();
        closeResources(cacheMetaDataSerializer, cacheDataSerializer);
        executor.shutdown();
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
