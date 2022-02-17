package io.github.alliedium.ignite.migration.serializer;

import io.github.alliedium.ignite.migration.IDataWriter;
import io.github.alliedium.ignite.migration.util.PathCombine;
import io.github.alliedium.ignite.migration.dto.ICacheData;

import java.util.HashMap;
import java.util.Map;

public class CacheDataWriterManager implements IDataWriter<ICacheData> {

    private final PathCombine rootSerializedDataPath;
    private final AvroSerializer avroSerializer;
    private final Map<String, CacheDataWriter> cacheWriters = new HashMap<>();

    public CacheDataWriterManager(PathCombine rootSerializedDataPath, AvroSerializer avroSerializer) {
        this.rootSerializedDataPath = rootSerializedDataPath;
        this.avroSerializer = avroSerializer;
    }

    @Override
    public void write(ICacheData data) {
        if (cacheWriters.get(data.getCacheName()) == null) {
            PathCombine cacheRelatedPath = rootSerializedDataPath.plus(data.getCacheName());
            CacheDataWriter fileWriter = avroSerializer.prepareWriter(data, cacheRelatedPath);
            cacheWriters.put(data.getCacheName(), fileWriter);
        }

        cacheWriters.get(data.getCacheName()).write(data);
    }

    @Override
    public void close() throws Exception {
        for(Map.Entry<String, CacheDataWriter> entry : cacheWriters.entrySet()) {
            entry.getValue().close();
        }
        cacheWriters.clear();
    }
}
