package io.github.alliedium.ignite.migration.serializer;

import io.github.alliedium.ignite.migration.IDataWriter;
import io.github.alliedium.ignite.migration.util.PathCombine;

import java.util.Map;

import io.github.alliedium.ignite.migration.dto.ICacheData;
import io.github.alliedium.ignite.migration.dto.ICacheMetaData;

/**
 * Provides mechanisms of serializing the data received from dispatchers to external format and backward deserialization.
 */
public interface ISerializer {

    IDataWriter<ICacheMetaData> getCacheMetaDataSerializer();

    IDataWriter<ICacheData> getCacheDataSerializer();

    IDataWriter<Map.Entry<String, Long>> getAtomicLongsConsumer();

    CacheDataWriter prepareWriter(ICacheData cacheData, PathCombine cacheRelatedPath);
}
