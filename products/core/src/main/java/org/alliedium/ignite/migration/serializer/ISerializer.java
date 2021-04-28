package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.IDataWriter;

import java.util.Map;

import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;
import org.alliedium.ignite.migration.util.PathCombine;

/**
 * Provides mechanisms of serializing the data received from dispatchers to external format and backward deserialization.
 */
public interface ISerializer {

    IDataWriter<ICacheMetaData> getCacheMetaDataSerializer();

    IDataWriter<ICacheData> getCacheDataSerializer();

    IDataWriter<Map.Entry<String, Long>> getAtomicLongsConsumer();

    CacheDataWriter prepareWriter(ICacheData cacheData, PathCombine cacheRelatedPath);
}
