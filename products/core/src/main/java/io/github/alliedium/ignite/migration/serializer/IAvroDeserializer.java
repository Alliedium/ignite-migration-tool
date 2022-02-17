package io.github.alliedium.ignite.migration.serializer;

import io.github.alliedium.ignite.migration.IDispatcher;
import io.github.alliedium.ignite.migration.dto.ICacheData;
import io.github.alliedium.ignite.migration.dto.ICacheMetaData;

import java.util.Map;

public interface IAvroDeserializer {

    void deserializeCaches(IDispatcher<ICacheMetaData> cacheMetaDataDispatcher, IDispatcher<ICacheData> cacheDataDispatcher);

    void deserializeAtomicsLong(IDispatcher<Map.Entry<String, Long>> atomicsLongDispatcher);
}
