package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.IDispatcher;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;

import java.util.Map;

public interface IAvroDeserializer {

    void deserializeCaches(IDispatcher<ICacheMetaData> cacheMetaDataDispatcher, IDispatcher<ICacheData> cacheDataDispatcher);

    void deserializeAtomicsLong(IDispatcher<Map.Entry<String, Long>> atomicsLongDispatcher);
}
