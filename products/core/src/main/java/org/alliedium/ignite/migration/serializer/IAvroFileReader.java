package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.IDispatcher;
import org.alliedium.ignite.migration.dto.ICacheData;

import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;

/**
 * Reads avro files and returns the data required for cache dto initialization.
 * Also capable for reading files containing avro schema. Returning schemas are required for avro data deserialization.
 * Deserialization mechanism is built-in.
 */
public interface IAvroFileReader {

    Schema getCacheConfigurationsAvroSchema() throws IOException;

    Schema getCacheDataAvroSchema() throws IOException;

    String getCacheConfiguration() throws IOException;

    String getCacheEntryMeta() throws IOException;

    void distributeAtomicsLongData(IDispatcher<Map.Entry<String, Long>> atomicsLongDispatcher) throws IOException;

    void distributeCacheData(String cacheName, Map<String, String> fieldsTypes,
                             IDispatcher<ICacheData> cacheDataDispatcher) throws IOException;

}
