package org.alliedium.ignite.migration.serializer;

import java.nio.file.Path;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

/**
 * Provides the functionality of cache data conversion to avro and resulting avro files storing in the filesystem.
 * Cache data and cache configurations serializing mechanism is built-in. Correspondent avro schema needs to be provided for avro serialization.
 * Files in the filesystem are being created automatically per provided paths when absent.
 */
public interface IAvroFileWriter {

    void writeAvroSchemaToFile(Schema avroSchema, Path avroSchemaFilePath);

    void writeCacheConfigurationsToFile(Schema avroSchema, Path cacheConfigurationsAvroFilePath, String cacheConfigurations,
                                        String cacheQueryEntities);

    DataFileWriter<GenericRecord> prepareFileWriter(Schema schema, Path path);
}
