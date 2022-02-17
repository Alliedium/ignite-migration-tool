package io.github.alliedium.ignite.migration.serializer;

import io.github.alliedium.ignite.migration.dto.CacheDataTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

import static io.github.alliedium.ignite.migration.Utils.createFileFromPath;
import static io.github.alliedium.ignite.migration.serializer.utils.FieldNames.*;

/**
 * Represent a writer which writes cache data to avro file
 */
public class AvroCacheConfigurationsWriter {
    private static final Logger logger = LoggerFactory.getLogger(AvroCacheConfigurationsWriter.class);
    /**
     * pre-defined avro schema, which describes how provided cache configurations should be stored in avro
     */
    private final Schema avroSchema;
    /**
     * path to a file where serialized cache configurations are going to be stored
     */
    private final Path cacheConfigurationsAvroFilePath;
    private final String cacheConfigurations;
    private final String cacheQueryEntities;
    private final CacheDataTypes cacheDataTypes;

    private AvroCacheConfigurationsWriter(Builder builder) {
        avroSchema = builder.avroSchema;
        cacheConfigurationsAvroFilePath = builder.cacheConfigurationsAvroFilePath;
        cacheConfigurations = builder.cacheConfigurations;
        cacheQueryEntities = builder.cacheQueryEntities;
        cacheDataTypes = builder.cacheDataTypes;
    }

    /**
     * Serializes cache configurations to avro file.
     */
    public void write() {
        createFileFromPath(cacheConfigurationsAvroFilePath);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(avroSchema, cacheConfigurationsAvroFilePath.toFile());

            GenericRecord genericRecord = new GenericData.Record(avroSchema);
            genericRecord.put(CONFIGURATIONS_FIELD_NAME, cacheConfigurations);
            genericRecord.put(QUERY_ENTITIES_FIELD_NAME, cacheQueryEntities);

            Schema dataTypesSchema = avroSchema.getField(CACHE_DATA_TYPES_FIELD_NAME).schema();
            GenericRecord dataTypesRecord = new GenericData.Record(dataTypesSchema);
            dataTypesRecord.put(CACHE_KEY_TYPE_FIELD_NAME, cacheDataTypes.getKeyType());
            dataTypesRecord.put(CACHE_VAL_TYPE_FIELD_NAME, cacheDataTypes.getValType());

            genericRecord.put(CACHE_DATA_TYPES_FIELD_NAME, dataTypesRecord);

            dataFileWriter.append(genericRecord);
        }
        catch (IOException e) {
            logger.error("Cache configurations to " + cacheConfigurationsAvroFilePath + " file writing error: " + e.getMessage());
        }
    }

    public static class Builder {
        private Schema avroSchema;
        private Path cacheConfigurationsAvroFilePath;
        private String cacheConfigurations;
        private String cacheQueryEntities;
        private CacheDataTypes cacheDataTypes;

        public Builder setAvroSchema(Schema avroSchema) {
            this.avroSchema = avroSchema;
            return this;
        }

        public Builder setCacheConfigurationsAvroFilePath(Path cacheConfigurationsAvroFilePath) {
            this.cacheConfigurationsAvroFilePath = cacheConfigurationsAvroFilePath;
            return this;
        }

        public Builder setCacheConfigurations(String cacheConfigurations) {
            this.cacheConfigurations = cacheConfigurations;
            return this;
        }

        public Builder setCacheQueryEntities(String cacheQueryEntities) {
            this.cacheQueryEntities = cacheQueryEntities;
            return this;
        }

        public Builder setCacheDataTypes(CacheDataTypes cacheDataTypes) {
            this.cacheDataTypes = cacheDataTypes;
            return this;
        }

        public AvroCacheConfigurationsWriter build() {
            return new AvroCacheConfigurationsWriter(this);
        }
    }
}
