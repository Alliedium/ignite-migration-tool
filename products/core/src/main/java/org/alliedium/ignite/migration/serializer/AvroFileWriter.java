package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMetaContainer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AvroFileWriter accepts the data and executes it's serialization to avro.
 * Serialized avro data is stored as avro files in the filesystem. Paths for avro files need to be provided. Files and directories
 * are being created respectively based on provided paths when absent.
 * When the derived data type is not supported by avro, converter needs to be applied.
 * Converters are taken from correspondent {@link ICacheFieldMetaContainer} containers which need to be passed from the outside.
 */
public class AvroFileWriter implements IAvroFileWriter {

    private static final Logger logger = LoggerFactory.getLogger(AvroFileWriter.class);
    private static final String AVRO_GENERIC_RECORD_CONFIGURATIONS_FIELD_NAME = "cacheConfigurations";
    private static final String AVRO_GENERIC_RECORD_QUERY_ENTITIES_FIELD_NAME = "cacheQueryEntities";

    @Override
    public void writeAvroSchemaToFile(Schema avroSchema, Path avroSchemaFilePath) {
        createFileFromPath(avroSchemaFilePath);
        try {
            Files.write(avroSchemaFilePath, String.valueOf(avroSchema).getBytes());
            logger.info("Successfully saved avro schema to " + avroSchemaFilePath);
        }
        catch (IOException e) {
            logger.warn("Failed to write avro schema to " + avroSchemaFilePath + " due to an error: " + e.getMessage());
        }
    }

    @Override
    public DataFileWriter<GenericRecord> prepareFileWriter(Schema schema, Path path) {
        createFileFromPath(path);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        try {
            dataFileWriter.create(schema, path.toFile());
        } catch (IOException e) {
            logger.error("Failed to prepare file writer", e);
            throw new IllegalStateException(e);
        }

        return dataFileWriter;
    }

    /**
     * Serializes cache configurations to avro file.
     *
     * @see <a href="http://avro.apache.org/docs/current/gettingstartedjava.html#Serializing+and+deserializing+without+code+generation">Avro serialization short guide</a>
     *
     * @param avroSchema pre-defined avro schema, which describes how should provided cache configurations be stored in avro
     * @param cacheConfigurationsAvroFilePath path to a file where serialized cache configurations are going to be stored
     * @param cacheConfigurations cache configurations from DTO
     * @param cacheQueryEntities cache query entities from DTO
     */
    @Override
    public void writeCacheConfigurationsToFile(Schema avroSchema, Path cacheConfigurationsAvroFilePath, String cacheConfigurations,
                                               String cacheQueryEntities) {
        createFileFromPath(cacheConfigurationsAvroFilePath);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(avroSchema, cacheConfigurationsAvroFilePath.toFile());

            GenericRecord genericRecord = new GenericData.Record(avroSchema);
            genericRecord.put(AVRO_GENERIC_RECORD_CONFIGURATIONS_FIELD_NAME, cacheConfigurations);
            genericRecord.put(AVRO_GENERIC_RECORD_QUERY_ENTITIES_FIELD_NAME, cacheQueryEntities);
            dataFileWriter.append(genericRecord);
        }
        catch (IOException e) {
            logger.error("Cache configurations to " + cacheConfigurationsAvroFilePath + " file writing error: " + e.getMessage());
        }
    }

    private void createFileFromPath(Path filePath) {
        if(Files.exists(filePath)) {
            return;
        }

        try {
            Path parent = filePath.getParent();
            if (parent != null && !Files.exists(parent)) {
                logger.info("parent directory " + parent + " does not exist. Creating the directory.");
                Files.createDirectories(parent);
            }
            Files.createFile(filePath);
        } catch(IOException e) {
            logger.error("Failed to create file by path: " + filePath, e);
            throw new IllegalArgumentException(e);
        }
    }
}
