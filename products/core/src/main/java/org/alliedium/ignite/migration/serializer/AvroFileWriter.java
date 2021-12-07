package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMetaContainer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.alliedium.ignite.migration.Utils.createFileFromPath;

/**
 * AvroFileWriter accepts the data and executes it's serialization to avro.
 * Serialized avro data is stored as avro files in the filesystem. Paths for avro files need to be provided. Files and directories
 * are being created respectively based on provided paths when absent.
 * When the derived data type is not supported by avro, converter needs to be applied.
 * Converters are taken from correspondent {@link ICacheFieldMetaContainer} containers which need to be passed from the outside.
 */
public class AvroFileWriter implements IAvroFileWriter {

    private static final Logger logger = LoggerFactory.getLogger(AvroFileWriter.class);

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
}
