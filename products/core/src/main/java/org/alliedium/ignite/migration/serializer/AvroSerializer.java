package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.IDataWriter;
import org.alliedium.ignite.migration.dao.IIgniteReader;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;
import org.alliedium.ignite.migration.serializer.converters.CacheFieldMetaContainer;
import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMetaContainer;
import org.alliedium.ignite.migration.serializer.utils.AvroFileNames;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.alliedium.ignite.migration.util.PathCombine;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AvroSerializer general responsibility is to execute conversion from DTO to avro format.
 * Resulting avro data is stored as avro files in a filesystem.
 * {@link IIgniteReader} needs to be passed as an argument into 'serialize' method of current class.
 * If the incoming DTO data is not natively supported by avro, converters are being applied for "unknown" datatypes.
 */
public class AvroSerializer implements ISerializer {

    private static final Logger logger = LoggerFactory.getLogger(AvroSerializer.class);

    private final AvroFileWriter avroFileWriter;
    private final PathCombine rootSerializedDataPath;
    private final IAvroSchemaBuilder schemaBuilder;

    public AvroSerializer(Path rootSerializedDataPath) {
        avroFileWriter = new AvroFileWriter();
        schemaBuilder = new AvroSchemaBuilder();
        this.rootSerializedDataPath = new PathCombine(rootSerializedDataPath);
    }

    public IDataWriter<Map.Entry<String, Long>> getAtomicLongsConsumer() {
        return prepareAtomicWriter(rootSerializedDataPath, schemaBuilder);
    }

    @Override
    public IDataWriter<ICacheMetaData> getCacheMetaDataSerializer() {
        return cacheMetaData -> {
            String cacheName = cacheMetaData.getCacheName();
            PathCombine cacheRelatedPath = rootSerializedDataPath.plus(cacheName);
            logger.info("Starting " + cacheName + " cache serialization to avro");

            writeCacheMetaDataToAvro(cacheMetaData, schemaBuilder, cacheRelatedPath);
            logger.info("Successfully serialized " + cacheName + " cache configurations to avro");
        };
    }

    @Override
    public IDataWriter<ICacheData> getCacheDataSerializer() {
        return new CacheDataWriterManager(rootSerializedDataPath, this);
    }

    @Override
    public CacheDataWriter prepareWriter(ICacheData cacheData, PathCombine cacheRelatedPath) {
        ICacheFieldMetaContainer cacheFieldAvroMetaContainer = new CacheFieldMetaContainer(cacheData.getCacheEntryValue());
        List<String> cacheValueDTOFieldNamesList = cacheData.getCacheEntryValue().getFieldNamesList();

        Schema cacheDataAvroSchema = schemaBuilder.getCacheDataAvroSchema(cacheValueDTOFieldNamesList, cacheFieldAvroMetaContainer);
        Path cacheDataAvroFilePath = cacheRelatedPath.plus(AvroFileNames.CACHE_DATA_FILENAME).getPath();
        Path cacheDataAvroSchemaFilePath = cacheRelatedPath.plus(AvroFileNames.SCHEMA_FOR_CACHE_DATA_FILENAME).getPath();

        avroFileWriter.writeAvroSchemaToFile(cacheDataAvroSchema, cacheDataAvroSchemaFilePath);

        DataFileWriter<GenericRecord> dataFileWriter = avroFileWriter.prepareFileWriter(cacheDataAvroSchema, cacheDataAvroFilePath);

        return new CacheDataWriter(dataFileWriter, cacheDataAvroSchema, cacheFieldAvroMetaContainer);
    }

    private AtomicDataWriter prepareAtomicWriter(PathCombine rootSerializationPath, IAvroSchemaBuilder schemaBuilder) {
        Schema atomicStructureSchema = schemaBuilder.getAtomicStructureSchema();
        avroFileWriter.writeAvroSchemaToFile(atomicStructureSchema,
                rootSerializationPath.plus(AvroFileNames.ATOMIC_STRUCTURE_FILE_NAME).getPath());

        PathCombine atomicDataFilePath = rootSerializationPath.plus(AvroFileNames.ATOMIC_DATA_FILE_NAME);

        DataFileWriter<GenericRecord> dataFileWriter = avroFileWriter.prepareFileWriter(atomicStructureSchema, atomicDataFilePath.getPath());

        return new AtomicDataWriter(dataFileWriter, atomicStructureSchema);
    }

    private void writeCacheMetaDataToAvro(ICacheMetaData cacheMetaData, IAvroSchemaBuilder avroSchemaBuilder,
                                          PathCombine cacheRelatedPath) {
        Schema cacheConfigurationsAvroSchema = avroSchemaBuilder.getCacheConfigurationsAvroSchema();
        Path cacheConfigurationsAvroFilePath = cacheRelatedPath.plus(AvroFileNames.CACHE_CONFIGURATION_FILENAME).getPath();
        Path cacheConfigurationsAvroSchemaFilePath = cacheRelatedPath.plus(AvroFileNames.SCHEMA_FOR_CACHE_CONFIGURATION_FILENAME).getPath();

        avroFileWriter.writeAvroSchemaToFile(cacheConfigurationsAvroSchema, cacheConfigurationsAvroSchemaFilePath);
        avroFileWriter.writeCacheConfigurationsToFile(cacheConfigurationsAvroSchema, cacheConfigurationsAvroFilePath,
                cacheMetaData.getConfiguration().toString(), cacheMetaData.getEntryMeta().toString());
    }
}
