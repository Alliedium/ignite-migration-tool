package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.IDispatcher;
import org.alliedium.ignite.migration.dto.*;
import org.alliedium.ignite.migration.serializer.converters.AvroFieldMetaContainer;
import org.alliedium.ignite.migration.serializer.converters.AvroGenericRecordToConverter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.alliedium.ignite.migration.serializer.utils.AvroFileNames;
import org.alliedium.ignite.migration.util.PathCombine;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AvroFileReader is used for getting avro files from the filesystem.
 * Avro data from picked files is being deserialized and prepared for further processing.
 */
public class AvroFileReader implements IAvroFileReader {

    private static final Logger logger = LoggerFactory.getLogger(AvroFileReader.class);
    private static final String AVRO_GENERIC_RECORD_KEY_FIELD_NAME = "key";
    private static final String AVRO_GENERIC_RECORD_CONFIGURATIONS_FIELD_NAME = "cacheConfigurations";
    private static final String AVRO_GENERIC_RECORD_QUERY_ENTITIES_FIELD_NAME = "cacheQueryEntities";
    private static final String AVRO_GENERIC_RECORD_IGNITE_ATOMIC_LONG_NAME_FIELD_NAME = "igniteAtomicLongName";
    private static final String AVRO_GENERIC_RECORD_IGNITE_ATOMIC_LONG_VALUE_FIELD_NAME = "igniteAtomicLongValue";

    private final PathCombine serializedCachePath;

    public AvroFileReader(PathCombine serializedCachePath) {
        this.serializedCachePath = serializedCachePath;
    }

    public Schema getCacheConfigurationsAvroSchema() throws IOException {
        Path avroSchemaFile = serializedCachePath.plus(AvroFileNames.SCHEMA_FOR_CACHE_CONFIGURATION_FILENAME).getPath();
        return new Schema.Parser().parse(avroSchemaFile.toFile());
    }

    public Schema getCacheDataAvroSchema() throws IOException {
        Path avroSchemaFile = serializedCachePath.plus(AvroFileNames.SCHEMA_FOR_CACHE_DATA_FILENAME).getPath();
        return new Schema.Parser().parse(avroSchemaFile.toFile());
    }

    public String getCacheConfiguration() throws IOException {
        Schema cacheConfigurationsAvroSchema = getCacheConfigurationsAvroSchema();
        Path cacheConfigurationFilePath = serializedCachePath.plus(AvroFileNames.CACHE_CONFIGURATION_FILENAME).getPath();
        GenericRecord deserializedCacheConfiguration = deserializeCacheConfiguration(cacheConfigurationFilePath, cacheConfigurationsAvroSchema);
        return deserializedCacheConfiguration.get(AVRO_GENERIC_RECORD_CONFIGURATIONS_FIELD_NAME).toString();
    }

    public String getCacheEntryMeta() throws IOException {
        Schema cacheConfigurationsAvroSchema = getCacheConfigurationsAvroSchema();
        Path cacheConfigurationFilePath = serializedCachePath.plus(AvroFileNames.CACHE_CONFIGURATION_FILENAME).getPath();
        GenericRecord deserializedCacheConfiguration = deserializeCacheConfiguration(cacheConfigurationFilePath, cacheConfigurationsAvroSchema);
        return deserializedCacheConfiguration.get(AVRO_GENERIC_RECORD_QUERY_ENTITIES_FIELD_NAME).toString();
    }

    public void distributeAtomicsLongData(IDispatcher<Map.Entry<String, Long>> atomicsLongDispatcher) throws IOException {
        Path atomicStructureFile = serializedCachePath.plus(AvroFileNames.ATOMIC_STRUCTURE_FILE_NAME).getPath();
        if (!Files.exists(atomicStructureFile)) {
            return;
        }

        Schema atomicsSchema = new Schema.Parser().parse(atomicStructureFile.toFile());
        Path atomicsDataFilePath = serializedCachePath.plus(AvroFileNames.ATOMIC_DATA_FILE_NAME).getPath();
        List<GenericRecord> atomicsData = deserializeAvro(atomicsDataFilePath, atomicsSchema);

        atomicsData.forEach(genericRecord -> {
            String atomicLongName = genericRecord.get(AVRO_GENERIC_RECORD_IGNITE_ATOMIC_LONG_NAME_FIELD_NAME).toString();
            long atomicLongValue = (long) genericRecord.get(AVRO_GENERIC_RECORD_IGNITE_ATOMIC_LONG_VALUE_FIELD_NAME);

            atomicsLongDispatcher.publish(new AbstractMap.SimpleEntry<>(atomicLongName, atomicLongValue));
        });
    }

    private GenericRecord deserializeCacheConfiguration(Path cacheConfigurationFilePath, Schema avroSchema) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(cacheConfigurationFilePath.toFile(), datumReader);
        GenericRecord deserializedCacheConfigurationRecord = null;
        while (dataFileReader.hasNext()) {
            deserializedCacheConfigurationRecord = dataFileReader.next(deserializedCacheConfigurationRecord);
        }

        if (deserializedCacheConfigurationRecord == null) {
            String message = "Cannot read records from provided cache configuration file: " + cacheConfigurationFilePath
                + " the file is empty or contains invalid data";
            logger.error(message);
            throw new IllegalArgumentException(message);
        }

        return deserializedCacheConfigurationRecord;
    }

    /**
     * Deserializes avro data read from file and distributes row by row data via dispatcher.
     *
     * @throws IOException when fails to execute avro file reading.
     * @see <a href="http://avro.apache.org/docs/current/gettingstartedjava.html#Deserializing-N10220">Avro deserialization short guide</a>
     */
    @Override
    public void distributeCacheData(String cacheName, Map<String, String> fieldsTypes, IDispatcher<ICacheData> cacheDataDispatcher) throws IOException {
        Path cacheDataFilePath = serializedCachePath.plus(AvroFileNames.CACHE_DATA_FILENAME).getPath();
        if (Files.exists(cacheDataFilePath)) {
            Schema cacheDataAvroSchema = getCacheDataAvroSchema();
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(cacheDataAvroSchema);
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(cacheDataFilePath.toFile(), datumReader);
            AvroGenericRecordToConverter avroGenericRecordToConverter = new AvroGenericRecordToConverter(fieldsTypes);
            AvroFieldMetaContainer avroFieldMetaContainer = new AvroFieldMetaContainer(cacheDataAvroSchema);

            GenericRecord cacheDataRecord = null;
            while (dataFileReader.hasNext()) {
                cacheDataRecord = dataFileReader.next(cacheDataRecord);
                ICacheEntryKey cacheEntryKey = new CacheEntryKey(
                        cacheDataRecord.get(AVRO_GENERIC_RECORD_KEY_FIELD_NAME).toString());
                ICacheEntryValue cacheEntryValue = avroGenericRecordToConverter
                        .getCacheEntryValue(cacheDataRecord, avroFieldMetaContainer);

                cacheDataDispatcher.publish(new CacheData(cacheName, cacheEntryKey, cacheEntryValue));
            }
        }
        else {
            logger.info("Data file for cache " + cacheDataFilePath.getFileName() + " does not exist. Nothing to deserialize.");
        }
    }

    public List<GenericRecord> deserializeAvro(Path dataFilePath, Schema avroSchema) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(dataFilePath.toFile(), datumReader);
        List<GenericRecord> deserializedCacheDataRecordsList = new ArrayList<>();
        while (dataFileReader.hasNext()) {
            deserializedCacheDataRecordsList.add(dataFileReader.next());
        }

        return deserializedCacheDataRecordsList;
    }

}
