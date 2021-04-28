package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.IDataWriter;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheEntryKey;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMetaContainer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class CacheDataWriter implements IDataWriter<ICacheData> {

    private static final String AVRO_GENERIC_RECORD_KEY_FIELD_NAME = "key";

    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final Schema cacheDataAvroSchema;
    private final ICacheFieldMetaContainer cacheFieldAvroMetaContainer;

    /**
     * @param dataFileWriter avro data file writer
     * @param cacheDataAvroSchema pre-defined avro schema, which describes how should current cache data be stored in avro
     * @param cacheFieldAvroMetaContainer meta-data for each of the fields from cacheData
     */
    public CacheDataWriter(DataFileWriter<GenericRecord> dataFileWriter, Schema cacheDataAvroSchema,
                           ICacheFieldMetaContainer cacheFieldAvroMetaContainer) {
        this.dataFileWriter = dataFileWriter;
        this.cacheDataAvroSchema = cacheDataAvroSchema;
        this.cacheFieldAvroMetaContainer = cacheFieldAvroMetaContainer;
    }

    /**
     * Serializes cache data and writes it intoavro file.
     *
     * @see <a href="http://avro.apache.org/docs/current/gettingstartedjava.html#Serializing+and+deserializing+without+code+generation">Avro serialization short guide</a>
     * @param cacheData cache data from processing DTO
     */
    @Override
    public void write(ICacheData cacheData) {
        try {
            dataFileWriter.append(genericRecordFromCacheDataEntry(cacheData.getCacheEntryKey(),
                    cacheData.getCacheEntryValue(), cacheDataAvroSchema, cacheFieldAvroMetaContainer));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write cache data into avro file");
        }
    }

    private GenericRecord genericRecordFromCacheDataEntry(ICacheEntryKey cacheKey, ICacheEntryValue cacheValue,
                                                          Schema avroSchema, ICacheFieldMetaContainer fieldTypeConvertersContainer) {
        GenericRecord genericRecord = new GenericData.Record(avroSchema);

        genericRecord.put(AVRO_GENERIC_RECORD_KEY_FIELD_NAME, cacheKey.toString());

        List<String> cacheValueFieldNameList = cacheValue.getFieldNamesList();
        for (String fieldName : cacheValueFieldNameList) {
            Optional<Object> cacheValueFieldValue = cacheValue.getField(fieldName).getFieldValue().getValue();
            if (!cacheValueFieldValue.isPresent()) {
                genericRecord.put(fieldName, null);
            }
            else {
                genericRecord.put(fieldName, fieldTypeConvertersContainer.getFieldTypeMeta(fieldName)
                        .getAvroDerivedTypeConverter().convertForAvro(cacheValueFieldValue.get())
                );
            }
        }

        return genericRecord;
    }

    @Override
    public void close() throws Exception {
        dataFileWriter.close();
    }
}
