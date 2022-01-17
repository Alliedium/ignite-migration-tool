package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.IDataWriter;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValueField;
import org.alliedium.ignite.migration.serializer.converters.datatypes.AvroDerivedTypeConverterFactory;
import org.alliedium.ignite.migration.serializer.utils.FieldNames;
import org.alliedium.ignite.migration.util.TypeUtils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CacheDataWriter implements IDataWriter<ICacheData> {

    /**
     * avro data file writer
     */
    private final DataFileWriter<GenericRecord> dataFileWriter;

    /**
     * pre-defined avro schema, which describes how should current cache data be stored in avro
     */
    private final Schema schema;

    private CacheDataWriter(Builder builder) {
        this.dataFileWriter = builder.dataFileWriter;
        this.schema = builder.cacheDataAvroSchema;
    }

    /**
     * Serializes cache data and writes it into avro file.
     *
     * @see <a href="http://avro.apache.org/docs/current/gettingstartedjava.html#Serializing+and+deserializing+without+code+generation">Avro serialization short guide</a>
     * @param cacheData cache data from processing DTO
     */
    @Override
    public void write(ICacheData cacheData) {
        try {
            dataFileWriter.append(genericRecordFromCacheDataEntry(cacheData.getCacheEntryKey(),
                    cacheData.getCacheEntryValue()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write cache data into avro file");
        }
    }

    private GenericRecord genericRecordFromCacheDataEntry(ICacheEntryValue cacheKey, ICacheEntryValue cacheValue) {
        GenericRecord genericRecord = generateGenericRecord(schema, cacheValue);

        genericRecord.put(FieldNames.KEY_FIELD_NAME, generateGenericRecord(
                schema.getField(FieldNames.KEY_FIELD_NAME).schema(), cacheKey));

        return genericRecord;
    }

    private GenericRecord generateGenericRecord(Schema schema, ICacheEntryValue value) {
        GenericRecord genericRecord = new GenericData.Record(schema);

        List<String> cacheValueFieldNameList = value.getFieldNames();
        for (String fieldName : cacheValueFieldNameList) {
            Object resultVal = getFieldObject(value.getField(fieldName), schema);
            genericRecord.put(fieldName, resultVal);
        }

        return genericRecord;
    }

    /**
     *
     * @param field - field from which object should be constructed
     * @return object constructed from field, can return null cause the field value itself can be null
     */
    private Object getFieldObject(ICacheEntryValueField field, Schema schema) {
        String fieldName = field.getName();
        Optional<Object> fieldValue = field.getFieldValue();
        if (field.hasNested()) {
            Schema nestedSchema = Schema.Type.RECORD.equals(schema.getType()) ?
                    schema.getField(fieldName).schema() : schema.getElementType();
            GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
            field.getNested().forEach(nested -> {
                nestedRecord.put(nested.getName(), getFieldObject(nested, nestedSchema));
            });
            return nestedRecord;
        }
        if (TypeUtils.isCollection(field.getTypeClassName())) {
            Schema arraySchema = schema.getField(fieldName).schema();
            GenericData.Array<Object> array = new GenericData.Array<>(0, arraySchema);
            List<ICacheEntryValueField> valueFields = (List<ICacheEntryValueField>) field.getFieldValue().get();
            valueFields.forEach(valueField ->
                    array.add(getFieldObject(valueField, arraySchema)));
            return array;
        }
        if (TypeUtils.isMap(field.getTypeClassName())) {
            Schema arraySchema = schema.getField(fieldName).schema();
            GenericData.Array<GenericRecord> array = new GenericData.Array<>(0, arraySchema);
            Schema arrayElementSchema = arraySchema.getElementType();
            Map<ICacheEntryValueField, ICacheEntryValueField> valueFields =
                    (Map<ICacheEntryValueField, ICacheEntryValueField>) field.getFieldValue().get();
            valueFields.forEach((key, value) -> {
                GenericRecord pair = new GenericData.Record(arrayElementSchema);
                Object resultVal = getFieldObject(value, arrayElementSchema);
                Object resultKey = getFieldObject(key, arrayElementSchema);
                pair.put(key.getName(), resultKey);
                pair.put(value.getName(), resultVal);
                array.add(pair);
            });

            return array;
        }

        if (fieldValue.isPresent()) {
            return AvroDerivedTypeConverterFactory.get(schema.getField(fieldName))
                    .convertForAvro(fieldValue.get());
        }

        // we return null because the value itself of any field can be null
        return null;
    }

    @Override
    public void close() throws Exception {
        dataFileWriter.close();
    }

    public static class Builder {
        private DataFileWriter<GenericRecord> dataFileWriter;
        private Schema cacheDataAvroSchema;

        public Builder setDataFileWriter(DataFileWriter<GenericRecord> dataFileWriter) {
            this.dataFileWriter = dataFileWriter;
            return this;
        }

        public Builder setCacheDataAvroSchema(Schema cacheDataAvroSchema) {
            this.cacheDataAvroSchema = cacheDataAvroSchema;
            return this;
        }

        public CacheDataWriter build() {
            return new CacheDataWriter(this);
        }
    }
}
