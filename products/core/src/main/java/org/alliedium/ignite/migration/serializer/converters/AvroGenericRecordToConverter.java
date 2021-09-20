package org.alliedium.ignite.migration.serializer.converters;

import org.alliedium.ignite.migration.dto.CacheEntryValue;
import org.alliedium.ignite.migration.dto.CacheEntryValueField;
import org.alliedium.ignite.migration.dto.CacheEntryValueFieldValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValueField;
import org.alliedium.ignite.migration.dto.ICacheEntryValueFieldValue;
import org.alliedium.ignite.migration.serializer.converters.datatypes.AvroByteArrayConverter;
import org.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.alliedium.ignite.migration.serializer.utils.FieldNames;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * Unit executes conversion from provided avro {@link GenericRecord} to {@link ICacheEntryValue} based on meta-data, which also needs to be provided.
 * Used for deserializing cache data from avro.
 *
 * @see AvroFieldMetaContainer
 */
public class AvroGenericRecordToConverter implements IAvroGenericRecordToConverter {

    private final Map<String, String> fieldsTypes;

    public AvroGenericRecordToConverter(Map<String, String> fieldsTypes) {
        this.fieldsTypes = new HashMap<>(fieldsTypes);
    }
    public ICacheEntryValue getCacheEntryValue(GenericRecord avroGenericRecord, AvroFieldMetaContainer avroFieldMetaContainer) {
        List<ICacheEntryValueField> cacheEntryValueFieldList = new ArrayList<>();
        for (Field genericRecordField : avroGenericRecord.getSchema().getFields()) {
            if (!genericRecordField.name().equalsIgnoreCase(FieldNames.AVRO_GENERIC_RECORD_KEY_FIELD_NAME)) {
                ICacheEntryValueFieldValue CacheEntryValueFieldValue = getCacheEntryValueFieldValue(
                        avroGenericRecord.get(genericRecordField.name()), genericRecordField.name(), avroFieldMetaContainer);
                ICacheEntryValueField cacheEntryValueField = new CacheEntryValueField(
                        genericRecordField.name(), getFieldType(genericRecordField.name()), CacheEntryValueFieldValue);
                cacheEntryValueFieldList.add(cacheEntryValueField);
            }
        }

        return new CacheEntryValue(cacheEntryValueFieldList);
    }

    private String getFieldType(String fieldName) {
        String type = fieldsTypes.get(fieldName);
        if (type == null) {
            type = fieldsTypes.get(fieldName.toUpperCase());
        }

        return type;
    }

    private ICacheEntryValueFieldValue getCacheEntryValueFieldValue(Object genericRecordFieldValue, String genericRecordFieldName, AvroFieldMetaContainer avroFieldMetaContainer) {
        if (genericRecordFieldValue == null) {
            return CacheEntryValueFieldValue.noValue();
        }
        IAvroDerivedTypeConverter avroDerivedTypeConverter = avroFieldMetaContainer.getAvroFieldConverter(genericRecordFieldName);
        // todo: type verification, fix required
        if (genericRecordFieldValue.getClass().equals(Utf8.class) && !avroDerivedTypeConverter.getClass().equals(AvroByteArrayConverter.class)) {
            return new CacheEntryValueFieldValue(genericRecordFieldValue.toString());
        }

        return new CacheEntryValueFieldValue(avroDerivedTypeConverter.convertFromAvro(genericRecordFieldValue));
    }

}
