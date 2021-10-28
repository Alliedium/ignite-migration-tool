package org.alliedium.ignite.migration.serializer.converters;

import org.alliedium.ignite.migration.dto.CacheEntryValue;
import org.alliedium.ignite.migration.dto.CacheEntryValueField;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValueField;
import org.alliedium.ignite.migration.serializer.converters.datatypes.AvroByteArrayConverter;
import org.alliedium.ignite.migration.serializer.converters.datatypes.AvroDerivedTypeConverterFactory;
import org.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;

import java.util.*;

import org.alliedium.ignite.migration.util.UniqueKey;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * Unit executes conversion from provided avro {@link GenericRecord} to {@link ICacheEntryValue} based on meta-data, which also needs to be provided.
 * Used for deserializing cache data from avro.
 *
 * @see AvroFieldMetaContainer
 */
public class AvroToGenericRecordConverter implements IAvroToGenericRecordConverter {

    private final Map<String, String> fieldsTypes;

    public AvroToGenericRecordConverter(Map<String, String> fieldsTypes) {
        this.fieldsTypes = new HashMap<>(fieldsTypes);
    }

    @Override
    public ICacheEntryValue getCacheEntryValue(GenericRecord record) {
        return getCacheEntryValue(record, Collections.emptySet());
    }

    @Override
    public ICacheEntryValue getCacheEntryValue(GenericRecord record, Set<String> fieldNamesToIgnore) {
        List<ICacheEntryValueField> cacheEntryValueFieldList = new ArrayList<>();
        for (Field field : record.getSchema().getFields()) {
            if (fieldNamesToIgnore.contains(field.name())) {
                continue;
            }

            ICacheEntryValueField cacheEntryValueField = getEntryValueField(field, record);
            cacheEntryValueFieldList.add(cacheEntryValueField);
        }

        return new CacheEntryValue(cacheEntryValueFieldList);
    }

    private ICacheEntryValueField getEntryValueField(Schema.Field field, GenericRecord record) {
        Object fieldVal = record.get(field.name());
        CacheEntryValueField.Builder builder = new CacheEntryValueField.Builder()
                .setName(field.name())
                .setTypeClassName(getFieldType(field));

        if (fieldVal == null) {
            return builder.build();
        }

        if (field.schema().getType().equals(Schema.Type.RECORD)) {
            List<ICacheEntryValueField> nestedFields = new ArrayList<>();
            List<Field> nestedAvroFields = field.schema().getFields();
            nestedAvroFields.forEach(nestedAvroField -> {
                nestedFields.add(getEntryValueField(nestedAvroField, (GenericRecord) fieldVal));
            });
            return builder.setNested(nestedFields).build();
        }

        IAvroDerivedTypeConverter converter = AvroDerivedTypeConverterFactory.get(field);
        // todo: type verification, fix required
        if (fieldVal.getClass().equals(Utf8.class) && !converter.getClass().equals(AvroByteArrayConverter.class)) {
            return builder.setValue(fieldVal.toString()).build();
        }

        return builder.setValue(converter.convertFromAvro(fieldVal)).build();
    }

    private String getFieldType(Schema.Field field) {
        String fieldName = field.name();
        String type = fieldsTypes.get(fieldName);
        if (type == null) {
            type = fieldsTypes.get(fieldName.toUpperCase());
        }

        Schema.Type schemaType = field.schema().getType();

        if (schemaType.equals(Schema.Type.RECORD)) {
            Optional<String> optionalType = UniqueKey.getRecordType(field.schema().getFullName());
            if (optionalType.isPresent()) {
                type = optionalType.get();
            }
        }
        if (schemaType.equals(Schema.Type.UNION)) {
            List<Schema> types = field.schema().getTypes();
            if (types.size() > 2) {
                // todo: a place for improvement
                throw new UnsupportedOperationException(
                        "more than two types for one schema union are not supported");
            }
            Optional<Schema> optionalSchema = types.stream().filter(schema -> !schema.getType().equals(Schema.Type.NULL))
                    .findFirst();
            if (optionalSchema.isPresent()) {
                type = optionalSchema.get().getType().getName();
            }
        }
        if (type == null && !schemaType.equals(Schema.Type.RECORD)
                && !schemaType.equals(Schema.Type.UNION)) {
            type = schemaType.getName();
        }
        if (type == null) {
            type = field.schema().getDoc();
        }
        if (type == null) {
            throw new IllegalStateException("cannot find type for field: " + fieldName);
        }

        return type;
    }
}
