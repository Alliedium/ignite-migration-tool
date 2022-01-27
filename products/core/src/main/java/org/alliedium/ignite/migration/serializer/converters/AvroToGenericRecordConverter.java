package org.alliedium.ignite.migration.serializer.converters;

import org.alliedium.ignite.migration.dto.CacheEntryValue;
import org.alliedium.ignite.migration.dto.CacheEntryValueField;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValueField;
import org.alliedium.ignite.migration.serializer.converters.datatypes.AvroByteArrayConverter;
import org.alliedium.ignite.migration.serializer.converters.datatypes.AvroDerivedTypeConverterFactory;
import org.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;

import java.util.*;

import org.alliedium.ignite.migration.util.TypeUtils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
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
        String fieldTypeClassName = getFieldType(field);
        CacheEntryValueField.Builder builder = new CacheEntryValueField.Builder()
                .setName(field.name())
                .setTypeClassName(fieldTypeClassName);

        if (fieldVal == null) {
            return builder.build();
        }

        if (isArrayOrRecordSchema(field.schema())) {
            return extractComplexEntryValueField(builder, field.schema(), fieldVal);
        }

        IAvroDerivedTypeConverter converter = AvroDerivedTypeConverterFactory.get(field);
        // todo: type verification, fix required
        if (fieldVal.getClass().equals(Utf8.class) && !converter.getClass().equals(AvroByteArrayConverter.class)) {
            return builder.setValue(fieldVal.toString()).build();
        }

        return builder.setValue(converter.convertFromAvro(fieldVal)).build();
    }

    /**
     * Extracts complex entry value field. It Will throw an exception if type is of object is not complex.
     * Complex types are: records and arrays
     * A Map is processed like an array because Apache avro supports only maps with string keys,
     * while map key in java can be any object.
     *
     * @param builder - external builder which collects other field values
     * @param schema - fieldVal avro schema
     * @param fieldVal - value to be extracted into dto
     * @return {@link ICacheEntryValueField} extracted from fieldVal or throws IllegalStateException
     *           in case type of schema is not complex
     */
    private ICacheEntryValueField extractComplexEntryValueField(CacheEntryValueField.Builder builder, Schema schema, Object fieldVal) {
        if (schema.getType().equals(Schema.Type.RECORD)) {
            List<ICacheEntryValueField> nestedFields = new ArrayList<>();
            List<Field> nestedAvroFields = schema.getFields();
            nestedAvroFields.forEach(nestedAvroField -> {
                nestedFields.add(getEntryValueField(nestedAvroField, (GenericRecord) fieldVal));
            });
            return builder.setNested(nestedFields).build();
        }
        if (schema.getType().equals(Schema.Type.ARRAY)) {
            List<ICacheEntryValueField> values = new ArrayList<>();
            Schema elementSchema = schema.getElementType();
            GenericArray<GenericRecord> array = (GenericArray<GenericRecord>) fieldVal;

            if (elementSchema.getFields().size() == 2) {
                Map<ICacheEntryValueField, ICacheEntryValueField> valueMap = new HashMap<>();
                array.forEach(val -> {
                    valueMap.put(
                            getEntryValueField(elementSchema.getField(TypeUtils.KEY), val),
                            getEntryValueField(elementSchema.getField(TypeUtils.VALUE), val));
                });
                return builder.setValue(valueMap).build();
            }

            array.forEach(val -> {
                String valueType = getSchemaType(elementSchema);
                CacheEntryValueField.Builder valueBuilder = new CacheEntryValueField.Builder()
                        .setName(TypeUtils.VALUE)
                        .setTypeClassName(valueType);
                values.add(extractComplexEntryValueField(valueBuilder, elementSchema, val));
            });
            return builder.setValue(values).build();
        }

        throw new IllegalStateException("Complex type not found for schema: " + schema);
    }

    private boolean isArrayOrRecordSchema(Schema schema) {
        Schema.Type type = schema.getType();
        return Schema.Type.RECORD.equals(type)
                || Schema.Type.ARRAY.equals(type);
    }

    private String getFieldType(Schema.Field field) {
        String fieldName = field.name();
        String type = fieldsTypes.get(fieldName);
        if (type == null) {
            type = fieldsTypes.get(fieldName.toUpperCase());
        }

        return getSchemaType(field.schema(), Optional.ofNullable(type));
    }

    private String getSchemaType(Schema schema) {
        return getSchemaType(schema, Optional.empty());
    }

    private String getSchemaType(Schema schema, Optional<String> defaultType) {
        if (isArrayOrRecordSchema(schema)) {
            return schema.getProp(TypeUtils.FIELD_TYPE);
        }

        Schema.Type schemaType = schema.getType();
        String type = null;
        if (schemaType.equals(Schema.Type.UNION)) {
            List<Schema> types = schema.getTypes();
            if (types.size() > 2) {
                throw new UnsupportedOperationException(
                        "more than two types for one schema union are not supported");
            }
            Optional<Schema> optionalSchema = types.stream()
                    .filter(unitSchema -> !unitSchema.getType().equals(Schema.Type.NULL))
                    .findFirst();
            if (optionalSchema.isPresent()) {
                type = optionalSchema.get().getType().getName();
            }
        }
        if (type == null && defaultType.isPresent()) {
            type = defaultType.get();
        }
        if (type == null) {
            throw new IllegalStateException("cannot extract type from schema: " + schema);
        }

        return type;
    }
}
