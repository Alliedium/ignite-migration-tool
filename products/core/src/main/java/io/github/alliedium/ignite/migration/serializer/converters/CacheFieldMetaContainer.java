package io.github.alliedium.ignite.migration.serializer.converters;

import io.github.alliedium.ignite.migration.serializer.converters.datatypes.AvroDerivedTypeConverterFactory;
import io.github.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;
import io.github.alliedium.ignite.migration.serializer.converters.schemafields.IAvroSchemaFieldAssembler;
import io.github.alliedium.ignite.migration.serializer.converters.schemafields.SchemaFieldAssemblerFactory;
import io.github.alliedium.ignite.migration.util.TypeUtils;
import io.github.alliedium.ignite.migration.dto.ICacheEntryValue;
import io.github.alliedium.ignite.migration.dto.ICacheEntryValueField;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Unit contains meta-data as {@link ICacheFieldMeta} for each separate {@link ICacheEntryValueField}.
 * Required {@link ICacheFieldMeta} can be requested by by field name ({@link ICacheEntryValueField#getName()}).
 * Needs to be created in terms of serializing DTO to avro.
 */
public class CacheFieldMetaContainer implements ICacheFieldMetaContainer {

    private final Map<String, ICacheFieldMeta> fieldsAvroInfoContainer;

    public CacheFieldMetaContainer(ICacheEntryValue cacheEntryValue) {
        fieldsAvroInfoContainer = new HashMap<>();

        List<String> fieldNamesList = cacheEntryValue.getFieldNames();
        //TODO: to add a mechanism which checks whether provided DTO can be serialized to avro (all the incoming value types are supported or custom converters are available)
        for (String fieldName : fieldNamesList) {
            if (fieldsAvroInfoContainer.containsKey(fieldName)) {
                throw new IllegalStateException(String.format("Duplicate fieldName %s", fieldName));
            }

            fieldsAvroInfoContainer.put(fieldName, createFieldMeta(cacheEntryValue.getField(fieldName)));
        }
    }

    /**
     * Creates field meta from provided {@link ICacheEntryValueField}, main purpose to create field meta is
     * to find appropriate avro schema assembler and types converter from java to avro.
     * This method will create metadata for provided field as well as for it's nested fields.
     * Collections and Maps are treated as they have nested fields. This is done in order to combine
     * nested objects architecture and collections/maps cause there could be even nested collections inside.
     *
     * @param field
     * @return cache field metadata with all nested fields
     */
    private CacheFieldMeta createFieldMeta(ICacheEntryValueField field) {
        String fieldName = field.getName();
        String fieldAvroType = field.getTypeClassName();
        if (field.hasNested()) {
            List<ICacheFieldMeta> nested = new ArrayList<>();
            field.getNested().forEach(val -> nested.add(createFieldMeta(val)));
            IAvroSchemaFieldAssembler avroSchemaFieldAssembler = SchemaFieldAssemblerFactory.get(fieldAvroType, nested);
            IAvroDerivedTypeConverter avroDerivedTypeConverter = AvroDerivedTypeConverterFactory.get(fieldAvroType);
            return new CacheFieldMeta.Builder()
                    .setFieldName(fieldName)
                    .setAvroSchemaFieldAssembler(avroSchemaFieldAssembler)
                    .setAvroDerivedTypeConverter(avroDerivedTypeConverter)
                    .setNested(nested)
                    .setFieldType(fieldAvroType)
                    .build();
        }
        if (TypeUtils.isCollection(fieldAvroType) && field.getFieldValue().isPresent()) {
            Collection<ICacheEntryValueField> collection = (Collection<ICacheEntryValueField>) field.getFieldValue().get();
            ICacheEntryValueField elementField = collection.iterator().next();
            ICacheFieldMeta elementFieldMeta = createFieldMeta(elementField);
            IAvroSchemaFieldAssembler avroSchemaFieldAssembler = SchemaFieldAssemblerFactory.get(fieldAvroType);
            IAvroDerivedTypeConverter avroDerivedTypeConverter = AvroDerivedTypeConverterFactory.get(fieldAvroType);
            return new CacheFieldMeta.Builder()
                    .setFieldName(fieldName)
                    .setAvroSchemaFieldAssembler(avroSchemaFieldAssembler)
                    .setAvroDerivedTypeConverter(avroDerivedTypeConverter)
                    .setNested(Stream.of(elementFieldMeta).collect(Collectors.toList()))
                    .setFieldType(fieldAvroType)
                    .build();
        }
        if (TypeUtils.isMap(fieldAvroType) && field.getFieldValue().isPresent()) {
            Map<ICacheEntryValueField, ICacheEntryValueField> map = (Map<ICacheEntryValueField, ICacheEntryValueField>) field.getFieldValue().get();
            ICacheEntryValueField elementKeyField = map.keySet().iterator().next();
            ICacheFieldMeta elementKeyFieldMeta = createFieldMeta(elementKeyField);
            ICacheEntryValueField valueKeyField = map.values().iterator().next();
            ICacheFieldMeta elementValueFieldMeta = createFieldMeta(valueKeyField);
            IAvroSchemaFieldAssembler avroSchemaFieldAssembler = SchemaFieldAssemblerFactory.get(fieldAvroType);
            IAvroDerivedTypeConverter avroDerivedTypeConverter = AvroDerivedTypeConverterFactory.get(fieldAvroType);
            return new CacheFieldMeta.Builder()
                    .setFieldName(fieldName)
                    .setAvroSchemaFieldAssembler(avroSchemaFieldAssembler)
                    .setAvroDerivedTypeConverter(avroDerivedTypeConverter)
                    .setNested(Stream.of(elementKeyFieldMeta, elementValueFieldMeta).collect(Collectors.toList()))
                    .setFieldType(fieldAvroType)
                    .build();
        }

        IAvroSchemaFieldAssembler avroSchemaFieldAssembler = SchemaFieldAssemblerFactory.get(fieldAvroType);
        IAvroDerivedTypeConverter avroDerivedTypeConverter = AvroDerivedTypeConverterFactory.get(fieldAvroType);
        return new CacheFieldMeta.Builder()
                .setFieldName(fieldName)
                .setAvroSchemaFieldAssembler(avroSchemaFieldAssembler)
                .setAvroDerivedTypeConverter(avroDerivedTypeConverter)
                .setFieldType(fieldAvroType)
                .build();
    }

    public ICacheFieldMeta getFieldTypeMeta(String fieldName) {
        return this.fieldsAvroInfoContainer.get(fieldName);
    }
}
