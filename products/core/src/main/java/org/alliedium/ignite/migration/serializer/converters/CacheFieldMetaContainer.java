package org.alliedium.ignite.migration.serializer.converters;

import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.dto.ICacheEntryValueField;
import org.alliedium.ignite.migration.serializer.converters.datatypes.AvroDerivedTypeConverterFactory;
import org.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;
import org.alliedium.ignite.migration.serializer.converters.schemafields.IAvroSchemaFieldAssembler;
import org.alliedium.ignite.migration.serializer.converters.schemafields.SchemaFieldAssemblerFactory;

import java.util.*;

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
