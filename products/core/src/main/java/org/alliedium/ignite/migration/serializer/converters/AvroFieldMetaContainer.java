package org.alliedium.ignite.migration.serializer.converters;

import java.util.HashMap;
import java.util.Map;

import org.alliedium.ignite.migration.serializer.converters.datatypes.AvroDerivedTypeConverterFactory;
import org.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

/**
 * Unit contains meta-data for avro field (field name, converter needed to restore the initial value type, which had not been supported by avro and was casted to supported one),
 * required for deserializing field data.
 * Meta-data can be requested by avro field name.
 * Needs to be created in terms of deserializing from avro to DTO.
 *
 * @see IAvroDerivedTypeConverter
 */
public class AvroFieldMetaContainer {

    private final Map<String, IAvroDerivedTypeConverter> fieldNameToContainerMap;

    public AvroFieldMetaContainer(Schema avroSchema) {
        Map<String, IAvroDerivedTypeConverter> fieldNameToContainerMap = new HashMap<>();
        for (Field avroSchemaField : avroSchema.getFields()) {
            String fieldName = avroSchemaField.name();
            IAvroDerivedTypeConverter avroTypeConverter = AvroDerivedTypeConverterFactory.get(avroSchemaField.doc());
            fieldNameToContainerMap.put(fieldName, avroTypeConverter);
        }
        this.fieldNameToContainerMap = fieldNameToContainerMap;
    }

    public IAvroDerivedTypeConverter getAvroFieldConverter(String avroFieldName) {
        return this.fieldNameToContainerMap.get(avroFieldName);
    }
}