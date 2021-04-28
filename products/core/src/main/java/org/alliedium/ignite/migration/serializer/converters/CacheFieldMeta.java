package org.alliedium.ignite.migration.serializer.converters;

import org.alliedium.ignite.migration.dto.CacheEntryValue;
import org.alliedium.ignite.migration.dto.CacheEntryValueField;
import org.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;
import org.alliedium.ignite.migration.serializer.converters.schemafields.IAvroSchemaFieldAssembler;

/**
 * Unit returns meta-data of separate {@link CacheEntryValueField}, required for this field to be serialized to avro (field name,
 * assembler needed to define this field in avro schema, converter needed to convert field value to avro type if current one is not supported).
 * Units are being created for all the fields included into {@link CacheEntryValue}
 * and then collected in {@link CacheFieldMetaContainer}.
 *
 * @see IAvroDerivedTypeConverter
 * @see IAvroSchemaFieldAssembler
 */
public class CacheFieldMeta implements ICacheFieldMeta {

    private final String fieldName;
    private final IAvroSchemaFieldAssembler avroSchemaFieldAssembler;
    private final IAvroDerivedTypeConverter avroDerivedTypeConverter;

    public CacheFieldMeta(String fieldName, IAvroSchemaFieldAssembler avroSchemaFieldAssembler, IAvroDerivedTypeConverter avroDerivedTypeConverter) {
        this.fieldName = fieldName;
        this.avroSchemaFieldAssembler = avroSchemaFieldAssembler;
        this.avroDerivedTypeConverter = avroDerivedTypeConverter;
    }

    public String getName() {
        return this.fieldName;
    }

    public IAvroSchemaFieldAssembler getAvroSchemaFieldAssembler() {
        return this.avroSchemaFieldAssembler;
    }

    public IAvroDerivedTypeConverter getAvroDerivedTypeConverter() {
        return this.avroDerivedTypeConverter;
    }
}
