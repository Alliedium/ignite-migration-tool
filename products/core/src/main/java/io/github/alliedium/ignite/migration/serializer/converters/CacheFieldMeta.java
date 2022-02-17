package io.github.alliedium.ignite.migration.serializer.converters;

import io.github.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;
import io.github.alliedium.ignite.migration.serializer.converters.schemafields.IAvroSchemaFieldAssembler;
import io.github.alliedium.ignite.migration.dto.CacheEntryValue;
import io.github.alliedium.ignite.migration.dto.CacheEntryValueField;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private final Map<String, ICacheFieldMeta> nested;
    private final String fieldType;

    private CacheFieldMeta(Builder builder) {
        fieldName = builder.fieldName;
        avroSchemaFieldAssembler = builder.avroSchemaFieldAssembler;
        avroDerivedTypeConverter = builder.avroDerivedTypeConverter;
        nested = builder.nested;
        fieldType = builder.fieldType;
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

    @Override
    public Map<String, ICacheFieldMeta> getNested() {
        return new HashMap<>(nested);
    }

    @Override
    public boolean hasNested() {
        return !nested.isEmpty();
    }

    public String getFieldType() {
        return fieldType;
    }

    public static class Builder {
        private String fieldName;
        private IAvroSchemaFieldAssembler avroSchemaFieldAssembler;
        private IAvroDerivedTypeConverter avroDerivedTypeConverter;
        private Map<String, ICacheFieldMeta> nested;
        private String fieldType;

        public Builder setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder setAvroSchemaFieldAssembler(IAvroSchemaFieldAssembler avroSchemaFieldAssembler) {
            this.avroSchemaFieldAssembler = avroSchemaFieldAssembler;
            return this;
        }

        public Builder setAvroDerivedTypeConverter(IAvroDerivedTypeConverter avroDerivedTypeConverter) {
            this.avroDerivedTypeConverter = avroDerivedTypeConverter;
            return this;
        }

        public Builder setNested(Map<String, ICacheFieldMeta> nested) {
            this.nested = nested;
            return this;
        }

        public Builder setNested(List<ICacheFieldMeta> nested) {
            return setNested(nested.stream().collect(
                    Collectors.toMap(ICacheFieldMeta::getName, Function.identity())));
        }

        public Builder setFieldType(String fieldType) {
            this.fieldType = fieldType;
            return this;
        }

        public CacheFieldMeta build() {
            this.nested = this.nested == null ? Collections.emptyMap() : new HashMap<>(this.nested);
            return new CacheFieldMeta(this);
        }
    }

}
