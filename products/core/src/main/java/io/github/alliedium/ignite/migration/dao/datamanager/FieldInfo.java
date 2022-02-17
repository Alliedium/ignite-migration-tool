package io.github.alliedium.ignite.migration.dao.datamanager;

import io.github.alliedium.ignite.migration.dao.converters.IIgniteBinaryDataConverter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit is used to be a container for meta-data of separate Apache Ignite cache field (field name, value type class name,
 * converter needed to get a common Java representation when the field value is of Apache Ignite specific class).
 * Storing meta-data needs to be passed in on initialization.
 */
public class FieldInfo implements IFieldInfo {

    private final String name;
    private final String typeInfo;
    private final IIgniteBinaryDataConverter binaryObjectFieldDataConverter;
    private final Map<String, IFieldInfo> nested;

    private FieldInfo(Builder builder) {
        this.name = builder.name;
        this.binaryObjectFieldDataConverter = builder.binaryObjectFieldDataConverter;
        this.typeInfo = builder.typeInfo;
        this.nested = builder.nested;
    }

    public String getName() {
        return this.name;
    }

    public String getTypeInfo() {
        return this.typeInfo;
    }

    @Override
    public Map<String, IFieldInfo> getNested() {
        return new HashMap<>(nested);
    }

    public IIgniteBinaryDataConverter getFieldDataConverter() {
        return this.binaryObjectFieldDataConverter;
    }

    public static class Builder {
        private String name;
        private String typeInfo;
        private IIgniteBinaryDataConverter binaryObjectFieldDataConverter;
        private Map<String, IFieldInfo> nested;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setTypeInfo(String typeInfo) {
            this.typeInfo = typeInfo;
            return this;
        }

        public Builder setBinaryObjectFieldDataConverter(IIgniteBinaryDataConverter binaryObjectFieldDataConverter) {
            this.binaryObjectFieldDataConverter = binaryObjectFieldDataConverter;
            return this;
        }

        public Builder setNested(Map<String, IFieldInfo> nested) {
            this.nested = new HashMap<>(nested);
            return this;
        }

        public FieldInfo build() {
            nested = nested == null ? Collections.emptyMap() : nested;
            return new FieldInfo(this);
        }
    }
}
