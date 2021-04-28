package org.alliedium.ignite.migration.dao.datamanager;

import org.alliedium.ignite.migration.dao.converters.IIgniteBinaryDataConverter;

/**
 * Unit is used to be a container for meta-data of separate ignite cache field (field name, value type class name,
 * converter needed to get a common Java representation when the field value is of Ignite specific class).
 * Storing meta-data needs to be passed in on initialization.
 */
public class IgniteCacheFieldMeta implements IIgniteCacheFieldMeta {

    private final String name;
    private final String typeInfo;
    private final IIgniteBinaryDataConverter binaryObjectFieldDataConverter;

    public IgniteCacheFieldMeta(String name, String typeInfo, IIgniteBinaryDataConverter binaryObjectFieldDataConverter) {
        this.name = name;
        this.typeInfo = typeInfo;
        this.binaryObjectFieldDataConverter = binaryObjectFieldDataConverter;
    }

    public String getName() {
        return this.name;
    }

    public String getTypeInfo() {
        return this.typeInfo;
    }

    public IIgniteBinaryDataConverter getFieldDataConverter() {
        return this.binaryObjectFieldDataConverter;
    }
}
