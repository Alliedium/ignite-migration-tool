package io.github.alliedium.ignite.migration.dao.converters;

import io.github.alliedium.ignite.migration.dao.datamanager.BinaryObjectFieldsInfoResolver;
import io.github.alliedium.ignite.migration.dao.datamanager.FieldInfo;

/**
 * Non-functional converter.
 * Since {@link FieldInfo} container needs any converter to be assigned to each of Apache Ignite cache fields,
 * current converter is being used for fields, whose values don't need to be converted (== value type is not ignite-specific).
 *
 * @see BinaryObjectFieldsInfoResolver
 */
public class PlainObjectProviderDataConverter implements IIgniteBinaryDataConverter {

    public Object convert(Object fieldData) {
        return fieldData;
    }
}
